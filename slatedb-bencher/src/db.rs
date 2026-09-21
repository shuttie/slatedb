//! # Database benchmarker
//!
//! This module contains the database benchmarker, which is used to benchmark
//! SlateDB. The benchmarker is a subcommand of the `bencher` CLI tool.
//!
//! The DB benchmarker supports:
//!
//! - Configurable key/value sizes
//! - Configurable `WriteOptions`
//! - A pluggable key generator strategy (defaults to fixed keyset)
//! - Configurable `DbOptions`` (for common variables)
//! - Charts with gnuplots
//! - Mixed read/write workloads
//! - Concurrent workloads
//!
//! ## Design
//!
//! The benchmarker spins up `concurrency` tasks, each of which runs a loop.
//! The loop generates a key (and value if needed), and then either puts they
//! key/value pair or gets the key. The ratio of puts to gets is controlled by
//! the `put_percentage`.
//!
//! Every `REPORT_INTERVAL`, the task records the number of puts and gets since
//! the last report. The stats are kept in a rolling window of fixed duration
//! (`WINDOW_SIZE`).
//!
//! Meanwhile, the main thread loops, sleeping for `REPORT_INTERVAL` and
//! then checking if it's been more than `STAT_DUMP_INTERVAL` since the last
//! dump. If so, it sums all puts and gets starting from the most recently
//! completed window, looking back `STAT_DUMP_LOOKBACK`. It then prints the sum
//! to the console.
//!
//! If `STAT_DUMP_LOOKBACK` is greater than `STAT_DUMP_INTERVAL` and
//! `WINDOW_SIZE`, the result will be the sum of multiple windows, and thus
//! some smoothing will occur.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::{FlushOptions, FlushType, MultiGetOptions, PutOptions, WriteOptions};
use slatedb::db_stats::MULTI_GET_WAVES;
use slatedb::Db;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::args::Reader;
use crate::bench_object_store::BenchObjectStore;
use crate::stats::{StatsRecorder, WindowStats};

/// How frequently to dump stats to the console.
const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// How far back to look when dumping stats.
const STAT_DUMP_LOOKBACK: Duration = Duration::from_secs(60);

/// How frequently to update stats between puts and gets and
/// how frequently to check if we need to dump new stats.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of randomly generated keys to keep track of for reuse in
/// [`RandomKeyGenerator`]. Once this limit is reached, newly generated keys
/// will replace a randomly selected existing key.
const MAX_RANDOM_USED_KEYS: usize = 10_000;

/// Maximum number of read latency samples between two stats dumps. Above
/// this limit, a new sample replaces a random old one.
const MAX_LATENCY_SAMPLES: usize = 200_000;

/// How the benchmark reads keys.
pub enum ReadMode {
    /// One `get` call per read, as the benchmark always did.
    Get,
    /// A batch of keys per read, through one of the readers.
    Mget {
        reader: Reader,
        batch_size: usize,
        options: MultiGetOptions,
    },
}

impl ReadMode {
    fn batch_size(&self) -> usize {
        match self {
            ReadMode::Get => 1,
            ReadMode::Mget { batch_size, .. } => *batch_size,
        }
    }
}

/// Logs the number of L0 SSTs, sorted runs, and SSTs of the database.
pub fn log_manifest_shape(db: &Db) {
    let manifest = db.manifest();
    let l0_ssts = manifest.l0().len();
    let sorted_runs = manifest.compacted().len();
    let ssts = l0_ssts
        + manifest
            .compacted()
            .iter()
            .map(|run| run.sst_views().len())
            .sum::<usize>();
    info!(l0_ssts, sorted_runs, ssts, "manifest shape");
}

/// Flushes the memtable, then waits until the compactor has emptied L0 and
/// the sorted runs stay the same for a few seconds. Returns after `timeout`
/// in any case.
pub async fn wait_for_compaction(db: &Db, timeout: Duration) {
    const POLL: Duration = Duration::from_millis(500);
    const STABLE_POLLS: u32 = 10;
    let flush = FlushOptions {
        flush_type: FlushType::MemTable,
    };
    if let Err(e) = db.flush_with_options(flush).await {
        warn!("memtable flush failed [error={}]", e);
    }
    log_manifest_shape(db);
    let start = Instant::now();
    let mut last_runs = None;
    let mut stable = 0;
    while start.elapsed() < timeout {
        let manifest = db.manifest();
        let runs = manifest.compacted().len();
        if manifest.l0().is_empty() && last_runs == Some(runs) {
            stable += 1;
        } else {
            stable = 0;
        }
        last_runs = Some(runs);
        if stable >= STABLE_POLLS {
            log_manifest_shape(db);
            return;
        }
        tokio::time::sleep(POLL).await;
    }
    warn!("compaction did not settle before the timeout");
    log_manifest_shape(db);
}

/// A key generator trait that generates keys for the benchmarker.
pub trait KeyGenerator: Send {
    /// Generate and return the next key that should be used in the workload.
    /// Implementations **must** push the generated key onto an internal
    /// `used_keys` vector so that it can later be sampled by [`Self::used_key`].
    fn next_key(&mut self) -> Bytes;

    /// Return one of the previously generated keys **at random**. If no keys
    /// have been generated yet, this method will fall back to calling
    /// [`Self::next_key`] to ensure that a valid key is always returned.
    fn used_key(&mut self) -> Bytes;
}

/// A key generator that generates random keys of a fixed length.
pub struct RandomKeyGenerator {
    key_len_bytes: usize,
    rng: XorShiftRng,
    used_keys: Vec<Bytes>,
}

fn rng_from(seed: Option<u64>) -> XorShiftRng {
    match seed {
        Some(seed) => XorShiftRng::seed_from_u64(seed),
        None => XorShiftRng::from_os_rng(),
    }
}

impl RandomKeyGenerator {
    /// With a seed, the generator returns the same keys on each run.
    pub fn new(key_bytes: usize, seed: Option<u64>) -> Self {
        Self {
            key_len_bytes: key_bytes,
            rng: rng_from(seed),
            used_keys: Vec::new(),
        }
    }
}

impl KeyGenerator for RandomKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let mut bytes = vec![0u8; self.key_len_bytes];
        self.rng.fill_bytes(bytes.as_mut_slice());
        let key = Bytes::copy_from_slice(bytes.as_slice());
        // Track the generated key so that it can be sampled later. Keep the
        // list bounded to `MAX_RANDOM_USED_KEYS` entries by randomly replacing
        // an existing key once the limit is reached.
        if self.used_keys.len() < MAX_RANDOM_USED_KEYS {
            self.used_keys.push(key.clone());
        } else {
            let idx = self.rng.random_range(0..self.used_keys.len());
            self.used_keys[idx] = key.clone();
        }
        key
    }

    fn used_key(&mut self) -> Bytes {
        if self.used_keys.is_empty() {
            return self.next_key();
        }
        let idx = self.rng.random_range(0..self.used_keys.len());
        self.used_keys[idx].clone()
    }
}

pub struct FixedSetKeyGenerator {
    keys: Vec<Bytes>,
    rng: XorShiftRng,
    used_keys: Vec<Bytes>,
}

impl FixedSetKeyGenerator {
    /// `set_seed` fixes the key set, and `pick_seed` fixes the order in which
    /// the generator picks from it.
    pub fn new(
        key_bytes: usize,
        key_count: u64,
        set_seed: Option<u64>,
        pick_seed: Option<u64>,
    ) -> Self {
        let mut random_key_generator = RandomKeyGenerator::new(key_bytes, set_seed);
        let mut keys = Vec::new();
        for _ in 0..key_count {
            keys.push(random_key_generator.next_key());
        }
        Self {
            keys,
            rng: rng_from(pick_seed),
            used_keys: Vec::new(),
        }
    }
}

impl KeyGenerator for FixedSetKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let index = self.rng.random_range(0..self.keys.len());
        let key = self.keys[index].clone();
        self.used_keys.push(key.clone());
        key
    }

    fn used_key(&mut self) -> Bytes {
        if self.used_keys.is_empty() {
            return self.next_key();
        }
        let idx = self.rng.random_range(0..self.used_keys.len());
        self.used_keys[idx].clone()
    }
}

/// The database benchmarker.
pub struct DbBench {
    key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
    val_len: usize,
    write_options: WriteOptions,
    await_durable: bool,
    concurrency: u32,
    num_rows: Option<u64>,
    duration: Option<Duration>,
    put_percentage: u32,
    get_hit_percentage: u32,
    read_mode: Arc<ReadMode>,
    db: Arc<Db>,
    store: Arc<BenchObjectStore>,
    recorder: Arc<DefaultMetricsRecorder>,
}

impl DbBench {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_len: usize,
        write_options: WriteOptions,
        await_durable: bool,
        concurrency: u32,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        put_percentage: u32,
        get_hit_percentage: u32,
        read_mode: ReadMode,
        db: Arc<Db>,
        store: Arc<BenchObjectStore>,
        recorder: Arc<DefaultMetricsRecorder>,
    ) -> Self {
        Self {
            key_gen_supplier,
            val_len,
            write_options,
            await_durable,
            concurrency,
            num_rows,
            duration,
            put_percentage,
            get_hit_percentage,
            read_mode: Arc::new(read_mode),
            db,
            store,
            recorder,
        }
    }

    /// Run the benchmarker.
    ///
    /// This method spins up `concurrency` tasks, each of which runs a loop,
    /// and then waits for all the tasks to complete. It also spawns a task
    /// to dump stats to the console.
    pub async fn run(&self) {
        let stats_recorder = Arc::new(DbStatsRecorder::new());
        let mut tasks = Vec::new();
        for _ in 0..self.concurrency {
            let mut task = Task::new(
                (*self.key_gen_supplier)(),
                self.val_len,
                self.write_options.clone(),
                self.await_durable,
                self.num_rows,
                self.duration,
                self.put_percentage,
                self.get_hit_percentage,
                self.read_mode.clone(),
                stats_recorder.clone(),
                self.db.clone(),
            );
            tasks.push(tokio::spawn(async move { task.run().await }));
        }
        let store = self.store.clone();
        let recorder = self.recorder.clone();
        tokio::spawn(async move { dump_stats(stats_recorder, store, recorder).await });
        for task in tasks {
            task.await.unwrap();
        }
    }
}

struct Task {
    key_generator: Box<dyn KeyGenerator>,
    val_len: usize,
    write_options: WriteOptions,
    await_durable: bool,
    num_keys: Option<u64>,
    duration: Option<Duration>,
    put_percentage: u32,
    get_hit_percentage: u32,
    read_mode: Arc<ReadMode>,
    stats_recorder: Arc<DbStatsRecorder>,
    db: Arc<Db>,
}

impl Task {
    #[allow(clippy::too_many_arguments)]
    fn new(
        key_generator: Box<dyn KeyGenerator>,
        val_len: usize,
        write_options: WriteOptions,
        await_durable: bool,
        num_keys: Option<u64>,
        duration: Option<Duration>,
        put_percentage: u32,
        get_hit_percentage: u32,
        read_mode: Arc<ReadMode>,
        stats_recorder: Arc<DbStatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            val_len,
            write_options,
            await_durable,
            num_keys,
            duration,
            put_percentage,
            get_hit_percentage,
            read_mode,
            stats_recorder,
            db,
        }
    }

    /// Run the task.
    ///
    /// This method runs a loop, generating a key (and value if needed), and
    /// then either puts the key/value pair or gets the key.
    async fn run(&mut self) {
        let mut random = XorShiftRng::from_os_rng();
        let mut puts = 0u64;
        let mut puts_bytes = 0u64;
        let mut gets = 0u64;
        let mut gets_bytes = 0u64;
        let mut gets_hits = 0u64;
        let mut latencies = Vec::new();
        let duration = self.duration.unwrap_or(Duration::MAX);
        let num_keys = self.num_keys.unwrap_or(u64::MAX);
        let start = Instant::now();
        let mut last_report = start;
        while self.stats_recorder.puts() < num_keys && start.elapsed() < duration {
            if random.random_range(0..100) < self.put_percentage {
                let key = self.key_generator.next_key();
                let mut value = vec![0; self.val_len];
                random.fill_bytes(value.as_mut_slice());
                let result = self
                    .db
                    .put_with_options(key, value, &PutOptions::default(), &self.write_options)
                    .await;
                let result = match result {
                    Ok(handle) if self.await_durable => handle.await_durable().await,
                    Ok(_) => Ok(()),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => {
                        puts += 1;
                        puts_bytes += self.val_len as u64;
                    }
                    Err(e) => warn!("put failed [error={}]", e),
                }
            } else {
                let keys = self.read_keys(&mut random);
                let read_start = Instant::now();
                match Self::read(&self.db, &self.read_mode, &keys).await {
                    Ok(values) => {
                        latencies.push(read_start.elapsed());
                        gets += values.len() as u64;
                        for (key, val) in keys.iter().zip(values) {
                            gets_hits += val.is_some() as u64;
                            gets_bytes +=
                                key.len() as u64 + val.map(|v| v.len() as u64).unwrap_or(0);
                        }
                    }
                    Err(e) => warn!("get failed [error={}]", e),
                }
            }
            if last_report.elapsed() >= REPORT_INTERVAL {
                last_report = Instant::now();
                self.stats_recorder
                    .record_puts(last_report, puts, puts_bytes);
                self.stats_recorder
                    .record_gets(last_report, gets, gets_bytes, gets_hits);
                self.stats_recorder.record_latencies(&mut latencies);
                puts = 0;
                gets = 0;
                puts_bytes = 0;
                gets_bytes = 0;
                gets_hits = 0;
            }
        }
    }

    /// Picks the keys of one read call. Each key is a hit with
    /// `get_hit_percentage` probability.
    fn read_keys(&mut self, random: &mut XorShiftRng) -> Vec<Bytes> {
        (0..self.read_mode.batch_size())
            .map(|_| {
                if random.random_range(0..100) < self.get_hit_percentage {
                    self.key_generator.used_key()
                } else {
                    self.key_generator.next_key()
                }
            })
            .collect()
    }

    /// Reads the keys of one call with the reader of the read mode.
    ///
    /// The function does not borrow the task, because the key generator is
    /// not `Sync` and the future must be `Send`.
    async fn read(
        db: &Db,
        read_mode: &ReadMode,
        keys: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, slatedb::Error> {
        match read_mode {
            ReadMode::Get => Ok(vec![db.get(&keys[0]).await?]),
            ReadMode::Mget {
                reader: Reader::Seq,
                ..
            } => {
                let mut values = Vec::with_capacity(keys.len());
                for key in keys {
                    values.push(db.get(key).await?);
                }
                Ok(values)
            }
            ReadMode::Mget {
                reader: Reader::Concurrent,
                options,
                ..
            } => {
                let gets: Vec<_> = keys.iter().map(|key| db.get(key)).collect();
                futures::stream::iter(gets)
                    .buffered(options.max_fetch_tasks)
                    .try_collect()
                    .await
            }
            ReadMode::Mget {
                reader: Reader::MultiGet,
                options,
                ..
            } => db.multi_get_with_options(keys, options).await,
        }
    }
}

/// Represents the number of puts and gets in a window of time.
#[derive(Debug)]
struct DbWindow {
    range: Range<Instant>,
    puts: u64,
    gets: u64,
    puts_bytes: u64,
    gets_bytes: u64,
    gets_hits: u64,
}

impl Default for DbWindow {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            range: now..now,
            puts: 0,
            gets: 0,
            puts_bytes: 0,
            gets_bytes: 0,
            gets_hits: 0,
        }
    }
}

impl WindowStats for DbWindow {
    fn range(&self) -> Range<Instant> {
        self.range.clone()
    }

    fn set_range(&mut self, range: Range<Instant>) {
        self.range = range;
    }
}

struct DbStatsRecorder {
    recorder: StatsRecorder<DbWindow>,
    // Overall totals tracked separately
    total_puts: AtomicU64,
    total_gets: AtomicU64,
    total_puts_bytes: AtomicU64,
    total_gets_bytes: AtomicU64,
    total_gets_hits: AtomicU64,
    /// Read calls since the start. One call reads one key or one batch.
    total_calls: AtomicU64,
    /// Latency samples of the read calls since the last stats dump.
    latencies: Mutex<Vec<Duration>>,
}

impl DbStatsRecorder {
    fn new() -> Self {
        Self {
            recorder: StatsRecorder::new(),
            total_puts: AtomicU64::new(0),
            total_gets: AtomicU64::new(0),
            total_puts_bytes: AtomicU64::new(0),
            total_gets_bytes: AtomicU64::new(0),
            total_gets_hits: AtomicU64::new(0),
            total_calls: AtomicU64::new(0),
            latencies: Mutex::new(Vec::new()),
        }
    }

    /// Moves the samples into the recorder. Above `MAX_LATENCY_SAMPLES`, a
    /// new sample replaces a random old one, so the memory stays bounded.
    fn record_latencies(&self, samples: &mut Vec<Duration>) {
        self.total_calls
            .fetch_add(samples.len() as u64, Ordering::Relaxed);
        let mut latencies = self.latencies.lock().expect("lock failed");
        let mut rng = XorShiftRng::from_os_rng();
        for sample in samples.drain(..) {
            if latencies.len() < MAX_LATENCY_SAMPLES {
                latencies.push(sample);
            } else {
                let idx = rng.random_range(0..latencies.len());
                latencies[idx] = sample;
            }
        }
    }

    /// Returns the sorted samples since the last call.
    fn take_latencies(&self) -> Vec<Duration> {
        let mut samples = std::mem::take(&mut *self.latencies.lock().expect("lock failed"));
        samples.sort_unstable();
        samples
    }

    fn calls(&self) -> u64 {
        self.total_calls.load(Ordering::Relaxed)
    }

    fn record_puts(&self, now: Instant, puts: u64, bytes: u64) {
        self.total_puts.fetch_add(puts, Ordering::Relaxed);
        self.total_puts_bytes.fetch_add(bytes, Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.puts += puts;
            window.puts_bytes += bytes;
        });
    }

    fn record_gets(&self, now: Instant, gets: u64, bytes: u64, hits: u64) {
        self.total_gets.fetch_add(gets, Ordering::Relaxed);
        self.total_gets_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_gets_hits.fetch_add(hits, Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.gets += gets;
            window.gets_bytes += bytes;
            window.gets_hits += hits;
        });
    }

    fn puts(&self) -> u64 {
        self.total_puts.load(Ordering::Relaxed)
    }

    fn gets(&self) -> u64 {
        self.total_gets.load(Ordering::Relaxed)
    }

    fn operations_since(
        &self,
        lookback: Duration,
    ) -> Option<(Range<Instant>, u64, u64, u64, u64, u64)> {
        self.recorder.stats_since(lookback, |range, windows| {
            let puts = windows.iter().map(|w| w.puts).sum();
            let gets = windows.iter().map(|w| w.gets).sum();
            let puts_bytes = windows.iter().map(|w| w.puts_bytes).sum();
            let gets_bytes = windows.iter().map(|w| w.gets_bytes).sum();
            let gets_hits = windows.iter().map(|w| w.gets_hits).sum();
            (range, puts, gets, puts_bytes, gets_bytes, gets_hits)
        })
    }
}

/// Returns the value at quantile `q` of sorted samples, or zero with no samples.
fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

/// Reads the `multi_get_waves` counter from the metrics recorder.
fn waves(recorder: &DefaultMetricsRecorder) -> u64 {
    recorder
        .snapshot()
        .by_name(MULTI_GET_WAVES)
        .first()
        .and_then(|metric| match metric.value {
            MetricValue::Counter(value) => Some(value),
            _ => None,
        })
        .unwrap_or(0)
}

/// The per call numbers of one dump interval. They are ratios of shared
/// counters, so with more than one task they are averages over all tasks.
struct CallStats {
    calls: u64,
    sst_gets: u64,
    sst_bytes: u64,
    waves: u64,
}

impl CallStats {
    fn per_call(&self, value: u64) -> f64 {
        if self.calls == 0 {
            0.0
        } else {
            value as f64 / self.calls as f64
        }
    }
}

async fn dump_stats(
    stats: Arc<DbStatsRecorder>,
    store: Arc<BenchObjectStore>,
    recorder: Arc<DefaultMetricsRecorder>,
) {
    let mut last_stats_dump: Option<Instant> = None;
    let mut first_dump_start: Option<Instant> = None;
    let mut last_calls = stats.calls();
    let mut last_counters = store.counters();
    let mut last_waves = waves(&recorder);
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let operations_since = stats.operations_since(STAT_DUMP_LOOKBACK);
        if let Some((
            range,
            puts_since,
            gets_since,
            puts_bytes_since,
            gets_bytes_since,
            gets_hits_since,
        )) = operations_since
        {
            let interval = range.end - range.start;
            let puts = stats.puts();
            let gets = stats.gets();
            let should_print = match last_stats_dump {
                Some(last_stats_dump) => (range.end - last_stats_dump) >= STAT_DUMP_INTERVAL,
                None => (range.end - range.start) >= STAT_DUMP_INTERVAL,
            };
            first_dump_start = first_dump_start.or(Some(range.start));
            if should_print {
                let put_rate = puts_since as f32 / interval.as_secs() as f32;
                let put_bytes_rate = puts_bytes_since as f32 / interval.as_secs() as f32;
                let get_rate = gets_since as f32 / interval.as_secs() as f32;
                let get_bytes_rate = gets_bytes_since as f32 / interval.as_secs() as f32;
                let get_hit_pct = if gets_since > 0 {
                    gets_hits_since as f32 / gets_since as f32
                } else {
                    0.0
                };

                // The per call numbers cover the time since the last dump.
                let calls = stats.calls();
                let counters = store.counters();
                let waves_now = waves(&recorder);
                let call_stats = CallStats {
                    calls: calls - last_calls,
                    sst_gets: counters.sst_gets - last_counters.sst_gets,
                    sst_bytes: counters.sst_bytes - last_counters.sst_bytes,
                    waves: waves_now - last_waves,
                };
                last_calls = calls;
                last_counters = counters;
                last_waves = waves_now;
                let latencies = stats.take_latencies();

                info!(
                    "stats dump [elapsed {:?}, put/s: {:.3} ({:.3} MiB/s), get/s: {:.3} ({:.3} MiB/s), get db hit ratio: {:.3}%, window: {:?}, total puts: {}, total gets: {}]",
                    range.end.duration_since(first_dump_start.unwrap()).as_secs_f64(),
                    put_rate,
                    put_bytes_rate / 1_048_576.0,
                    get_rate,
                    get_bytes_rate / 1_048_576.0,
                    get_hit_pct * 100f32,
                    range.end - range.start,
                    puts,
                    gets,
                );
                info!(
                    "read calls [calls: {}, p50: {:.3} ms, p99: {:.3} ms, sst gets/call: {:.3}, sst KiB/call: {:.3}, waves/call: {:.3}, other gets: {}]",
                    call_stats.calls,
                    percentile(&latencies, 0.5).as_secs_f64() * 1000.0,
                    percentile(&latencies, 0.99).as_secs_f64() * 1000.0,
                    call_stats.per_call(call_stats.sst_gets),
                    call_stats.per_call(call_stats.sst_bytes) / 1024.0,
                    call_stats.per_call(call_stats.waves),
                    counters.other_gets,
                );
                last_stats_dump = Some(range.end);
            }
        }
    }
}
