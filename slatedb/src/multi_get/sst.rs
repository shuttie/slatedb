//! How `multi_get` reads one SST for a group of its keys.
//!
//! [`SstReader::read`] loads the index, merges the wanted blocks into
//! ranges, reads the ranges in parallel, and scans the blocks for each key.
//! The keys of a range come back when that range is read, so a key does not
//! wait for the slowest range of the SST.
//! The filters, the index, and the blocks go through the cache of the
//! [`TableStore`], so a warm SST sends no object store request.
//!
//! A read returns the raw `RowEntry`s that the SST holds for each key,
//! newest first. The orchestrator ([`super`]) resolves the values with the
//! components of the single-key `get` path.

use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::{self, try_join_all};
use futures::stream::{self, FuturesUnordered};
use futures::{FutureExt, Stream, TryStreamExt};
use log::error;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::{JoinError, JoinSet};

use crate::block_iterator::DataBlockIterator;
use crate::config::MultiGetOptions;
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::filter_policy::NamedFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::iter::IterationOrder;
use crate::partitioned_keyspace;
use crate::reader::{ReadTrace, Reader};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::panic_string;

use super::candidates::SstTarget;

/// The entries that one SST holds for one key of the batch.
pub(crate) struct KeyEntries {
    /// The slot of the key, as in [`PendingKey::idx`].
    pub(crate) idx: usize,
    /// Newest first, and never empty.
    pub(crate) entries: Vec<RowEntry>,
}

/// One result of [`SstReader::read`]: the entries of some keys of the read.
pub(crate) struct SstEntries {
    /// The slots of the keys that this result answers.
    pub(crate) keys: Vec<usize>,
    /// The keys that the SST holds. A key that it does not hold has no entry.
    pub(crate) found: Vec<KeyEntries>,
    /// The index that the read used. The batch keeps it for later reads.
    pub(crate) index: Arc<SsTableIndexOwned>,
}

/// A key from the `multi_get` batch paired with the slot it should resolve into
/// (an index into the orchestrator's deduplicated key list), so per-SST results
/// can be scattered back to the correct accumulator.
#[derive(Clone, Debug)]
pub(crate) struct PendingKey {
    pub(crate) idx: usize,
    pub(crate) key: Bytes,
}

/// A key with the block range that can hold its versions.
struct KeyBlocks {
    key: PendingKey,
    blocks: Range<usize>,
}

/// Reads the SSTs of one batch. It holds only borrows, so a read is a
/// future on the task of the batch, with no clone of the shared state. A
/// read that must fetch blocks clones what it needs into a [`BlockFetch`].
#[derive(Clone, Copy)]
pub(crate) struct SstReader<'a> {
    table_store: &'a Arc<TableStore>,
    /// Each object store request of the batch holds a permit.
    requests: &'a Arc<Semaphore>,
    pub(crate) options: &'a MultiGetOptions,
    pub(crate) read_trace: &'a ReadTrace,
    pub(crate) db_stats: &'a DbStats,
}

impl<'a> SstReader<'a> {
    pub(crate) fn new(
        reader: &'a Reader,
        requests: &'a Arc<Semaphore>,
        options: &'a MultiGetOptions,
        read_trace: &'a ReadTrace,
    ) -> Self {
        Self {
            table_store: &reader.table_store,
            requests,
            options,
            read_trace,
            db_stats: &reader.db_stats,
        }
    }

    /// Load the filters of an SST. With an empty result, the SST has no
    /// filter.
    pub(crate) async fn load_filters(
        self,
        sst: SstTarget,
    ) -> Result<Arc<[NamedFilter]>, SlateDBError> {
        let _permit = self.permit().await;
        let segment = Some(sst.segment);
        self.table_store
            .read_filters(
                &sst.handle,
                true,
                segment,
                self.read_trace,
                Some(&sst.level),
            )
            .await
    }

    /// Read `keys` from an SST whose filters passed them. `index` is the
    /// index from an earlier read of the batch. `filtered` says that the SST
    /// has filters, for the false positive stats.
    ///
    /// The stream yields one result per [`BlockFetch`], when its ranges are
    /// read. Each key is in exactly one result.
    pub(crate) fn read(
        self,
        sst: SstTarget,
        index: Option<Arc<SsTableIndexOwned>>,
        filtered: bool,
        keys: Vec<PendingKey>,
    ) -> impl Stream<Item = Result<SstEntries, SlateDBError>> + 'a {
        let fetches = async move {
            let index = match index {
                Some(index) => index,
                None => {
                    let _permit = self.permit().await;
                    let segment = Some(sst.segment.clone());
                    self.table_store
                        .read_index(
                            &sst.handle,
                            true,
                            segment,
                            self.read_trace,
                            Some(&sst.level),
                        )
                        .await?
                }
            };
            if index.borrow().block_meta().is_empty() {
                let keys = keys.iter().map(|key| key.idx).collect();
                let found = Vec::new();
                let empty = SstEntries { keys, found, index };
                let empty = future::ready(Ok(empty)).boxed();
                return Ok::<_, SlateDBError>(FuturesUnordered::from_iter([empty]));
            }
            let keys = key_blocks(keys, &index);
            let fetches = self.cached_blocks(sst, index, keys, filtered).await;
            Ok(fetches
                .into_iter()
                .map(|fetch| fetch.fetch().boxed())
                .collect())
        };
        stream::once(fetches).try_flatten()
    }

    async fn permit(&self) -> SemaphorePermit<'a> {
        self.requests
            .acquire()
            .await
            .expect("the request semaphore is never closed")
    }

    /// Take the blocks of `keys` from the cache, and merge the block ranges
    /// of the other blocks into runs. A cache lookup here is cheaper than
    /// the load path of `read_blocks_using_index` for one block.
    ///
    /// Returns one fetch per group of runs. A group is one run, or more runs
    /// when the missing blocks of a key are in more than one run. The keys
    /// with all blocks in the cache go into a fetch with no run.
    async fn cached_blocks(
        self,
        sst: SstTarget,
        index: Arc<SsTableIndexOwned>,
        keys: Vec<KeyBlocks>,
        filtered: bool,
    ) -> Vec<BlockFetch> {
        let mut needed: Vec<usize> = Vec::new();
        for key in &keys {
            needed.extend(key.blocks.clone());
        }
        needed.sort_unstable();
        needed.dedup();

        let mut blocks: BTreeMap<usize, Arc<Block>> = BTreeMap::new();
        let mut missing: Vec<usize> = Vec::new();
        for b in needed {
            match self.table_store.cached_block(&sst.handle, &index, b).await {
                Some(block) => {
                    blocks.insert(b, block);
                }
                None => missing.push(b),
            }
        }

        let runs: Vec<Vec<usize>> = coalesce_runs(
            &missing,
            |blocks| {
                self.table_store
                    .block_byte_range(&sst.handle, &index, blocks)
            },
            self.options.coalesce_gap_bytes as u64,
            self.options.max_coalesced_bytes as u64,
        )
        .into_iter()
        .map(|run| {
            let start = missing.partition_point(|&b| b < run.start);
            let end = missing.partition_point(|&b| b < run.end);
            missing[start..end].to_vec()
        })
        .collect();

        // The runs that hold the missing blocks of each key.
        let run_of = |b: usize| runs.partition_point(|run| run[run.len() - 1] < b);
        let key_runs: Vec<Option<Range<usize>>> = keys
            .iter()
            .map(|key| {
                let mut own = key
                    .blocks
                    .clone()
                    .filter(|b| missing.binary_search(b).is_ok());
                let first = own.next()?;
                let last = own.next_back().unwrap_or(first);
                Some(run_of(first)..run_of(last) + 1)
            })
            .collect();
        // A run starts a new group, unless a key needs it and the run before.
        let mut starts = vec![true; runs.len()];
        for key_run in key_runs.iter().flatten() {
            starts[key_run.start + 1..key_run.end].fill(false);
        }
        // Group 0 holds the keys with all blocks in the cache.
        let mut group = 0;
        let group_of_run: Vec<usize> = starts
            .iter()
            .map(|&start| {
                group += usize::from(start);
                group
            })
            .collect();

        let mut fetches: Vec<BlockFetch> = (0..=group)
            .map(|_| BlockFetch {
                table_store: self.table_store.clone(),
                requests: self.requests.clone(),
                db_stats: self.db_stats.clone(),
                cache_blocks: self.options.cache_blocks,
                sst: sst.clone(),
                index: index.clone(),
                keys: Vec::new(),
                blocks: BTreeMap::new(),
                runs: Vec::new(),
                filtered,
            })
            .collect();
        for (run, &group) in runs.into_iter().zip(&group_of_run) {
            fetches[group].runs.push(run);
        }
        for (key, key_run) in keys.into_iter().zip(key_runs) {
            let fetch = &mut fetches[key_run.map_or(0, |run| group_of_run[run.start])];
            for b in key.blocks.clone() {
                if let Some(block) = blocks.get(&b) {
                    fetch.blocks.insert(b, block.clone());
                }
            }
            fetch.keys.push(key);
        }
        fetches.retain(|fetch| !fetch.keys.is_empty());
        fetches
    }
}

/// The rest of one SST read after the cache pass, for some keys of the read:
/// fetch their missing blocks, then scan the blocks of each key. It owns its
/// data, so it can run as a task.
struct BlockFetch {
    table_store: Arc<TableStore>,
    requests: Arc<Semaphore>,
    db_stats: DbStats,
    cache_blocks: bool,
    sst: SstTarget,
    index: Arc<SsTableIndexOwned>,
    keys: Vec<KeyBlocks>,
    /// The blocks from the cache. The fetched blocks join them.
    blocks: BTreeMap<usize, Arc<Block>>,
    /// The missing blocks, grouped by the GET that reads them. A GET reads
    /// from the first to the last block of its group.
    runs: Vec<Vec<usize>>,
    /// The SST has filters, for the false positive stats.
    filtered: bool,
}

impl BlockFetch {
    /// Run the fetch. With no missing block, the scan runs on the batch
    /// task. Otherwise the fetch, the decode, and the scan run on another
    /// thread, and a drop of the future aborts that task.
    async fn fetch(self) -> Result<SstEntries, SlateDBError> {
        if self.runs.is_empty() {
            return self.run().await;
        }
        let mut task = JoinSet::new();
        task.spawn(self.run());
        let joined = task.join_next().await.expect("one task was spawned");
        joined.map_err(fetch_join_error)?
    }

    /// Read all runs in parallel, then scan. A key that the filters passed
    /// and that the SST does not hold is a filter false positive.
    async fn run(mut self) -> Result<SstEntries, SlateDBError> {
        let this = &self;
        let fetched = try_join_all(this.runs.iter().map(|run| async move {
            let _permit = this
                .requests
                .acquire()
                .await
                .expect("the request semaphore is never closed");
            let segment = Some(this.sst.segment.clone());
            if let [block] = run[..] {
                // One block goes through the single-flight load of the cache.
                let blocks = this.table_store.read_blocks_using_index(
                    &this.sst.handle,
                    this.index.clone(),
                    block..block + 1,
                    this.cache_blocks,
                    segment,
                );
                return Ok(Vec::from(blocks.await?));
            }
            // The blocks between the needed ones are read, but not decoded
            // or cached, and the cache is not checked again. So a run is
            // exactly one GET under its permit.
            this.table_store
                .read_sparse_blocks(
                    &this.sst.handle,
                    &this.index,
                    run,
                    this.cache_blocks,
                    segment,
                )
                .await
        }))
        .await?;
        for (run, run_blocks) in self.runs.iter().zip(fetched) {
            for (&block_num, block) in run.iter().zip(run_blocks) {
                self.blocks.insert(block_num, block);
            }
        }

        let sst_version = self.sst.handle.format_version;
        let mut found = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let entries = scan_key(sst_version, key, &self.blocks).await?;
            if !entries.is_empty() {
                let idx = key.key.idx;
                found.push(KeyEntries { idx, entries });
            } else if self.filtered {
                self.db_stats.sst_filter_point_false_positives.increment(1);
            }
        }
        let keys = self.keys.iter().map(|key| key.key.idx).collect();
        let index = self.index;
        Ok(SstEntries { keys, found, index })
    }
}

/// Convert a failed join of a fetch task into an error. A task is cancelled
/// when its runtime shuts down.
fn fetch_join_error(join_err: JoinError) -> SlateDBError {
    let task_name = "multi_get_block_fetch".to_string();
    match join_err.try_into_panic() {
        Ok(panic) => {
            error!(
                "multi_get block fetch task panicked [panic={}]",
                panic_string(&panic)
            );
            SlateDBError::BackgroundTaskPanic(task_name)
        }
        Err(_) => SlateDBError::BackgroundTaskCancelled(task_name),
    }
}

/// Map each key to the block range that may hold its versions. Uses the
/// same `partitions_covering_range` as the single-key path, so a key whose
/// versions span a block boundary is covered.
fn key_blocks(keys: Vec<PendingKey>, index: &SsTableIndexOwned) -> Vec<KeyBlocks> {
    keys.into_iter()
        .map(|key| {
            let blocks = partitioned_keyspace::partitions_covering_range(
                &index.borrow(),
                Included(key.key.as_ref()),
                Included(key.key.as_ref()),
            );
            KeyBlocks { key, blocks }
        })
        .collect()
}

/// Scan the block range of one key for its entries, in iteration
/// (sequence-descending) order. Stops at the first larger key, since later
/// blocks only hold larger keys.
async fn scan_key(
    sst_version: u16,
    key: &KeyBlocks,
    blocks: &BTreeMap<usize, Arc<Block>>,
) -> Result<Vec<RowEntry>, SlateDBError> {
    let key_bytes = key.key.key.as_ref();
    let mut entries = Vec::new();
    for bi in key.blocks.clone() {
        let Some(block) = blocks.get(&bi) else {
            // Every block in the range of a key was read, so this is
            // unreachable in practice.
            break;
        };
        let mut iter =
            DataBlockIterator::new(block.clone(), sst_version, IterationOrder::Ascending)?;
        iter.seek(key_bytes).await?;
        while let Some(entry) = iter.next().await? {
            match entry.key.as_ref().cmp(key_bytes) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => entries.push(entry),
                std::cmp::Ordering::Greater => return Ok(entries),
            }
        }
    }
    Ok(entries)
}

/// Group sorted, deduplicated block indices into fetch ranges. A block joins
/// the last range when the gap to it is at most `gap_bytes` and the merged
/// range is at most `max_bytes`. Adjacent blocks have a gap of 0.
/// `byte_range` gives the byte range of a block range in the SST object.
fn coalesce_runs(
    sorted_blocks: &[usize],
    byte_range: impl Fn(Range<usize>) -> Range<u64>,
    gap_bytes: u64,
    max_bytes: u64,
) -> Vec<Range<usize>> {
    let len = |blocks: Range<usize>| {
        if blocks.is_empty() {
            return 0;
        }
        let bytes = byte_range(blocks);
        bytes.end - bytes.start
    };
    let mut runs: Vec<Range<usize>> = Vec::new();
    for &b in sorted_blocks {
        if let Some(last) = runs.last_mut() {
            if b < last.end {
                continue; // already covered
            }
            if len(last.end..b) <= gap_bytes && len(last.start..b + 1) <= max_bytes {
                last.end = b + 1;
                continue;
            }
        }
        runs.push(b..b + 1);
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::*;

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use rstest::rstest;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::metrics::MetricsRecorderHelper;

    use crate::block_cache_policy::BlockCachePolicy;
    use crate::db_cache::test_utils::TestCache;
    use crate::db_cache::{DbCacheWrapper, SplitCache};
    use crate::db_state::{SsTableId, SsTableView};
    use crate::format::sst::SsTableFormat;
    use crate::reader::SstTraceLevel;
    use crate::tablestore::TableStoreKind;
    use crate::test_utils::{GatedObjectStore, RecordingObjectStore};

    /// Keys per SST. A value has 1 KiB, so a 4 KiB block holds about 3 keys
    /// and the SST has about 60 blocks.
    const NUM_KEYS: usize = 200;

    struct Fixture {
        gated: Arc<GatedObjectStore>,
        recording: Arc<RecordingObjectStore>,
        table_store: Arc<TableStore>,
        view: SsTableView,
    }

    fn key(i: usize) -> Bytes {
        Bytes::from(format!("k{i:03}"))
    }

    fn pending(keys: &[usize]) -> Vec<PendingKey> {
        keys.iter()
            .map(|&i| PendingKey {
                idx: i,
                key: key(i),
            })
            .collect()
    }

    fn new_table_store(
        object_store: Arc<dyn object_store::ObjectStore>,
        cached: bool,
    ) -> Arc<TableStore> {
        let cache = cached.then(|| {
            let split_cache = SplitCache::new()
                .with_block_cache(Some(Arc::new(TestCache::new())))
                .with_meta_cache(Some(Arc::new(TestCache::new())))
                .build();
            Arc::new(DbCacheWrapper::new(
                Arc::new(split_cache),
                &MetricsRecorderHelper::noop(),
                Arc::new(DefaultSystemClock::default()),
                1,
            ))
        });
        Arc::new(TableStore::new(
            object_store,
            SsTableFormat::default(),
            Path::from("/test"),
            cache.map(|c| c as _),
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ))
    }

    /// One SST with the even keys of `0..NUM_KEYS`, read through the stack
    /// gate -> recorder -> memory. A table store with no cache writes the SST,
    /// so the cache of the fixture starts cold.
    async fn fixture(cached: bool) -> Fixture {
        let recording = Arc::new(RecordingObjectStore::new(Arc::new(InMemory::new())));
        let gated = Arc::new(GatedObjectStore::new(recording.clone()));
        let writer = new_table_store(gated.clone(), false);
        let mut builder = writer.table_builder();
        for i in (0..NUM_KEYS).step_by(2) {
            builder
                .add(RowEntry::new_value(&key(i), &[7u8; 1024], 1))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let handle = writer
            .write_sst(&SsTableId::from(ulid::Ulid::new()), &encoded, None)
            .await
            .unwrap();
        recording.clear();
        Fixture {
            gated: gated.clone(),
            recording,
            table_store: new_table_store(gated, cached),
            view: SsTableView::identity(handle),
        }
    }

    impl Fixture {
        fn target(&self) -> SstTarget {
            SstTarget {
                handle: self.view.sst.clone(),
                segment: Bytes::new(),
                level: SstTraceLevel::L0,
            }
        }

        /// Read `keys` with no index from an earlier read.
        async fn read(&self, keys: &[usize], options: MultiGetOptions) -> Vec<usize> {
            self.read_with_index(keys, None, options).await
        }

        /// Read `keys` and return the found keys, sorted.
        async fn read_with_index(
            &self,
            keys: &[usize],
            index: Option<Arc<SsTableIndexOwned>>,
            options: MultiGetOptions,
        ) -> Vec<usize> {
            let results = self.results(keys, index, options).await;
            let found = results.iter().flat_map(|result| &result.found);
            let mut keys: Vec<usize> = found.map(|key| key.idx).collect();
            keys.sort_unstable();
            keys
        }

        /// Read `keys` and return the keys of each result, sorted.
        async fn groups(&self, keys: &[usize], options: MultiGetOptions) -> Vec<Vec<usize>> {
            let results = self.results(keys, None, options).await;
            let mut groups: Vec<Vec<usize>> = results
                .into_iter()
                .map(|result| {
                    let mut keys = result.keys;
                    keys.sort_unstable();
                    keys
                })
                .collect();
            groups.sort_unstable();
            groups
        }

        /// Read `keys` and return all results of the read.
        async fn results(
            &self,
            keys: &[usize],
            index: Option<Arc<SsTableIndexOwned>>,
            options: MultiGetOptions,
        ) -> Vec<SstEntries> {
            let requests = Arc::new(Semaphore::new(options.max_fetch_tasks));
            let read_trace = ReadTrace::new(None);
            let db_stats = DbStats::new(&MetricsRecorderHelper::noop());
            let reader = SstReader {
                table_store: &self.table_store,
                requests: &requests,
                options: &options,
                read_trace: &read_trace,
                db_stats: &db_stats,
            };
            let read = reader.read(self.target(), index, true, pending(keys));
            read.try_collect().await.unwrap()
        }

        /// The index, loaded so that only block reads go to the object store.
        async fn index(&self) -> Arc<SsTableIndexOwned> {
            let trace = ReadTrace::new(None);
            let read = self
                .table_store
                .read_index(&self.view.sst, false, None, &trace, None);
            let index = read.await.unwrap();
            self.recording.clear();
            index
        }

        fn gets(&self) -> usize {
            self.recording.recorded_get_ranges(false).len()
        }
    }

    #[rstest]
    #[case::scattered_blocks(&[0, 100, 198], 0, 3, vec![vec![0], vec![100], vec![198]])]
    #[case::near_blocks_merge(&[0, 20, 40], 64 * 1024, 1, vec![vec![0, 20, 40]])]
    #[case::near_blocks_with_no_merge(&[0, 20, 40], 0, 3, vec![vec![0], vec![20], vec![40]])]
    #[case::two_keys_in_one_block(&[0, 2], 0, 1, vec![vec![0, 2]])]
    #[tokio::test]
    async fn should_send_one_get_per_range(
        #[case] keys: &[usize],
        #[case] coalesce_gap_bytes: usize,
        #[case] expected_block_gets: usize,
        #[case] expected_groups: Vec<Vec<usize>>,
    ) {
        let fx = fixture(false).await;
        let options = MultiGetOptions::default().with_coalesce_gap_bytes(coalesce_gap_bytes);
        let groups = fx.groups(keys, options).await;

        // Each range returns its keys in its own result.
        assert_eq!(groups, expected_groups);
        // One GET for the index, and the block ranges.
        assert_eq!(fx.gets(), 1 + expected_block_gets);
    }

    #[tokio::test]
    async fn should_read_ranges_in_parallel() {
        let fx = fixture(false).await;
        let index = fx.index().await;
        let gate = &fx.gated.get_opts_gate;
        gate.close();
        let before = gate.arrivals();
        let options = MultiGetOptions::default().with_coalesce_gap_bytes(0);

        let read = fx.read_with_index(&[0, 100, 198], Some(index), options);
        let (found, _) = tokio::join!(read, async {
            // All three GETs wait at the gate at the same time.
            gate.wait_for_arrivals(before + 3).await;
            gate.release();
        });

        assert_eq!(found, [0, 100, 198]);
    }

    #[tokio::test]
    async fn should_bound_requests_with_semaphore() {
        let fx = fixture(false).await;
        let index = fx.index().await;
        let gate = &fx.gated.get_opts_gate;
        gate.close();
        let before = gate.arrivals();
        let options = MultiGetOptions::default()
            .with_coalesce_gap_bytes(0)
            .with_max_fetch_tasks(2);

        let read = fx.read_with_index(&[0, 60, 120, 198], Some(index), options);
        let (found, _) = tokio::join!(read, async {
            gate.wait_for_arrivals(before + 2).await;
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            assert_eq!(gate.arrivals(), before + 2);
            gate.release();
        });

        assert_eq!(found, [0, 60, 120, 198]);
        assert_eq!(fx.gets(), 4);
    }

    #[tokio::test]
    async fn should_answer_warm_sst_from_cache() {
        let fx = fixture(true).await;
        let keys = [0, 100, 198];
        fx.read(&keys, MultiGetOptions::default()).await;
        fx.recording.clear();

        // Key 1 is absent. Its block is in the cache, so the read sends no
        // request for it.
        let found = fx.read(&[0, 1, 100, 198], MultiGetOptions::default()).await;

        assert_eq!(found, keys);
        assert_eq!(fx.gets(), 0);
    }

    #[tokio::test]
    async fn should_read_only_the_misses_of_a_partly_warm_sst() {
        let fx = fixture(true).await;
        fx.read(&[0], MultiGetOptions::default()).await;
        fx.recording.clear();

        let found = fx.read(&[0, 198], MultiGetOptions::default()).await;

        assert_eq!(found, [0, 198]);
        // One block read. The index and the block of key 0 came from the
        // cache.
        assert_eq!(fx.gets(), 1);
    }

    /// The run of keys 0 and 40 has the blocks of keys 10, 20 and 30 in
    /// its gap.
    #[tokio::test]
    async fn should_read_a_run_in_one_get_and_not_cache_its_gap() {
        let fx = fixture(true).await;
        let no_merge = MultiGetOptions::default().with_coalesce_gap_bytes(0);
        fx.read(&[10, 30], no_merge.clone()).await;
        fx.recording.clear();

        fx.read(&[0, 40], MultiGetOptions::default()).await;
        assert_eq!(fx.gets(), 1);

        fx.read(&[20], no_merge).await;
        assert_eq!(fx.gets(), 2);
    }

    fn fixed_blocks(blocks: Range<usize>) -> Range<u64> {
        (blocks.start * 4096) as u64..(blocks.end * 4096) as u64
    }

    #[rstest]
    #[case::empty(&[], 0, u64::MAX, vec![])]
    #[case::single(&[7], 0, u64::MAX, vec![7..8])]
    #[case::adjacent_merge_with_gap_0(&[2, 3], 0, u64::MAX, vec![2..4])]
    #[case::gap_0_splits(&[0, 2], 0, u64::MAX, vec![0..1, 2..3])]
    #[case::gap_at_limit_merges(&[0, 2], 4096, u64::MAX, vec![0..3])]
    #[case::gap_above_limit_splits(&[0, 3], 4096, u64::MAX, vec![0..1, 3..4])]
    #[case::size_cap_splits(&[0, 1, 2], 4096, 8192, vec![0..2, 2..3])]
    fn should_coalesce_runs_by_bytes(
        #[case] blocks: &[usize],
        #[case] gap_bytes: u64,
        #[case] max_bytes: u64,
        #[case] expected: Vec<Range<usize>>,
    ) {
        assert_eq!(
            coalesce_runs(blocks, fixed_blocks, gap_bytes, max_bytes),
            expected
        );
    }
}
