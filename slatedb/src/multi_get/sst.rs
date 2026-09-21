//! Batched per-SST point reads for `multi_get`.
//!
//! A batch visits one SST one time for all the keys that the SST can hold. The
//! plan ([`super::plan`]) selects these keys. The visit has two parts:
//!
//! - [`read_from_cache`] runs on the task of the caller. It answers the keys
//!   whose index and blocks are in the block cache, and it sends no object
//!   store request.
//! - [`read_sst`] runs as a spawned task for the keys that missed. It loads
//!   the filters and the index one time, prunes the keys, merges the wanted
//!   blocks into ranges, and reads all ranges in parallel through
//!   [`crate::tablestore::TableStore::read_blocks_using_index`].
//!
//! Both return the raw `RowEntry`s each SST holds for a key, newest
//! first. Sequence/merge/tombstone resolution is intentionally left to the
//! orchestrator ([`super`]) so the final value is produced by
//! the exact same components the single-key `get` path uses.

use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use tokio::sync::Semaphore;

use crate::block_iterator::DataBlockIterator;
use crate::bytes_range::BytesRange;
use crate::config::MultiGetOptions;
use crate::db_state::{SsTableHandle, SsTableView};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::filter_policy::{FilterContext, FilterQuery, NamedFilter};
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::iter::IterationOrder;
use crate::partitioned_keyspace;
use crate::reader::{ReadTrace, SstTraceLevel};
use crate::sst_iter::{all_filters_might_match, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;

/// The result of [`read_from_cache`] for one SST.
pub(crate) struct CacheRead {
    /// Entries of the keys that the cache answered.
    pub(crate) found: Vec<(usize, Vec<RowEntry>)>,
    /// Keys that need [`read_sst`].
    pub(crate) misses: Vec<PendingKey>,
    /// The cached index, so that [`read_sst`] does not look it up again.
    pub(crate) index: Option<Arc<SsTableIndexOwned>>,
}

/// The work of [`read_sst`] for one SST. It owns its data, so it can move
/// into a task.
pub(crate) struct SstRead {
    pub(crate) view: SsTableView,
    pub(crate) keys: Vec<PendingKey>,
    /// `Some` means that the keys are already pruned with these filters.
    pub(crate) filters: Option<Arc<[NamedFilter]>>,
    pub(crate) index: Option<Arc<SsTableIndexOwned>>,
    pub(crate) sst_level: Option<SstTraceLevel>,
    /// Carries the segment of the SST.
    pub(crate) options: SstIteratorOptions,
}

/// A key from the `multi_get` batch paired with the slot it should resolve into
/// (an index into the orchestrator's deduplicated key list), so per-SST results
/// can be scattered back to the correct accumulator.
#[derive(Clone, Debug)]
pub(crate) struct PendingKey {
    pub(crate) idx: usize,
    pub(crate) key: Bytes,
}

/// One key that survived pruning, paired with the contiguous block range that
/// may hold its versions.
struct Candidate {
    idx: usize,
    key: Bytes,
    blocks: Range<usize>,
}

/// Answer the keys of one SST from the block cache. A key is a hit when the
/// index and each block of its range are in the cache. This function sends no
/// object store request.
///
/// The plan already pruned `keys` by range and by the cached filters.
/// `filters_present` says that a filter passed them, for the false positive
/// stats.
///
/// Entries are in sequence-descending (newest-first) order. They are not
/// sequence-filtered or merge-resolved here.
pub(crate) async fn read_from_cache(
    handle: &SsTableHandle,
    keys: &[PendingKey],
    filters_present: bool,
    table_store: &TableStore,
    db_stats: Option<&DbStats>,
) -> Result<CacheRead, SlateDBError> {
    let survivors: Vec<&PendingKey> = keys.iter().collect();
    let mut read = CacheRead {
        found: Vec::new(),
        misses: Vec::new(),
        index: table_store.cached_index(handle).await,
    };
    let Some(index) = read.index.clone() else {
        read.misses = keys.to_vec();
        return Ok(read);
    };
    if index.borrow().block_meta().is_empty() {
        return Ok(read);
    }

    // One cache lookup per block. `None` records a block that is not cached.
    let mut lookups: BTreeMap<usize, Option<Arc<Block>>> = BTreeMap::new();
    let mut hits = Vec::new();
    for cand in map_candidates(&survivors, &index) {
        let mut cached = true;
        for bi in cand.blocks.clone() {
            cached &= match lookups.get(&bi) {
                Some(block) => block.is_some(),
                None => {
                    let block = table_store.cached_block(handle, &index, bi).await;
                    let hit = block.is_some();
                    lookups.insert(bi, block);
                    hit
                }
            };
        }
        if cached {
            hits.push(cand);
        } else {
            read.misses.push(PendingKey {
                idx: cand.idx,
                key: cand.key,
            });
        }
    }
    let blocks = lookups
        .into_iter()
        .filter_map(|(bi, block)| block.map(|block| (bi, block)))
        .collect();
    read.found = scan_candidates(
        handle.format_version,
        &hits,
        &blocks,
        filters_present,
        db_stats,
    )
    .await?;
    Ok(read)
}

/// Read the keys of one SST from the object store. Each object store request
/// holds a permit of `requests`, the request semaphore of the batch.
///
/// The output has the same form as [`CacheRead::found`].
pub(crate) async fn read_sst(
    read: SstRead,
    table_store: Arc<TableStore>,
    requests: Arc<Semaphore>,
    batch_options: MultiGetOptions,
    read_trace: ReadTrace,
    db_stats: Option<DbStats>,
) -> Result<Vec<(usize, Vec<RowEntry>)>, SlateDBError> {
    let SstRead {
        view,
        keys,
        filters,
        index,
        sst_level,
        options,
    } = read;
    let handle = &view.sst;
    let db_stats = db_stats.as_ref();

    // Filters first, like the single-key path, so an SST that rules out every
    // key never reads its index.
    let (filters, survivors) = match filters {
        Some(filters) => (filters, keys.iter().collect()),
        None => {
            let _permit = request_permit(&requests).await;
            let filters = table_store
                .read_filters(
                    handle,
                    options.cache_metadata,
                    options.segment.clone(),
                    &read_trace,
                    sst_level.as_ref(),
                )
                .await?;
            let survivors = filter_keys(&keys, &filters, &options.filter_context, db_stats);
            (filters, survivors)
        }
    };
    if survivors.is_empty() {
        return Ok(Vec::new());
    }

    let index = match index {
        Some(index) => index,
        None => {
            let _permit = request_permit(&requests).await;
            table_store
                .read_index(
                    handle,
                    options.cache_metadata,
                    options.segment.clone(),
                    &read_trace,
                    sst_level.as_ref(),
                )
                .await?
        }
    };
    if index.borrow().block_meta().is_empty() {
        return Ok(Vec::new());
    }
    let candidates = map_candidates(&survivors, &index);

    let blocks = fetch_candidate_blocks(
        handle,
        &index,
        &candidates,
        &options,
        &batch_options,
        &table_store,
        &requests,
    )
    .await?;

    scan_candidates(
        handle.format_version,
        &candidates,
        &blocks,
        !filters.is_empty(),
        db_stats,
    )
    .await
}

async fn request_permit(requests: &Semaphore) -> tokio::sync::SemaphorePermit<'_> {
    requests
        .acquire()
        .await
        .expect("the request semaphore is never closed")
}

/// Visible-range projection (segments / clones). For identity views this
/// also prunes keys outside the SST's physical key range.
pub(crate) fn view_holds_key(view: &SsTableView, key: &[u8]) -> bool {
    view.calculate_view_range(BytesRange::from_slice(key..=key))
        .is_some()
}

/// Prune keys by bloom filter. Filter positives and negatives are recorded
/// here. False positives are recorded after scanning (see
/// [`scan_candidates`]).
pub(crate) fn filter_keys<'a>(
    keys: &'a [PendingKey],
    filters: &[NamedFilter],
    filter_context: &Option<FilterContext>,
    db_stats: Option<&DbStats>,
) -> Vec<&'a PendingKey> {
    if filters.is_empty() {
        return keys.iter().collect();
    }
    let mut survivors = Vec::with_capacity(keys.len());
    for pk in keys {
        let query = FilterQuery::point(pk.key.clone()).with_context(filter_context.clone());
        if !all_filters_might_match(filters, &query) {
            if let Some(stats) = db_stats {
                stats.sst_filter_point_negatives.increment(1);
            }
            continue;
        }
        if let Some(stats) = db_stats {
            stats.sst_filter_point_positives.increment(1);
        }
        survivors.push(pk);
    }
    survivors
}

/// Map each survivor to the block range that may hold its versions. Uses the
/// same `partitions_covering_range` as the single-key path, so a key whose
/// versions span a block boundary is covered.
fn map_candidates(survivors: &[&PendingKey], index: &Arc<SsTableIndexOwned>) -> Vec<Candidate> {
    survivors
        .iter()
        .map(|pk| {
            let blocks = partitioned_keyspace::partitions_covering_range(
                &index.borrow(),
                Included(pk.key.as_ref()),
                Included(pk.key.as_ref()),
            );
            Candidate {
                idx: pk.idx,
                key: pk.key.clone(),
                blocks,
            }
        })
        .collect()
}

/// Step 3: collect the union of the candidates' block ranges, coalesce them into
/// runs, and fetch all runs in parallel. `read_blocks_using_index` handles
/// caching and decode.
async fn fetch_candidate_blocks(
    handle: &SsTableHandle,
    index: &Arc<SsTableIndexOwned>,
    candidates: &[Candidate],
    options: &SstIteratorOptions,
    batch_options: &MultiGetOptions,
    table_store: &TableStore,
    requests: &Semaphore,
) -> Result<BTreeMap<usize, Arc<Block>>, SlateDBError> {
    let mut needed: Vec<usize> = Vec::new();
    for cand in candidates {
        needed.extend(cand.blocks.clone());
    }
    needed.sort_unstable();
    needed.dedup();

    let runs = coalesce_runs(
        &needed,
        |blocks| table_store.block_byte_range(handle, index, blocks),
        batch_options.coalesce_gap_bytes as u64,
        batch_options.max_coalesced_bytes as u64,
    );
    let fetched = try_join_all(runs.iter().map(|run| async move {
        let _permit = request_permit(requests).await;
        table_store
            .read_blocks_using_index(
                handle,
                index.clone(),
                run.clone(),
                options.cache_blocks,
                options.segment.clone(),
            )
            .await
    }))
    .await?;

    let mut blocks: BTreeMap<usize, Arc<Block>> = BTreeMap::new();
    for (run, run_blocks) in runs.iter().zip(fetched) {
        for (offset, block) in run_blocks.into_iter().enumerate() {
            blocks.insert(run.start + offset, block);
        }
    }
    Ok(blocks)
}

/// Step 4: scan each candidate's block range for its key, collecting that key's
/// `RowEntry`s newest-first. A candidate that yields nothing despite passing the
/// bloom filter is recorded as a filter false positive.
async fn scan_candidates(
    sst_version: u16,
    candidates: &[Candidate],
    blocks: &BTreeMap<usize, Arc<Block>>,
    filters_present: bool,
    db_stats: Option<&DbStats>,
) -> Result<Vec<(usize, Vec<RowEntry>)>, SlateDBError> {
    let mut out = Vec::with_capacity(candidates.len());
    for cand in candidates {
        let entries = scan_candidate_key(sst_version, cand, blocks).await?;
        if entries.is_empty() {
            if filters_present {
                if let Some(stats) = db_stats {
                    stats.sst_filter_point_false_positives.increment(1);
                }
            }
        } else {
            out.push((cand.idx, entries));
        }
    }
    Ok(out)
}

/// Scan a single candidate's block range for its key, collecting matching
/// entries in iteration (sequence-descending) order. Stops as soon as a larger
/// key is seen, since later blocks only hold larger keys.
async fn scan_candidate_key(
    sst_version: u16,
    cand: &Candidate,
    blocks: &BTreeMap<usize, Arc<Block>>,
) -> Result<Vec<RowEntry>, SlateDBError> {
    let mut entries = Vec::new();
    for bi in cand.blocks.clone() {
        let Some(block) = blocks.get(&bi) else {
            // Every block in a candidate's range was added to `needed` and
            // fetched, so this is unreachable in practice.
            break;
        };
        let mut iter =
            DataBlockIterator::new(block.clone(), sst_version, IterationOrder::Ascending)?;
        iter.seek(cand.key.as_ref()).await?;
        while let Some(entry) = iter.next().await? {
            match entry.key.as_ref().cmp(cand.key.as_ref()) {
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
    use crate::db_state::SsTableId;
    use crate::format::sst::SsTableFormat;
    use crate::reader::ReadTrace;
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
        fn sst_read(&self, keys: &[usize]) -> SstRead {
            SstRead {
                view: self.view.clone(),
                keys: pending(keys),
                filters: None,
                index: None,
                sst_level: None,
                options: SstIteratorOptions::default(),
            }
        }

        async fn read_sst(
            &self,
            read: SstRead,
            batch_options: MultiGetOptions,
        ) -> Vec<(usize, Vec<RowEntry>)> {
            let requests = Arc::new(Semaphore::new(batch_options.max_fetch_tasks));
            read_sst(
                read,
                self.table_store.clone(),
                requests,
                batch_options,
                ReadTrace::new(None),
                None,
            )
            .await
            .unwrap()
        }

        async fn read_from_cache(&self, keys: &[usize]) -> CacheRead {
            read_from_cache(
                &self.view.sst,
                &pending(keys),
                true,
                &self.table_store,
                None,
            )
            .await
            .unwrap()
        }

        /// A read with the filters and the index loaded, so that only block
        /// reads go to the object store.
        async fn block_only_read(&self, keys: &[usize]) -> SstRead {
            let handle = &self.view.sst;
            let trace = ReadTrace::new(None);
            let mut read = self.sst_read(keys);
            read.filters = Some(
                self.table_store
                    .read_filters(handle, false, None, &trace, None)
                    .await
                    .unwrap(),
            );
            read.index = Some(
                self.table_store
                    .read_index(handle, false, None, &trace, None)
                    .await
                    .unwrap(),
            );
            self.recording.clear();
            read
        }

        fn gets(&self) -> usize {
            self.recording.recorded_get_ranges(false).len()
        }
    }

    fn found_keys(found: &[(usize, Vec<RowEntry>)]) -> Vec<usize> {
        let mut keys: Vec<usize> = found.iter().map(|(idx, _)| *idx).collect();
        keys.sort_unstable();
        keys
    }

    #[tokio::test]
    async fn should_skip_index_read_when_filters_reject_every_key() {
        let fx = fixture(false).await;
        // Odd keys are absent.
        let result = fx
            .read_sst(fx.sst_read(&[1, 101]), MultiGetOptions::default())
            .await;

        assert!(result.is_empty());
        // Only the filter read reaches the object store.
        assert_eq!(fx.gets(), 1);
    }

    #[rstest]
    #[case::scattered_blocks(&[0, 100, 198], 0, 3)]
    #[case::near_blocks_merge(&[0, 20, 40], 64 * 1024, 1)]
    #[case::near_blocks_with_no_merge(&[0, 20, 40], 0, 3)]
    #[case::two_keys_in_one_block(&[0, 2], 0, 1)]
    #[tokio::test]
    async fn should_send_one_get_per_range(
        #[case] keys: &[usize],
        #[case] coalesce_gap_bytes: usize,
        #[case] expected_block_gets: usize,
    ) {
        let fx = fixture(false).await;
        let options = MultiGetOptions::default().with_coalesce_gap_bytes(coalesce_gap_bytes);
        let result = fx.read_sst(fx.sst_read(keys), options).await;

        assert_eq!(found_keys(&result), keys);
        // One GET for the filters, one for the index, and the block ranges.
        assert_eq!(fx.gets(), 2 + expected_block_gets);
    }

    #[tokio::test]
    async fn should_read_ranges_in_parallel() {
        let fx = fixture(false).await;
        let read = fx.block_only_read(&[0, 100, 198]).await;
        let gate = &fx.gated.get_opts_gate;
        gate.close();
        let before = gate.arrivals();
        let options = MultiGetOptions::default().with_coalesce_gap_bytes(0);

        let (result, _) = tokio::join!(fx.read_sst(read, options), async {
            // All three GETs wait at the gate at the same time.
            gate.wait_for_arrivals(before + 3).await;
            gate.release();
        });

        assert_eq!(found_keys(&result), [0, 100, 198]);
    }

    #[tokio::test]
    async fn should_bound_requests_with_semaphore() {
        let fx = fixture(false).await;
        let read = fx.block_only_read(&[0, 60, 120, 198]).await;
        let gate = &fx.gated.get_opts_gate;
        gate.close();
        let before = gate.arrivals();
        let options = MultiGetOptions::default()
            .with_coalesce_gap_bytes(0)
            .with_max_fetch_tasks(2);

        let (result, _) = tokio::join!(fx.read_sst(read, options), async {
            gate.wait_for_arrivals(before + 2).await;
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            assert_eq!(gate.arrivals(), before + 2);
            gate.release();
        });

        assert_eq!(found_keys(&result), [0, 60, 120, 198]);
        assert_eq!(fx.gets(), 4);
    }

    #[tokio::test]
    async fn should_answer_warm_sst_from_cache() {
        let fx = fixture(true).await;
        let keys = [0, 100, 198];
        let cold = fx.read_from_cache(&keys).await;
        assert!(cold.found.is_empty());
        assert_eq!(cold.misses.len(), keys.len());
        fx.read_sst(fx.sst_read(&keys), MultiGetOptions::default())
            .await;
        fx.recording.clear();

        // Key 1 is absent. Its block is in the cache, so it is a hit with no
        // entries.
        let warm = fx.read_from_cache(&[0, 1, 100, 198]).await;

        assert_eq!(found_keys(&warm.found), keys);
        assert!(warm.misses.is_empty());
        assert_eq!(fx.gets(), 0);
    }

    #[tokio::test]
    async fn should_read_only_the_misses_of_a_partly_warm_sst() {
        let fx = fixture(true).await;
        fx.read_sst(fx.sst_read(&[0]), MultiGetOptions::default())
            .await;
        fx.recording.clear();

        let cached = fx.read_from_cache(&[0, 198]).await;
        assert_eq!(found_keys(&cached.found), [0]);
        let missed: Vec<usize> = cached.misses.iter().map(|pk| pk.idx).collect();
        assert_eq!(missed, [198]);
        assert!(cached.index.is_some());

        let mut read = fx.sst_read(&[]);
        read.keys = cached.misses;
        read.filters = fx.table_store.cached_filters(&fx.view.sst).await;
        read.index = cached.index;
        let result = fx.read_sst(read, MultiGetOptions::default()).await;

        assert_eq!(found_keys(&result), [198]);
        // One block read. The filters and the index came from the cache.
        assert_eq!(fx.gets(), 1);
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
