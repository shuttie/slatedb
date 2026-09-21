//! Batched point reads (`multi_get`, RFC 0035).
//!
//! - [`plan`]: which SSTs can hold which keys. Memory only.
//! - [`sst`]: how one SST is read for its keys.
//! - This module: the orchestrator. It reads the write batch and the
//!   memtables, builds the plan, reads the SSTs, and resolves each key with
//!   the components of the single-key `get` path.

mod plan;
mod sst;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::Instrument;

use crate::batch::{WriteBatch, WriteBatchIterator};
use crate::bytes_range::BytesRange;
use crate::config::MultiGetOptions;
use crate::db_iter::GetIterator;
use crate::error::SlateDBError;
use crate::filter_policy::NamedFilter;
use crate::iter::{IterationOrder, RowEntryIterator, VecRowIterator};
use crate::mem_table::KVTable;
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorRequiredIterator};
use crate::reader::{DbStateReader, ReadTrace, Reader};
use crate::sst_iter::{task_join_error, SstIteratorOptions};
use crate::types::{RowEntry, ValueDeletable};

use plan::{candidate_ssts, pick_first, pick_next, BatchKeys, FilterState, Plan};
use sst::{load_filters, read_from_cache, read_sst, KeyEntries, PendingKey, SstEntries, SstRead};

/// The result of one SST read of a wave.
struct SstResult {
    /// Index into [`Plan::ssts`].
    sst: usize,
    entries: SstEntries,
}

/// The filters that a wave loaded for one SST.
struct LoadedFilters {
    /// Index into [`Plan::ssts`].
    sst: usize,
    filters: Arc<[NamedFilter]>,
}

impl Reader {
    /// Batched point lookup: resolve many keys against a single consistent
    /// snapshot, returning one slot per input key (order- and
    /// duplicate-preserving; `None` for a missing key).
    ///
    /// Semantics match a loop of [`Self::get_key_value_with_options`] over one
    /// snapshot; the I/O is batched. Each candidate SST is visited once for all
    /// of the keys that might live in it, instead of once per key (see
    /// [`sst`]).
    ///
    /// Value resolution deliberately reuses the single-key machinery: each key's
    /// candidate versions are gathered newest-first and replayed through the same
    /// [`GetIterator`] + [`MergeOperatorIterator`] pipeline that `get` uses, so
    /// the result is identical to calling `get` for each key.
    ///
    /// Mirrors the arguments of [`Self::get_key_value_with_options`]: a shared
    /// `db_state` view, an optional per-transaction `write_batch` (consulted
    /// first), and an optional `max_seq` bound (folded with durability/dirty via
    /// `prepare_max_seq`).
    pub(crate) async fn multi_get_with_options<K: AsRef<[u8]> + Sync>(
        &self,
        keys: &[K],
        options: &MultiGetOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&WriteBatch>,
        max_seq: Option<u64>,
    ) -> Result<Vec<Option<RowEntry>>, SlateDBError> {
        let read_trace = ReadTrace::new_multi_get(options.tracing_options.clone(), keys.len());
        let read = self.multi_get_with_options_inner(
            keys,
            options,
            db_state,
            write_batch,
            max_seq,
            read_trace.clone(),
        );
        read.instrument(read_trace.read_span()).await
    }

    async fn multi_get_with_options_inner<K: AsRef<[u8]> + Sync>(
        &self,
        keys: &[K],
        options: &MultiGetOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&WriteBatch>,
        max_seq: Option<u64>,
        read_trace: ReadTrace,
    ) -> Result<Vec<Option<RowEntry>>, SlateDBError> {
        self.db_stats.multi_get_requests.increment(1);
        self.db_stats.multi_get_keys.increment(keys.len() as u64);
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);

        // Duplicates resolve one time. The results are scattered back to every
        // input position at the end.
        let batch = BatchKeys::new(keys);
        let n = batch.keys.len();

        // Per unique key: write-batch entries (unfiltered, highest precedence),
        // everything-else entries (memtable + on-disk), and whether a base
        // (Value/Tombstone) has been found yet. `append_versions` is the sole
        // writer into `acc`, so every entry there is already max_seq filtered.
        let mut wb_acc: Vec<Vec<RowEntry>> = vec![Vec::new(); n];
        let mut acc: Vec<Vec<RowEntry>> = vec![Vec::new(); n];
        let mut resolved: Vec<bool> = vec![false; n];

        // 1. Write batch (transaction only). Entries carry seq u64::MAX and are
        //    not max_seq filtered (hence `None`), matching the single-key get
        //    path. One full-range pass over the batch instead of a point
        //    iterator per key; batches are memory-bounded, so the full walk is
        //    cheap even for small key lists.
        if let Some(wb) = write_batch {
            let mut iter = WriteBatchIterator::new(
                wb,
                BytesRange::from(..),
                IterationOrder::Ascending,
                u64::MAX,
                None,
                None,
            );
            iter.init().await?;
            while let Some(entry) = iter.next().await? {
                if let Some(u) = batch.position(entry.key.as_ref()) {
                    if !matches!(entry.value, ValueDeletable::Merge(_)) {
                        resolved[u] = true;
                    }
                    wb_acc[u].push(entry);
                }
            }
        }

        // 2. Active memtable, then immutable memtables (newest-first).
        let memtable = db_state.memtable();
        read_memtable_layer(
            &memtable,
            &batch.keys,
            max_seq,
            &read_trace,
            &mut acc,
            &mut resolved,
        );
        for imm in db_state.imm_memtable() {
            read_memtable_layer(
                &imm.table(),
                &batch.keys,
                max_seq,
                &read_trace,
                &mut acc,
                &mut resolved,
            );
        }

        // 3. Plan: the SSTs that can hold each open key. Only filters that are
        //    in the cache take part, so the plan sends no request.
        let mut ssts = candidate_ssts(db_state.core(), &batch.keys, &resolved);
        for sst in ssts.iter_mut() {
            sst.filters = self.table_store.cached_filters(&sst.view.sst).await;
        }
        let mut plan = Plan::new(ssts, n, &options.filter_context, Some(&self.db_stats));
        // A key with no candidates has nothing to read.
        for (u, candidates) in plan.candidates.iter().enumerate() {
            resolved[u] |= candidates.is_empty();
        }

        // 4. Read, in waves. Each open key reads its next candidates, newest
        //    first, until it has a base value or no candidate is left.
        let requests = Arc::new(Semaphore::new(options.max_fetch_tasks.max(1)));
        let mut open: Vec<usize> = (0..n).filter(|&u| !resolved[u]).collect();
        let mut waves: u64 = 0;
        while !open.is_empty() {
            waves += 1;
            let pick = |plan: &Plan, u: usize| {
                // An open key holds only merge operands.
                let has_operand = !wb_acc[u].is_empty() || !acc[u].is_empty();
                match waves {
                    1 => pick_first(&plan.candidates[u], has_operand),
                    _ => pick_next(&plan.candidates[u], has_operand, options.lookahead),
                }
            };
            // An UNKNOWN SST is free to pick, but only for its filter load.
            // After the load, its keys are POSITIVE or gone, and the second
            // pick applies the limit to them.
            let unknown: BTreeSet<usize> = open
                .iter()
                .flat_map(|&u| &plan.candidates[u][..pick(&plan, u)])
                .filter(|c| c.state == FilterState::Unknown)
                .map(|c| c.sst)
                .collect();
            let loads = self.load_filters(&plan, unknown, options, &requests, &read_trace);
            for LoadedFilters { sst, filters } in loads.await? {
                let stats = Some(&self.db_stats);
                plan.apply_filters(sst, filters, &resolved, &options.filter_context, stats);
            }
            let mut wave: BTreeMap<usize, Vec<PendingKey>> = BTreeMap::new();
            for &u in &open {
                let picked = pick(&plan, u);
                for candidate in plan.candidates[u].drain(..picked) {
                    wave.entry(candidate.sst).or_default().push(PendingKey {
                        idx: u,
                        key: batch.keys[u].clone(),
                    });
                }
            }

            let results = self
                .read_wave(&plan, wave, options, &requests, &read_trace)
                .await?;
            // `append_versions` re-filters by max_seq and stops each key at its
            // first surviving Value/Tombstone, so the entries of older SSTs are
            // dropped exactly as in the single-key walk. Memtable entries are
            // already in `acc` and stay first (they are newer than anything on
            // disk).
            for SstResult { sst, entries } in results {
                if entries.index.is_some() {
                    plan.ssts[sst].index = entries.index;
                }
                for KeyEntries { idx: u, entries } in entries.found {
                    if !resolved[u] {
                        append_versions(entries, max_seq, &mut acc[u], &mut resolved[u]);
                    }
                }
            }
            open.retain(|&u| !resolved[u] && !plan.candidates[u].is_empty());
        }
        self.db_stats.multi_get_waves.increment(waves);
        read_trace.read_span().record("waves", waves);

        // 5. Resolve each unique key, then scatter to input positions.
        let mut values: Vec<Option<RowEntry>> = Vec::with_capacity(n);
        for u in 0..n {
            let wb_entries = std::mem::take(&mut wb_acc[u]);
            let rest_entries = std::mem::take(&mut acc[u]);
            values.push(
                self.resolve_entry(&batch.keys[u], wb_entries, rest_entries)
                    .await?,
            );
        }
        if keys.len() == n {
            // No duplicates: each value moves to its one slot.
            return Ok(batch.slots.iter().map(|&u| values[u].take()).collect());
        }
        Ok(batch.slots.iter().map(|&u| values[u].clone()).collect())
    }

    /// Load the filters of the given SSTs of the plan, all in parallel.
    async fn load_filters(
        &self,
        plan: &Plan,
        ssts: BTreeSet<usize>,
        options: &MultiGetOptions,
        requests: &Arc<Semaphore>,
        read_trace: &ReadTrace,
    ) -> Result<Vec<LoadedFilters>, SlateDBError> {
        // A drop of the `JoinSet` aborts its tasks.
        let mut tasks: JoinSet<Result<LoadedFilters, SlateDBError>> = JoinSet::new();
        for sst in ssts {
            let plan_sst = &plan.ssts[sst];
            let (view, level) = (plan_sst.view.clone(), plan_sst.level.clone());
            let sst_options = sst_options(options, &plan_sst.segment);
            let table_store = self.table_store.clone();
            let (requests, read_trace) = (requests.clone(), read_trace.clone());
            tasks.spawn(async move {
                let filters = load_filters(
                    &view,
                    &sst_options,
                    Some(&level),
                    &table_store,
                    &requests,
                    &read_trace,
                );
                let filters = filters.await?;
                Ok(LoadedFilters { sst, filters })
            });
        }
        let mut loaded = Vec::with_capacity(tasks.len());
        while let Some(joined) = tasks.join_next().await {
            let result =
                joined.map_err(|e| task_join_error(e, "multi_get_filter_load".to_string()))?;
            loaded.push(result?);
        }
        Ok(loaded)
    }

    /// Read each SST of one wave for its keys. Each SST answers from the cache
    /// first, inline. Only the keys that miss go to a task. The results are in
    /// plan order, newest first.
    async fn read_wave(
        &self,
        plan: &Plan,
        wave: BTreeMap<usize, Vec<PendingKey>>,
        options: &MultiGetOptions,
        requests: &Arc<Semaphore>,
        read_trace: &ReadTrace,
    ) -> Result<Vec<SstResult>, SlateDBError> {
        let mut results: Vec<SstResult> = Vec::with_capacity(wave.len());
        // A drop of the `JoinSet` aborts its tasks.
        let mut tasks: JoinSet<Result<SstResult, SlateDBError>> = JoinSet::new();
        for (sst, keys) in wave {
            let plan_sst = &plan.ssts[sst];
            let filters_present = plan_sst.filters.as_ref().is_some_and(|f| !f.is_empty());
            let mut entries = read_from_cache(
                &plan_sst.view.sst,
                &keys,
                filters_present,
                &self.table_store,
                Some(&self.db_stats),
            )
            .await?;
            let misses = std::mem::take(&mut entries.misses);
            // With no cache, the index comes from an earlier wave.
            let index = entries.index.clone().or_else(|| plan_sst.index.clone());
            results.push(SstResult { sst, entries });
            if misses.is_empty() {
                continue;
            }
            let read = SstRead {
                view: plan_sst.view.clone(),
                keys: misses,
                filters: plan_sst.filters.clone(),
                index,
                sst_level: Some(plan_sst.level.clone()),
                options: sst_options(options, &plan_sst.segment),
            };
            let sst_read = read_sst(
                read,
                self.table_store.clone(),
                requests.clone(),
                options.clone(),
                read_trace.clone(),
                Some(self.db_stats.clone()),
            );
            tasks.spawn(async move {
                let entries = sst_read.await?;
                Ok(SstResult { sst, entries })
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let result =
                joined.map_err(|e| task_join_error(e, "multi_get_sst_read".to_string()))?;
            results.push(result?);
        }
        // Two results of one SST have different keys.
        results.sort_by_key(|result| result.sst);
        Ok(results)
    }

    /// Produce the final entry for one key by replaying its accumulated versions
    /// through the same components the single-key `get` path uses: a
    /// [`GetIterator`] wrapped in the merge operator. Returns `None` for a
    /// missing or deleted key.
    ///
    /// No max_seq filtering happens here: `rest_entries` were already filtered
    /// on append (see [`append_versions`]) and write-batch entries are exempt
    /// by design.
    async fn resolve_entry(
        &self,
        key: &Bytes,
        wb_entries: Vec<RowEntry>,
        rest_entries: Vec<RowEntry>,
    ) -> Result<Option<RowEntry>, SlateDBError> {
        if wb_entries.is_empty() && rest_entries.is_empty() {
            return Ok(None);
        }
        let wb_iter: Box<dyn RowEntryIterator + 'static> =
            Box::new(VecRowIterator::new(wb_entries));
        let rest_iter: Box<dyn RowEntryIterator + 'static> =
            Box::new(VecRowIterator::new(rest_entries));
        let get_iter = GetIterator::new(
            key.clone(),
            wb_iter,
            std::iter::empty::<Box<dyn RowEntryIterator + 'static>>(),
            rest_iter,
        );
        let mut top: Box<dyn RowEntryIterator + 'static> = match self.read_merge_operator.clone() {
            Some(merge_operator) => Box::new(MergeOperatorIterator::new(
                merge_operator,
                get_iter,
                true,
                None,
            )),
            None => Box::new(MergeOperatorRequiredIterator::new(get_iter)),
        };
        top.init().await?;
        Ok(match top.next().await? {
            Some(entry) if entry.value.is_tombstone() => None,
            other => other,
        })
    }
}

/// The read options of one SST of the batch.
fn sst_options(options: &MultiGetOptions, segment: &Bytes) -> SstIteratorOptions {
    SstIteratorOptions {
        cache_blocks: options.cache_blocks,
        filter_context: options.filter_context.clone(),
        segment: Some(segment.clone()),
        ..SstIteratorOptions::default()
    }
}

/// Append versions of one key to its accumulator, newest-first. Entries with
/// `seq > max_seq` are dropped (not visible at this snapshot) — the filter runs
/// *before* the resolve check, so a too-new `Value` must not resolve the key.
/// The first appended `Value` or `Tombstone` marks the key resolved and stops
/// the append: everything older is shadowed and would never be read by the
/// resolution iterators anyway. `Merge` operands keep accumulating (they
/// legitimately span layers).
fn append_versions(
    entries: Vec<RowEntry>,
    max_seq: Option<u64>,
    acc: &mut Vec<RowEntry>,
    resolved: &mut bool,
) {
    for entry in entries {
        if max_seq.is_some_and(|ms| entry.seq > ms) {
            continue;
        }
        let is_base = !matches!(entry.value, ValueDeletable::Merge(_));
        acc.push(entry);
        if is_base {
            *resolved = true;
            return;
        }
    }
}

/// Probe an in-memory table for every still-pending key, appending its versions.
fn read_memtable_layer(
    table: &KVTable,
    unique_keys: &[Bytes],
    max_seq: Option<u64>,
    read_trace: &ReadTrace,
    acc: &mut [Vec<RowEntry>],
    resolved: &mut [bool],
) {
    for (u, key) in unique_keys.iter().enumerate() {
        if resolved[u] {
            continue;
        }
        let entries = kv_table_get_versions(table, key, read_trace);
        append_versions(entries, max_seq, &mut acc[u], &mut resolved[u]);
    }
}

/// Collect all versions of `key` from a `KVTable`, newest-first. The table has
/// no direct point-get, so this uses a `key..=key` range iterator (in-memory,
/// O(log n), no I/O — hence the synchronous drain via `next_sync`).
fn kv_table_get_versions(table: &KVTable, key: &Bytes, read_trace: &ReadTrace) -> Vec<RowEntry> {
    let mut iter = table.range(
        key.clone()..=key.clone(),
        IterationOrder::Ascending,
        read_trace.clone(),
    );
    let mut out = Vec::new();
    while let Some(entry) = iter.next_sync() {
        if entry.key.as_ref() != key.as_ref() {
            break;
        }
        out.push(entry);
    }
    out
}
