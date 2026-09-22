//! Batched point reads (`multi_get`, RFC 0035).
//!
//! - [`plan`]: which SSTs can hold which keys. Memory only.
//! - [`sst`]: how one SST is read for its keys.
//! - [`pipeline`]: the read phase. Each key reads its SSTs newest first,
//!   and no key waits for the reads of another key.
//! - This module: the orchestrator. It reads the write batch and the
//!   memtables, builds the plan, runs the pipeline, and resolves each key
//!   with the components of the single-key `get` path.

mod pipeline;
mod plan;
mod sst;

use bytes::Bytes;
use tracing::Instrument;

use crate::batch::{WriteBatch, WriteBatchIterator};
use crate::bytes_range::BytesRange;
use crate::config::MultiGetOptions;
use crate::db_iter::GetIterator;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator, VecRowIterator};
use crate::mem_table::KVTable;
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorRequiredIterator};
use crate::reader::{DbStateReader, ReadTrace, Reader};
use crate::sst_iter::SstIteratorOptions;
use crate::types::{RowEntry, ValueDeletable};

use pipeline::Pipeline;
use plan::{candidate_ssts, BatchKeys, Plan};

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
                // Keep the in-memory walk cooperative.
                tokio::task::coop::consume_budget().await;
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
        )
        .await;
        for imm in db_state.imm_memtable() {
            read_memtable_layer(
                &imm.table(),
                &batch.keys,
                max_seq,
                &read_trace,
                &mut acc,
                &mut resolved,
            )
            .await;
        }

        // 3. Plan: the SSTs that can hold each open key. Only filters that are
        //    in the cache take part, so the plan sends no request.
        let mut ssts = candidate_ssts(db_state.core(), &batch.keys, &resolved).await;
        for sst in ssts.iter_mut() {
            sst.filters = self.table_store.cached_filters(&sst.view.sst).await;
            // Keep cached lookups cooperative.
            tokio::task::coop::consume_budget().await;
        }
        let mut plan = Plan::new(ssts, n, &options.filter_context, Some(&self.db_stats));
        // A key with no candidates has nothing to read.
        for (u, candidates) in plan.candidates.iter().enumerate() {
            resolved[u] |= candidates.is_empty();
        }

        // 4. Read. Each open key reads its candidates newest first, in
        //    rounds, until it has a base value or no candidate is left. The
        //    keys do not wait for each other.
        let pipeline = Pipeline::new(
            self,
            &mut plan,
            &batch.keys,
            max_seq,
            options,
            &read_trace,
            &wb_acc,
            &mut acc,
            &mut resolved,
        );
        let rounds = pipeline.run().await?;
        self.db_stats.multi_get_rounds.increment(rounds);
        read_trace.read_span().record("rounds", rounds);

        // 5. Resolve each unique key, then scatter to input positions.
        let mut values: Vec<Option<RowEntry>> = Vec::with_capacity(n);
        for u in 0..n {
            let wb_entries = std::mem::take(&mut wb_acc[u]);
            let rest_entries = std::mem::take(&mut acc[u]);
            values.push(
                self.resolve_entry(&batch.keys[u], wb_entries, rest_entries)
                    .await?,
            );
            // Keep in-memory resolution cooperative.
            tokio::task::coop::consume_budget().await;
        }
        if keys.len() == n {
            // No duplicates: each value moves to its one slot.
            return Ok(batch.slots.iter().map(|&u| values[u].take()).collect());
        }
        Ok(batch.slots.iter().map(|&u| values[u].clone()).collect())
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
async fn read_memtable_layer(
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
        // Keep in-memory lookups cooperative.
        tokio::task::coop::consume_budget().await;
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
