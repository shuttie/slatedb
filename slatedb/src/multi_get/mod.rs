//! Batched point reads (`multi_get`, RFC 0035).
//!
//! - [`key`]: the keys of the batch and the read state of each one.
//! - [`candidates`]: which SSTs can hold which keys. Memory only.
//! - [`sst`]: how one SST is read for its keys.
//! - [`pipeline`]: the read phase. Each key reads its SSTs newest first,
//!   and no key waits for the reads of another key.
//! - This module: the orchestrator. It reads the write batch and the
//!   memtables, lists the candidate SSTs, runs the pipeline, and resolves
//!   each key with the components of the single-key `get` path.

mod candidates;
mod key;
mod pipeline;
mod sst;

use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::Semaphore;
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
use crate::types::RowEntry;

use candidates::{candidate_ssts, Filters};
use key::{BatchKeys, KeyRead};
use pipeline::Pipeline;
use sst::SstReader;

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
        write_batch: Option<&RwLock<WriteBatch>>,
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
        write_batch: Option<&RwLock<WriteBatch>>,
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
        let mut key_reads: Vec<KeyRead> = batch.keys.iter().cloned().map(KeyRead::new).collect();

        // 1. Write batch (transaction only). Entries carry seq u64::MAX and are
        //    not max_seq filtered (hence `None`), matching the single-key get
        //    path. Each key is a point lookup under the read guard, as in
        //    `get`, so the cost grows with the keys and not with the batch.
        //    The guard is released before the next await.
        if let Some(wb) = write_batch {
            for key in key_reads.iter_mut() {
                let mut iter = {
                    let guard = wb.read();
                    WriteBatchIterator::new(
                        &guard,
                        BytesRange::from_slice(key.key.as_ref()..=key.key.as_ref()),
                        IterationOrder::Ascending,
                        u64::MAX,
                        None,
                        None,
                    )
                };
                iter.init().await?;
                while let Some(entry) = iter.next().await? {
                    key.push_write(entry);
                }
                // Keep the in-memory lookups cooperative.
                tokio::task::coop::consume_budget().await;
            }
        }

        // 2. Active memtable, then immutable memtables (newest-first).
        let memtable = db_state.memtable();
        read_memtable_layer(&memtable, &mut key_reads, max_seq, &read_trace).await;
        for imm in db_state.imm_memtable() {
            read_memtable_layer(&imm.table(), &mut key_reads, max_seq, &read_trace).await;
        }

        // 3. The SSTs that can hold each open key. The walk reads only the
        //    manifest.
        let mut ssts = candidate_ssts(db_state.core(), &mut key_reads).await;
        // Filters in the cache are loaded now, so a key checks a warm SST
        // with no load and no extra turn of the read loop.
        for sst in ssts.iter_mut() {
            if let Some(filters) = self.table_store.cached_filters(&sst.target.handle).await {
                sst.filters = Filters::Loaded(filters);
            }
            // Keep cached lookups cooperative.
            tokio::task::coop::consume_budget().await;
        }

        // 4. Read. Each open key reads its candidates newest first, in
        //    rounds, until it has a base value or no candidate is left. The
        //    keys do not wait for each other.
        let requests = Arc::new(Semaphore::new(options.max_fetch_tasks.max(1)));
        let reader = SstReader::new(self, &requests, options, &read_trace);
        let pipeline = Pipeline::new(reader, &mut key_reads, ssts, max_seq);
        let rounds = pipeline.run().await?;
        self.db_stats.multi_get_rounds.increment(rounds);
        read_trace.read_span().record("rounds", rounds);

        // 5. Resolve each unique key, then scatter to input positions.
        let mut values: Vec<Option<RowEntry>> = Vec::with_capacity(key_reads.len());
        for key in key_reads {
            values.push(self.resolve_entry(&key.key, key.wb, key.acc).await?);
            // Keep in-memory resolution cooperative.
            tokio::task::coop::consume_budget().await;
        }
        if keys.len() == values.len() {
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
    /// on append (see [`KeyRead::append`]) and write-batch entries are exempt
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

/// Probe an in-memory table for every still-pending key, appending its versions.
async fn read_memtable_layer(
    table: &KVTable,
    keys: &mut [KeyRead],
    max_seq: Option<u64>,
    read_trace: &ReadTrace,
) {
    for key in keys.iter_mut() {
        if key.done {
            continue;
        }
        let entries = kv_table_get_versions(table, &key.key, read_trace);
        key.append(entries, max_seq);
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
