//! The read phase of `multi_get`: pipelined reads of the candidate SSTs.
//!
//! Each open key reads its candidates newest first, in rounds. One round is
//! one pick ([`KeyRead::pick`]) and
//! the reads of the picked SSTs. The keys do not wait for each other: when
//! the read of one SST returns, the keys of that read make their next pick
//! at once. A batch that reads in lockstep pays the tail latency of the
//! object store one time per step. A pipeline pays it about one time.
//!
//! The [`Pipeline`] drives an event loop over one `JoinSet`. A cache hit is
//! an event that the caller handles inline, with no task.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::MultiGetOptions;
use crate::error::SlateDBError;
use crate::filter_policy::NamedFilter;
use crate::reader::{ReadTrace, Reader};
use crate::sst_iter::task_join_error;
use crate::types::RowEntry;

use super::key::KeyRead;
use super::plan::{FilterState, Plan};
use super::sst::{
    load_filters as load_sst_filters, read_from_cache, read_sst, PendingKey, SstEntries, SstRead,
};
use super::sst_options;

/// What one task of the read phase returns.
enum Event {
    /// The filters of one UNKNOWN SST.
    Filters {
        /// Index into [`Plan::ssts`].
        sst: usize,
        filters: Arc<[NamedFilter]>,
    },
    /// The read of one SST for some of its keys.
    Read {
        /// Index into [`Plan::ssts`].
        sst: usize,
        /// The keys that the read got. A key that the SST does not hold has
        /// no entries, but it still arrives.
        keys: Vec<usize>,
        entries: SstEntries,
    },
}

/// The read phase of one batch. It owns the state of the read and borrows
/// the state of the batch.
pub(crate) struct Pipeline<'a> {
    reader: &'a Reader,
    plan: &'a mut Plan,
    keys: &'a mut [KeyRead],
    max_seq: Option<u64>,
    options: &'a MultiGetOptions,
    read_trace: &'a ReadTrace,
    /// The request semaphore of the batch.
    requests: Arc<Semaphore>,
    /// The keys that wait for a read of each SST.
    ready: BTreeMap<usize, Vec<PendingKey>>,
    /// The SSTs with a filter load in flight.
    loading: BTreeSet<usize>,
    /// A drop of the `JoinSet` aborts its tasks.
    tasks: JoinSet<Result<Event, SlateDBError>>,
}

impl<'a> Pipeline<'a> {
    pub(crate) fn new(
        reader: &'a Reader,
        plan: &'a mut Plan,
        keys: &'a mut [KeyRead],
        max_seq: Option<u64>,
        options: &'a MultiGetOptions,
        read_trace: &'a ReadTrace,
    ) -> Self {
        Self {
            reader,
            plan,
            keys,
            max_seq,
            options,
            read_trace,
            requests: Arc::new(Semaphore::new(options.max_fetch_tasks.max(1))),
            ready: BTreeMap::new(),
            loading: BTreeSet::new(),
            tasks: JoinSet::new(),
        }
    }

    /// Read until each key has a base value or no candidate is left. Returns
    /// the number of rounds of the slowest key.
    pub(crate) async fn run(mut self) -> Result<u64, SlateDBError> {
        for u in 0..self.keys.len() {
            if !self.keys[u].done {
                self.schedule(u);
            }
            // Keep the first pick cooperative.
            tokio::task::coop::consume_budget().await;
        }
        self.flush().await?;
        while let Some(joined) = self.tasks.join_next().await {
            let event = joined.map_err(|e| task_join_error(e, "multi_get_read".to_string()))??;
            match event {
                Event::Filters { sst, filters } => self.on_filters(sst, filters).await,
                Event::Read { sst, keys, entries } => self.arrive(sst, keys, entries).await,
            }
            self.flush().await?;
        }
        Ok(self.keys.iter().map(|key| key.rounds).max().unwrap_or(0))
    }

    /// Make the next pick of an idle open key. The pick goes to `ready`, or
    /// the key waits for the filter loads of its UNKNOWN SSTs.
    fn schedule(&mut self, u: usize) {
        let candidates = &self.plan.candidates[u];
        if candidates.is_empty() {
            // No SST is left to read. The key is absent, or it has only
            // merge operands.
            self.keys[u].done = true;
            return;
        }
        let picked = self.keys[u].pick(candidates, self.options.lookahead);
        // An UNKNOWN SST is free to pick, but only for its filter load.
        // After the load, the key is POSITIVE or gone, and the next pick
        // applies the limit.
        let unknown: Vec<usize> = candidates[..picked]
            .iter()
            .filter(|c| c.state == FilterState::Unknown)
            .map(|c| c.sst)
            .collect();
        if !unknown.is_empty() {
            self.keys[u].waiting = true;
            for sst in unknown {
                self.load_filters(sst);
            }
            return;
        }
        let ssts: Vec<usize> = self.plan.candidates[u]
            .drain(..picked)
            .map(|c| c.sst)
            .collect();
        self.keys[u].send(&ssts);
        for sst in ssts {
            self.ready.entry(sst).or_default().push(PendingKey {
                idx: u,
                key: self.keys[u].key.clone(),
            });
        }
    }

    /// Read each SST of `ready`. A cache hit can make a new pick, so the
    /// loop runs until `ready` is empty.
    async fn flush(&mut self) -> Result<(), SlateDBError> {
        while let Some((sst, mut keys)) = self.ready.pop_first() {
            keys.sort_by_key(|pk| pk.idx);
            self.read_group(sst, keys).await?;
        }
        Ok(())
    }

    /// Read one SST for a group of its keys. The cache answers first, inline.
    /// Only the keys that miss go to a task.
    async fn read_group(&mut self, sst: usize, keys: Vec<PendingKey>) -> Result<(), SlateDBError> {
        let plan_sst = &self.plan.ssts[sst];
        let filters_present = plan_sst.filters.as_ref().is_some_and(|f| !f.is_empty());
        let mut entries = read_from_cache(
            &plan_sst.view.sst,
            &keys,
            filters_present,
            &self.reader.table_store,
            Some(&self.reader.db_stats),
        )
        .await?;
        let misses = std::mem::take(&mut entries.misses);
        let missed: BTreeSet<usize> = misses.iter().map(|pk| pk.idx).collect();
        if !misses.is_empty() {
            // With no cache, the index comes from an earlier read.
            let index = entries.index.clone().or_else(|| plan_sst.index.clone());
            let read = SstRead {
                view: plan_sst.view.clone(),
                keys: misses,
                filters: plan_sst.filters.clone(),
                index,
                sst_level: Some(plan_sst.level.clone()),
                options: sst_options(self.options, &plan_sst.segment),
            };
            let sst_read = read_sst(
                read,
                self.reader.table_store.clone(),
                self.requests.clone(),
                self.options.clone(),
                self.read_trace.clone(),
                Some(self.reader.db_stats.clone()),
            );
            let keys: Vec<usize> = missed.iter().copied().collect();
            self.tasks.spawn(async move {
                let entries = sst_read.await?;
                Ok(Event::Read { sst, keys, entries })
            });
        }
        let hits: Vec<usize> = keys
            .iter()
            .map(|pk| pk.idx)
            .filter(|idx| !missed.contains(idx))
            .collect();
        self.arrive(sst, hits, entries).await;
        Ok(())
    }

    /// Apply the read of one SST to its keys. A key whose pick is complete
    /// makes its next pick.
    async fn arrive(&mut self, sst: usize, keys: Vec<usize>, entries: SstEntries) {
        if entries.index.is_some() {
            self.plan.ssts[sst].index = entries.index;
        }
        let mut found: BTreeMap<usize, Vec<RowEntry>> = entries
            .found
            .into_iter()
            .map(|key| (key.idx, key.entries))
            .collect();
        for u in keys {
            let entries = found.remove(&u).unwrap_or_default();
            let key = &mut self.keys[u];
            key.arrive(sst, entries, self.max_seq);
            if !key.done && key.is_idle() {
                self.schedule(u);
            }
            // Keep the result pass cooperative.
            tokio::task::coop::consume_budget().await;
        }
    }

    /// Apply the loaded filters of one SST, then make the pick of each key
    /// that waits for a filter.
    async fn on_filters(&mut self, sst: usize, filters: Arc<[NamedFilter]>) {
        self.plan.apply_filters(
            sst,
            filters,
            |u| self.keys[u].done,
            &self.options.filter_context,
            Some(&self.reader.db_stats),
        );
        self.loading.remove(&sst);
        for u in 0..self.keys.len() {
            if self.keys[u].waiting && !self.keys[u].done {
                self.schedule(u);
            }
            // Keep the pick pass cooperative.
            tokio::task::coop::consume_budget().await;
        }
    }

    /// Spawn the filter load of one SST, unless one is in flight.
    fn load_filters(&mut self, sst: usize) {
        if !self.loading.insert(sst) {
            return;
        }
        let plan_sst = &self.plan.ssts[sst];
        let (view, level) = (plan_sst.view.clone(), plan_sst.level.clone());
        let sst_options = sst_options(self.options, &plan_sst.segment);
        let table_store = self.reader.table_store.clone();
        let (requests, read_trace) = (self.requests.clone(), self.read_trace.clone());
        self.tasks.spawn(async move {
            let filters = load_sst_filters(
                &view,
                &sst_options,
                Some(&level),
                &table_store,
                &requests,
                &read_trace,
            );
            let filters = filters.await?;
            Ok(Event::Filters { sst, filters })
        });
    }
}
