//! The read phase of `multi_get`: pipelined reads of the candidate SSTs.
//!
//! Each open key reads its candidates newest first, in rounds. One round is
//! one pick ([`super::plan::pick_first`] or [`super::plan::pick_next`]) and
//! the reads of the picked SSTs. The keys do not wait for each other: when
//! the read of one SST returns, the keys of that read make their next pick
//! at once. A batch that reads in lockstep pays the tail latency of the
//! object store one time per step. A pipeline pays it about one time.
//!
//! The [`Pipeline`] drives an event loop over one `JoinSet`. A cache hit is
//! an event that the caller handles inline, with no task.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::MultiGetOptions;
use crate::error::SlateDBError;
use crate::filter_policy::NamedFilter;
use crate::reader::{ReadTrace, Reader};
use crate::sst_iter::task_join_error;
use crate::types::RowEntry;

use super::plan::{pick_first, pick_next, Candidate, FilterState, Plan};
use super::sst::{
    load_filters as load_sst_filters, read_from_cache, read_sst, PendingKey, SstEntries, SstRead,
};
use super::{append_versions, sst_options};

/// The read state of one open key.
#[derive(Debug, Default)]
pub(crate) struct KeyState {
    /// How many picks the key made.
    rounds: u64,
    /// The pick has an UNKNOWN SST. The key waits for its filter load.
    waiting: bool,
    /// The SSTs of the current pick, newest first, each with its entries
    /// once its read arrived.
    inflight: Vec<(usize, Option<Vec<RowEntry>>)>,
}

impl KeyState {
    /// How many candidates the next pick reads, newest first.
    fn pick(&self, candidates: &[Candidate], has_operand: bool, lookahead: usize) -> usize {
        match self.rounds {
            0 => pick_first(candidates, has_operand),
            _ => pick_next(candidates, has_operand, lookahead),
        }
    }

    /// Send the key to the SSTs of one pick, newest first.
    fn send(&mut self, ssts: &[usize]) {
        self.rounds += 1;
        self.waiting = false;
        self.inflight = ssts.iter().map(|&sst| (sst, None)).collect();
    }

    /// Record the entries that `sst` holds for the key, and apply each pick
    /// whose read arrived. The entries apply newest SST first only, so a
    /// read waits until every newer SST of the pick arrived. A base value
    /// resolves the key and drops the rest of the pick.
    fn arrive(
        &mut self,
        sst: usize,
        entries: Vec<RowEntry>,
        max_seq: Option<u64>,
        acc: &mut Vec<RowEntry>,
        resolved: &mut bool,
    ) {
        if let Some(slot) = self.inflight.iter_mut().find(|(s, _)| *s == sst) {
            slot.1 = Some(entries);
        }
        let ready = self
            .inflight
            .iter()
            .take_while(|(_, entries)| entries.is_some())
            .count();
        for (_, entries) in self.inflight.drain(..ready) {
            if !*resolved {
                append_versions(entries.unwrap_or_default(), max_seq, acc, resolved);
            }
        }
        if *resolved {
            self.inflight.clear();
        }
    }

    /// The key has no read in flight and waits for no filter.
    fn is_idle(&self) -> bool {
        self.inflight.is_empty() && !self.waiting
    }
}

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
    keys: &'a [Bytes],
    max_seq: Option<u64>,
    options: &'a MultiGetOptions,
    read_trace: &'a ReadTrace,
    wb_acc: &'a [Vec<RowEntry>],
    acc: &'a mut [Vec<RowEntry>],
    resolved: &'a mut [bool],
    /// The request semaphore of the batch.
    requests: Arc<Semaphore>,
    states: Vec<KeyState>,
    /// The keys that wait for a read of each SST.
    ready: BTreeMap<usize, Vec<PendingKey>>,
    /// The SSTs with a filter load in flight.
    loading: BTreeSet<usize>,
    /// A drop of the `JoinSet` aborts its tasks.
    tasks: JoinSet<Result<Event, SlateDBError>>,
}

impl<'a> Pipeline<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        reader: &'a Reader,
        plan: &'a mut Plan,
        keys: &'a [Bytes],
        max_seq: Option<u64>,
        options: &'a MultiGetOptions,
        read_trace: &'a ReadTrace,
        wb_acc: &'a [Vec<RowEntry>],
        acc: &'a mut [Vec<RowEntry>],
        resolved: &'a mut [bool],
    ) -> Self {
        let states = (0..keys.len()).map(|_| KeyState::default()).collect();
        Self {
            reader,
            plan,
            keys,
            max_seq,
            options,
            read_trace,
            wb_acc,
            acc,
            resolved,
            requests: Arc::new(Semaphore::new(options.max_fetch_tasks.max(1))),
            states,
            ready: BTreeMap::new(),
            loading: BTreeSet::new(),
            tasks: JoinSet::new(),
        }
    }

    /// Read until each key has a base value or no candidate is left. Returns
    /// the number of rounds of the slowest key.
    pub(crate) async fn run(mut self) -> Result<u64, SlateDBError> {
        for u in 0..self.keys.len() {
            if !self.resolved[u] {
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
        Ok(self.states.iter().map(|s| s.rounds).max().unwrap_or(0))
    }

    /// Make the next pick of an idle open key. The pick goes to `ready`, or
    /// the key waits for the filter loads of its UNKNOWN SSTs.
    fn schedule(&mut self, u: usize) {
        let candidates = &self.plan.candidates[u];
        if candidates.is_empty() {
            // No SST is left to read. The key is absent, or it has only
            // merge operands.
            self.resolved[u] = true;
            return;
        }
        // An open key holds only merge operands.
        let has_operand = !self.wb_acc[u].is_empty() || !self.acc[u].is_empty();
        let picked = self.states[u].pick(candidates, has_operand, self.options.lookahead);
        // An UNKNOWN SST is free to pick, but only for its filter load.
        // After the load, the key is POSITIVE or gone, and the next pick
        // applies the limit.
        let unknown: Vec<usize> = candidates[..picked]
            .iter()
            .filter(|c| c.state == FilterState::Unknown)
            .map(|c| c.sst)
            .collect();
        if !unknown.is_empty() {
            self.states[u].waiting = true;
            for sst in unknown {
                self.load_filters(sst);
            }
            return;
        }
        let ssts: Vec<usize> = self.plan.candidates[u]
            .drain(..picked)
            .map(|c| c.sst)
            .collect();
        self.states[u].send(&ssts);
        for sst in ssts {
            self.ready.entry(sst).or_default().push(PendingKey {
                idx: u,
                key: self.keys[u].clone(),
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
            // `append_versions` re-filters by max_seq and stops each key at
            // its first surviving Value/Tombstone, so the entries of older
            // SSTs are dropped exactly as in the single-key walk. Memtable
            // entries are already in `acc` and stay first (they are newer
            // than anything on disk).
            self.states[u].arrive(
                sst,
                entries,
                self.max_seq,
                &mut self.acc[u],
                &mut self.resolved[u],
            );
            if !self.resolved[u] && self.states[u].is_idle() {
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
            &*self.resolved,
            &self.options.filter_context,
            Some(&self.reader.db_stats),
        );
        self.loading.remove(&sst);
        for u in 0..self.states.len() {
            if self.states[u].waiting && !self.resolved[u] {
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

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    use FilterState::{Positive, Unknown};

    fn value(seq: u64) -> Vec<RowEntry> {
        vec![RowEntry::new_value(b"k", b"v", seq)]
    }

    fn merge(seq: u64) -> Vec<RowEntry> {
        vec![RowEntry::new_merge(b"k", b"+", seq)]
    }

    fn seqs(acc: &[RowEntry]) -> Vec<u64> {
        acc.iter().map(|entry| entry.seq).collect()
    }

    /// One arrival of a pick over the SSTs 0, 1, 2, newest first: the SST,
    /// its entries, and the state after it: the seqs in `acc`, `resolved`,
    /// and `is_idle`.
    type Arrival = (usize, Vec<RowEntry>, Vec<u64>, bool, bool);

    #[rstest]
    #[case::in_order(vec![
        (0, vec![], vec![], false, false),
        (1, vec![], vec![], false, false),
        (2, value(1), vec![1], true, true),
    ])]
    // The read of SST 1 waits for SST 0. Both apply when SST 0 arrives.
    #[case::out_of_order_waits(vec![
        (1, value(2), vec![], false, false),
        (0, vec![], vec![2], true, true),
    ])]
    // The oldest SST arrives first. The middle one has nothing, and
    // releases it.
    #[case::not_found_releases_the_next(vec![
        (2, value(1), vec![], false, false),
        (0, vec![], vec![], false, false),
        (1, vec![], vec![1], true, true),
    ])]
    // A base value drops the rest of the pick.
    #[case::base_value_drops_the_rest(vec![
        (0, value(3), vec![3], true, true),
        (1, value(2), vec![3], true, true),
    ])]
    // Operands keep the pick open. The base value closes it.
    #[case::operands_accumulate(vec![
        (0, merge(3), vec![3], false, false),
        (1, merge(2), vec![3, 2], false, false),
        (2, value(1), vec![3, 2, 1], true, true),
    ])]
    // An entry above `max_seq` does not count.
    #[case::hidden_version_is_dropped(vec![
        (0, value(9), vec![], false, false),
        (1, vec![], vec![], false, false),
        (2, value(1), vec![1], true, true),
    ])]
    fn should_apply_arrivals_newest_first(#[case] arrivals: Vec<Arrival>) {
        let mut state = KeyState::default();
        state.send(&[0, 1, 2]);
        let mut acc = Vec::new();
        let mut resolved = false;
        assert!(!state.is_idle());

        for (sst, entries, expected_acc, expected_resolved, expected_idle) in arrivals {
            state.arrive(sst, entries, Some(5), &mut acc, &mut resolved);
            assert_eq!(seqs(&acc), expected_acc, "after sst {sst}");
            assert_eq!(resolved, expected_resolved, "after sst {sst}");
            assert_eq!(state.is_idle(), expected_idle, "after sst {sst}");
        }
    }

    #[test]
    fn should_pick_first_then_next() {
        let candidates: Vec<Candidate> = [Positive; 4]
            .iter()
            .enumerate()
            .map(|(sst, &state)| Candidate { sst, state })
            .collect();
        let mut state = KeyState::default();

        assert_eq!(state.pick(&candidates, false, 3), 1);
        state.send(&[0]);
        assert_eq!(state.rounds, 1);
        assert_eq!(state.pick(&candidates[1..], false, 3), 3);
        assert_eq!(state.pick(&candidates[1..], true, 3), 3);
    }

    #[test]
    fn should_wait_for_filters_until_sent() {
        let mut state = KeyState {
            waiting: true,
            ..KeyState::default()
        };
        assert!(!state.is_idle());
        let candidates = [Candidate {
            sst: 0,
            state: Unknown,
        }];
        // An UNKNOWN SST is free to pick.
        assert_eq!(state.pick(&candidates, false, 1), 1);

        state.send(&[0]);

        assert!(!state.waiting);
        assert!(!state.is_idle());
    }
}
