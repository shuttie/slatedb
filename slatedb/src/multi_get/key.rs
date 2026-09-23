//! The keys of a `multi_get` batch and the read state of each one.

use bytes::Bytes;

use crate::types::{RowEntry, ValueDeletable};

/// The keys of a batch: sorted, with no duplicates.
pub(crate) struct BatchKeys {
    pub(crate) keys: Vec<Bytes>,
    /// For each input position, the index of its key in `keys`.
    pub(crate) slots: Vec<usize>,
}

impl BatchKeys {
    pub(crate) fn new<K: AsRef<[u8]>>(input: &[K]) -> Self {
        let mut order: Vec<usize> = (0..input.len()).collect();
        order.sort_unstable_by(|&a, &b| input[a].as_ref().cmp(input[b].as_ref()));
        let mut keys: Vec<Bytes> = Vec::new();
        let mut slots = vec![0; input.len()];
        for i in order {
            let key = input[i].as_ref();
            if keys.last().is_none_or(|last| last.as_ref() != key) {
                keys.push(Bytes::copy_from_slice(key));
            }
            slots[i] = keys.len() - 1;
        }
        Self { keys, slots }
    }
}

/// One SST that can hold a key.
#[derive(Debug)]
struct Candidate {
    /// Index into the SSTs of the batch.
    sst: usize,
    /// The filters of the SST passed the key.
    passed: bool,
}

/// The next step of a key. See [`KeyRead::pick`].
#[derive(Debug, PartialEq)]
pub(crate) enum Pick {
    /// Load the filters of these SSTs, then pick again.
    Load(Vec<usize>),
    /// Read these SSTs, newest first.
    Read(Vec<usize>),
    /// No candidate is left.
    Done,
}

/// One SST of the current pick of a key.
#[derive(Debug)]
struct Inflight {
    /// Index into the SSTs of the batch.
    sst: usize,
    /// The entries of the SST for the key, once its read arrived.
    entries: Option<Vec<RowEntry>>,
}

/// Everything that the batch knows about one unique key.
#[derive(Debug)]
pub(crate) struct KeyRead {
    pub(crate) key: Bytes,
    /// Write batch entries. They are not filtered by `max_seq`.
    pub(crate) wb: Vec<RowEntry>,
    /// Memtable and SST entries, newest first, filtered by `max_seq`.
    pub(crate) acc: Vec<RowEntry>,
    /// The key has a base value, or it has nothing left to read.
    pub(crate) done: bool,
    /// The SSTs that can hold the key and that it did not read yet, newest
    /// first.
    candidates: Vec<Candidate>,
    /// How many reads the key sent.
    pub(crate) rounds: u64,
    /// The number of filter loads that the key waits for.
    pub(crate) waiting: usize,
    /// The SSTs of the current read, newest first.
    inflight: Vec<Inflight>,
}

impl KeyRead {
    pub(crate) fn new(key: Bytes) -> Self {
        Self {
            key,
            wb: Vec::new(),
            acc: Vec::new(),
            done: false,
            candidates: Vec::new(),
            rounds: 0,
            waiting: 0,
            inflight: Vec::new(),
        }
    }

    /// Add an SST that can hold the key. The SSTs come newest first.
    pub(crate) fn add_candidate(&mut self, sst: usize) {
        self.candidates.push(Candidate { sst, passed: false });
    }

    #[cfg(test)]
    pub(crate) fn candidate_ssts(&self) -> Vec<usize> {
        self.candidates.iter().map(|c| c.sst).collect()
    }

    /// Add one entry of the write batch. A base value resolves the key.
    pub(crate) fn push_write(&mut self, entry: RowEntry) {
        self.done |= !matches!(entry.value, ValueDeletable::Merge(_));
        self.wb.push(entry);
    }

    /// Append the versions of one layer, newest first. An entry above
    /// `max_seq` is not visible, so a too-new base value must not resolve
    /// the key. The first visible base value resolves the key and stops the
    /// append, because it shadows everything older. Merge operands keep
    /// accumulating, because they can span layers.
    pub(crate) fn append(&mut self, entries: Vec<RowEntry>, max_seq: Option<u64>) {
        for entry in entries {
            if max_seq.is_some_and(|ms| entry.seq > ms) {
                continue;
            }
            let is_base = !matches!(entry.value, ValueDeletable::Merge(_));
            self.acc.push(entry);
            if is_base {
                self.done = true;
                return;
            }
        }
    }

    /// An open key holds only merge operands.
    pub(crate) fn has_operand(&self) -> bool {
        !self.wb.is_empty() || !self.acc.is_empty()
    }

    /// Walk the candidates newest first and pick the next step, as `get`
    /// does. The first read takes one SST whose filters pass the key, and
    /// each later read takes `lookahead` of them. A key with a merge operand
    /// needs its base value, so it has no limit.
    ///
    /// `filter` says if the filters of an SST pass the key, or `None` when
    /// they are not loaded. An SST with no loaded filters does not count
    /// against the limit, because the pick only loads its filters. An SST
    /// whose filters reject the key leaves the candidates.
    pub(crate) fn pick(
        &mut self,
        lookahead: usize,
        mut filter: impl FnMut(usize, &Bytes) -> Option<bool>,
    ) -> Pick {
        let mut left = match (self.has_operand(), self.rounds) {
            (true, _) => usize::MAX,
            (false, 0) => 1,
            (false, _) => lookahead.max(1),
        };
        let mut load = Vec::new();
        let mut end = 0;
        while end < self.candidates.len() && left > 0 {
            let candidate = &mut self.candidates[end];
            if !candidate.passed {
                match filter(candidate.sst, &self.key) {
                    None => {
                        load.push(candidate.sst);
                        end += 1;
                        continue;
                    }
                    Some(false) => {
                        self.candidates.remove(end);
                        continue;
                    }
                    Some(true) => candidate.passed = true,
                }
            }
            left -= 1;
            end += 1;
        }
        if !load.is_empty() {
            return Pick::Load(load);
        }
        if end == 0 {
            return Pick::Done;
        }
        Pick::Read(self.candidates.drain(..end).map(|c| c.sst).collect())
    }

    /// Send the key to the SSTs of one read, newest first.
    pub(crate) fn send(&mut self, ssts: &[usize]) {
        self.rounds += 1;
        self.inflight = ssts
            .iter()
            .map(|&sst| Inflight { sst, entries: None })
            .collect();
    }

    /// Record the entries that `sst` holds for the key, and apply each read
    /// of the pick that arrived. The entries apply newest SST first only, so
    /// a read waits until every newer SST of the pick arrived. A base value
    /// resolves the key and drops the rest of the pick.
    pub(crate) fn arrive(&mut self, sst: usize, entries: Vec<RowEntry>, max_seq: Option<u64>) {
        if let Some(slot) = self.inflight.iter_mut().find(|slot| slot.sst == sst) {
            slot.entries = Some(entries);
        }
        let ready = self
            .inflight
            .iter()
            .take_while(|slot| slot.entries.is_some())
            .count();
        let arrived: Vec<Inflight> = self.inflight.drain(..ready).collect();
        for slot in arrived {
            if !self.done {
                self.append(slot.entries.unwrap_or_default(), max_seq);
            }
        }
        if self.done {
            self.inflight.clear();
        }
    }

    /// The key has no read in flight and waits for no filter.
    pub(crate) fn is_idle(&self) -> bool {
        self.inflight.is_empty() && self.waiting == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case::empty(&[], &[], &[])]
    #[case::sorted(&["a", "b"], &["a", "b"], &[0, 1])]
    #[case::reverse(&["c", "b", "a"], &["a", "b", "c"], &[2, 1, 0])]
    #[case::duplicates(&["b", "a", "b", "a"], &["a", "b"], &[1, 0, 1, 0])]
    fn should_sort_and_dedup_keys(
        #[case] input: &[&str],
        #[case] expected_keys: &[&str],
        #[case] expected_slots: &[usize],
    ) {
        let batch = BatchKeys::new(input);

        let expected_keys: Vec<Bytes> = expected_keys
            .iter()
            .map(|k| Bytes::copy_from_slice(k.as_bytes()))
            .collect();
        assert_eq!(batch.keys, expected_keys);
        assert_eq!(batch.slots, expected_slots);
    }

    /// The filter answer of each SST: passes, rejects, or not loaded.
    const P: Option<bool> = Some(true);
    const R: Option<bool> = Some(false);
    const N: Option<bool> = None;

    /// A key whose candidates are the SSTs `0..count`, after `rounds` reads.
    fn key_with_candidates(count: usize, rounds: u64, operand: bool) -> KeyRead {
        let mut key = key_read();
        for sst in 0..count {
            key.add_candidate(sst);
        }
        key.rounds = rounds;
        if operand {
            key.acc = merge(9);
        }
        key
    }

    #[rstest]
    #[case::no_candidates(&[], 0, 4, false, Pick::Done)]
    #[case::first_read_takes_one(&[P, P, P], 0, 4, false, Pick::Read(vec![0]))]
    #[case::later_read_takes_lookahead(&[P, P, P, P, P], 1, 4, false, Pick::Read(vec![0, 1, 2, 3]))]
    #[case::lookahead_0_acts_as_1(&[P, P], 1, 0, false, Pick::Read(vec![0]))]
    #[case::operand_removes_the_limit(&[P, P, P], 0, 1, true, Pick::Read(vec![0, 1, 2]))]
    #[case::not_loaded_is_free(&[N, N, P, P], 0, 4, false, Pick::Load(vec![0, 1]))]
    #[case::all_not_loaded_loads_all(&[N, N, N], 0, 4, false, Pick::Load(vec![0, 1, 2]))]
    #[case::rejects_do_not_count(&[P, R, P, P], 1, 2, false, Pick::Read(vec![0, 2]))]
    #[case::all_rejected(&[R, R], 0, 4, false, Pick::Done)]
    fn should_pick_like_get(
        #[case] filters: &[Option<bool>],
        #[case] rounds: u64,
        #[case] lookahead: usize,
        #[case] operand: bool,
        #[case] expected: Pick,
    ) {
        let mut key = key_with_candidates(filters.len(), rounds, operand);

        assert_eq!(key.pick(lookahead, |sst, _| filters[sst]), expected);
    }

    #[test]
    fn should_check_each_filter_one_time() {
        let mut key = key_with_candidates(3, 0, false);
        let mut checks = [0; 3];
        let mut filters = [N, P, R];

        let pick = key.pick(4, |sst, _| {
            checks[sst] += 1;
            filters[sst]
        });
        assert_eq!(pick, Pick::Load(vec![0]));
        // The filters of SST 0 arrive. SST 1 passed before, so the key does
        // not check it again.
        filters[0] = P;
        let pick = key.pick(4, |sst, _| {
            checks[sst] += 1;
            filters[sst]
        });
        assert_eq!(pick, Pick::Read(vec![0]));
        key.send(&[0]);
        let pick = key.pick(4, |sst, _| {
            checks[sst] += 1;
            filters[sst]
        });

        assert_eq!(pick, Pick::Read(vec![1]));
        assert_eq!(checks, [2, 1, 1]);
    }

    fn value(seq: u64) -> Vec<RowEntry> {
        vec![RowEntry::new_value(b"k", b"v", seq)]
    }

    fn merge(seq: u64) -> Vec<RowEntry> {
        vec![RowEntry::new_merge(b"k", b"+", seq)]
    }

    fn seqs(acc: &[RowEntry]) -> Vec<u64> {
        acc.iter().map(|entry| entry.seq).collect()
    }

    fn key_read() -> KeyRead {
        KeyRead::new(Bytes::from_static(b"k"))
    }

    /// One arrival of a pick over the SSTs 0, 1, 2, newest first: the SST,
    /// its entries, and the state after it: the seqs in `acc`, `done`, and
    /// `is_idle`.
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
        let mut key = key_read();
        key.send(&[0, 1, 2]);
        assert!(!key.is_idle());

        for (sst, entries, expected_acc, expected_done, expected_idle) in arrivals {
            key.arrive(sst, entries, Some(5));
            assert_eq!(seqs(&key.acc), expected_acc, "after sst {sst}");
            assert_eq!(key.done, expected_done, "after sst {sst}");
            assert_eq!(key.is_idle(), expected_idle, "after sst {sst}");
        }
    }
}
