//! The read state of one unique key of a `multi_get` batch.

use bytes::Bytes;

use crate::types::{RowEntry, ValueDeletable};

use super::plan::{pick_first, pick_next, Candidate};

/// One SST of the current pick of a key.
#[derive(Debug)]
struct Inflight {
    /// Index into [`super::plan::Plan::ssts`].
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
    /// How many picks the key made.
    pub(crate) rounds: u64,
    /// The pick has an UNKNOWN SST. The key waits for its filter load.
    pub(crate) waiting: bool,
    /// The SSTs of the current pick, newest first.
    inflight: Vec<Inflight>,
}

impl KeyRead {
    pub(crate) fn new(key: Bytes) -> Self {
        Self {
            key,
            wb: Vec::new(),
            acc: Vec::new(),
            done: false,
            rounds: 0,
            waiting: false,
            inflight: Vec::new(),
        }
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

    /// How many candidates the next pick reads, newest first.
    pub(crate) fn pick(&self, candidates: &[Candidate], lookahead: usize) -> usize {
        match self.rounds {
            0 => pick_first(candidates, self.has_operand()),
            _ => pick_next(candidates, self.has_operand(), lookahead),
        }
    }

    /// Send the key to the SSTs of one pick, newest first.
    pub(crate) fn send(&mut self, ssts: &[usize]) {
        self.rounds += 1;
        self.waiting = false;
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
        self.inflight.is_empty() && !self.waiting
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    use crate::multi_get::plan::FilterState::{Positive, Unknown};

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

    #[test]
    fn should_pick_first_then_next() {
        let candidates: Vec<Candidate> = [Positive; 4]
            .iter()
            .enumerate()
            .map(|(sst, &state)| Candidate { sst, state })
            .collect();
        let mut key = key_read();

        assert_eq!(key.pick(&candidates, 3), 1);
        key.send(&[0]);
        assert_eq!(key.rounds, 1);
        assert_eq!(key.pick(&candidates[1..], 3), 3);
        key.acc = merge(4);
        assert_eq!(key.pick(&candidates[1..], 1), 3);
    }

    #[test]
    fn should_wait_for_filters_until_sent() {
        let mut key = key_read();
        key.waiting = true;
        assert!(!key.is_idle());
        let candidates = [Candidate {
            sst: 0,
            state: Unknown,
        }];
        // An UNKNOWN SST is free to pick.
        assert_eq!(key.pick(&candidates, 1), 1);

        key.send(&[0]);

        assert!(!key.waiting);
        assert!(!key.is_idle());
    }
}
