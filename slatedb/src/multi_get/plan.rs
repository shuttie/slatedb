//! The plan phase of `multi_get`: which SSTs can hold which keys.
//!
//! The functions here are pure. They read the manifest and the filters that
//! the orchestrator found in the cache, and they send no request.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;

use crate::bytes_range::BytesRange;
use crate::db_state::{SortedRun, SsTableView};
use crate::db_stats::DbStats;
use crate::filter_policy::{FilterContext, NamedFilter};
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::manifest::{ManifestCore, Segment};
use crate::reader::SstTraceLevel;

use super::key::KeyRead;
use super::sst::{filter_keys, view_holds_key, PendingKey};

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

    pub(crate) fn position(&self, key: &[u8]) -> Option<usize> {
        self.keys.binary_search_by(|k| k.as_ref().cmp(key)).ok()
    }
}

/// What the cached filters say about one key in one SST. A key that the
/// filters reject is no candidate at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FilterState {
    /// The filters are not in the cache.
    Unknown,
    /// The filters passed the key, or the SST has no filter.
    Positive,
}

/// One SST of the plan with the keys that it can hold.
pub(crate) struct PlanSst {
    pub(crate) view: SsTableView,
    pub(crate) segment: Bytes,
    pub(crate) level: SstTraceLevel,
    /// The filters from the cache, or from the first load of the batch.
    /// `None` means that the batch does not have them yet.
    pub(crate) filters: Option<Arc<[NamedFilter]>>,
    /// The index, after the first read of the batch that used it.
    pub(crate) index: Option<Arc<SsTableIndexOwned>>,
    /// Sorted. `idx` points into [`BatchKeys::keys`].
    pub(crate) keys: Vec<PendingKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Candidate {
    /// Index into [`Plan::ssts`].
    pub(crate) sst: usize,
    pub(crate) state: FilterState,
}

pub(crate) struct Plan {
    /// Newest first inside each LSM tree.
    pub(crate) ssts: Vec<PlanSst>,
    /// For each key, its SSTs, newest first.
    pub(crate) candidates: Vec<Vec<Candidate>>,
}

impl Plan {
    /// Apply the cached filters of each SST to its keys. An SST that keeps
    /// no key leaves the plan.
    pub(crate) fn new(
        ssts: Vec<PlanSst>,
        num_keys: usize,
        filter_context: &Option<FilterContext>,
        db_stats: Option<&DbStats>,
    ) -> Self {
        let mut plan = Plan {
            ssts: Vec::with_capacity(ssts.len()),
            candidates: vec![Vec::new(); num_keys],
        };
        for mut sst in ssts {
            let state = match &sst.filters {
                None => FilterState::Unknown,
                Some(filters) => {
                    let kept = filter_keys(&sst.keys, filters, filter_context, db_stats);
                    if kept.len() < sst.keys.len() {
                        sst.keys = kept.into_iter().cloned().collect();
                    }
                    FilterState::Positive
                }
            };
            for pk in &sst.keys {
                plan.candidates[pk.idx].push(Candidate {
                    sst: plan.ssts.len(),
                    state,
                });
            }
            if !sst.keys.is_empty() {
                plan.ssts.push(sst);
            }
        }
        plan
    }

    /// Apply the loaded filters of the UNKNOWN SST `sst`. An open key that
    /// they reject loses this candidate. The other open keys become POSITIVE.
    pub(crate) fn apply_filters(
        &mut self,
        sst: usize,
        filters: Arc<[NamedFilter]>,
        is_done: impl Fn(usize) -> bool,
        filter_context: &Option<FilterContext>,
        db_stats: Option<&DbStats>,
    ) {
        let open: Vec<PendingKey> = self.ssts[sst]
            .keys
            .iter()
            .filter(|pk| !is_done(pk.idx))
            .cloned()
            .collect();
        // Sorted, as `open` is.
        let kept: Vec<usize> = filter_keys(&open, &filters, filter_context, db_stats)
            .iter()
            .map(|pk| pk.idx)
            .collect();
        for pk in &open {
            let candidates = &mut self.candidates[pk.idx];
            if kept.binary_search(&pk.idx).is_err() {
                candidates.retain(|c| c.sst != sst);
                continue;
            }
            for candidate in candidates.iter_mut().filter(|c| c.sst == sst) {
                candidate.state = FilterState::Positive;
            }
        }
        self.ssts[sst].filters = Some(filters);
    }
}

/// How many candidates of one key, newest first, one round reads. The walk
/// stops after `limit` POSITIVE SSTs. An UNKNOWN SST is free, because the
/// pick only loads its filter. A limit of 0 acts as 1.
fn pick(candidates: &[Candidate], limit: usize) -> usize {
    let mut left = limit.max(1);
    for (i, candidate) in candidates.iter().enumerate() {
        if candidate.state == FilterState::Positive {
            left -= 1;
            if left == 0 {
                return i + 1;
            }
        }
    }
    candidates.len()
}

/// The first pick of a key: down to the first POSITIVE SST. A key with a
/// merge operand needs its base value, so it has no limit.
pub(crate) fn pick_first(candidates: &[Candidate], has_operand: bool) -> usize {
    pick(candidates, if has_operand { usize::MAX } else { 1 })
}

/// Each later pick of a key: `lookahead` POSITIVE SSTs, as `get` does after
/// its first miss.
pub(crate) fn pick_next(candidates: &[Candidate], has_operand: bool, lookahead: usize) -> usize {
    pick(candidates, if has_operand { usize::MAX } else { lookahead })
}

/// The SSTs that can hold the open keys, newest first inside each LSM tree.
/// `keys` must be sorted. The `filters` of each SST start as `None`.
pub(crate) async fn candidate_ssts(core: &ManifestCore, keys: &[KeyRead]) -> Vec<PlanSst> {
    let mut ssts = Vec::new();
    for (segment, keys) in group_by_segment(core, keys) {
        let tree = &segment.tree;
        let mut push = |view: &SsTableView, level: SstTraceLevel, keys: Vec<PendingKey>| {
            if !keys.is_empty() {
                ssts.push(PlanSst {
                    view: view.clone(),
                    segment: segment.prefix.clone(),
                    level,
                    filters: None,
                    index: None,
                    keys,
                });
            }
        };
        // An L0 SST can hold each key of its range.
        for view in tree.l0.iter() {
            let inside = keys
                .iter()
                .filter(|pk| view_holds_key(view, pk.key.as_ref()))
                .cloned()
                .collect();
            push(view, SstTraceLevel::L0, inside);
            // Keep the plan pass cooperative.
            tokio::task::coop::consume_budget().await;
        }
        for run in tree.compacted.iter() {
            for (vi, keys) in merge_join(&keys, run) {
                push(&run.sst_views()[vi], SstTraceLevel::SortedRun(run.id), keys);
            }
            // Keep the plan pass cooperative.
            tokio::task::coop::consume_budget().await;
        }
    }
    ssts
}

/// Group the open keys by the segment that covers them. Trees cover disjoint
/// keys, so the order of the groups has no effect on a key.
fn group_by_segment(core: &ManifestCore, keys: &[KeyRead]) -> Vec<(Segment, Vec<PendingKey>)> {
    let default_segment = core.default_segment();
    // The map key is the tree's Arc pointer cast to `usize` (a raw pointer
    // would make the enclosing future `!Send`).
    let mut group_of: HashMap<usize, usize> = HashMap::new();
    let mut groups: Vec<(Segment, Vec<PendingKey>)> = Vec::new();
    for (idx, KeyRead { key, done, .. }) in keys.iter().enumerate() {
        if *done {
            continue;
        }
        let segment =
            match core.select_segments(&BytesRange::from_slice(key.as_ref()..=key.as_ref())) {
                None => default_segment.clone(),
                Some(segments) => match segments.last() {
                    Some(segment) => segment.clone(),
                    // Configured but no segment covers this key: no on-disk data.
                    None => continue,
                },
            };
        let group = *group_of
            .entry(Arc::as_ptr(&segment.tree) as usize)
            .or_insert_with(|| {
                groups.push((segment, Vec::new()));
                groups.len() - 1
            });
        groups[group].1.push(PendingKey {
            idx,
            key: key.clone(),
        });
    }
    groups
}

/// Pair the sorted `keys` with the views of `run` that can hold them, in one
/// forward pass. The result maps a view index to its keys.
///
/// A key can have more than one view: its versions can cross the border of
/// two views, and `get` reads such views in ascending order too.
fn merge_join(keys: &[PendingKey], run: &SortedRun) -> BTreeMap<usize, Vec<PendingKey>> {
    let views = run.sst_views();
    let mut joined: BTreeMap<usize, Vec<PendingKey>> = BTreeMap::new();
    // The number of views whose start key is at most the current key.
    let mut started = 0;
    for pk in keys {
        let key = pk.key.as_ref();
        while started < views.len()
            && views[started].compacted_effective_start_key().as_ref() <= key
        {
            started += 1;
        }
        if started == 0 {
            continue;
        }
        for vi in run.point_table_idx_ending_at(key, started - 1) {
            joined.entry(vi).or_default().push(pk.clone());
        }
    }
    joined
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;

    use rstest::rstest;

    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo};
    use crate::filter_policy::{BloomFilterPolicy, FilterPolicy};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::LsmTreeState;
    use crate::types::RowEntry;

    use FilterState::{Positive, Unknown};

    /// A view with no SST behind it. The plan reads only the key range.
    fn view(range: &(&str, &str)) -> SsTableView {
        let info = SsTableInfo {
            first_entry: Some(Bytes::copy_from_slice(range.0.as_bytes())),
            last_entry: Some(Bytes::copy_from_slice(range.1.as_bytes())),
            ..Default::default()
        };
        let id = SsTableId::from(ulid::Ulid::new());
        SsTableView::identity(SsTableHandle::new(id, SST_FORMAT_VERSION_LATEST, info))
    }

    fn run(id: u32, ranges: &[(&str, &str)]) -> SortedRun {
        SortedRun::new(id, ranges.iter().map(view))
    }

    fn tree(l0: &[(&str, &str)], runs: &[(u32, &[(&str, &str)])]) -> Arc<LsmTreeState> {
        Arc::new(LsmTreeState {
            l0: l0.iter().map(view).collect::<VecDeque<_>>(),
            compacted: runs.iter().map(|(id, ranges)| run(*id, ranges)).collect(),
            ..Default::default()
        })
    }

    fn sorted_keys(keys: &[&str]) -> Vec<Bytes> {
        BatchKeys::new(keys).keys
    }

    fn key_reads(keys: &[&str], done: &[bool]) -> Vec<KeyRead> {
        let keys = sorted_keys(keys).into_iter().zip(done);
        let reads = keys.map(|(key, &done)| {
            let mut read = KeyRead::new(key);
            read.done = done;
            read
        });
        reads.collect()
    }

    fn pending(keys: &[Bytes]) -> Vec<PendingKey> {
        let pending = keys.iter().cloned().enumerate();
        pending.map(|(idx, key)| PendingKey { idx, key }).collect()
    }

    /// `l0:a` is the L0 SST whose first key is `a`. `sr2:a` is in run 2.
    fn label(sst: &PlanSst) -> String {
        let first = sst.view.sst.info.first_entry.clone().unwrap();
        let first = String::from_utf8(first.to_vec()).unwrap();
        match sst.level {
            SstTraceLevel::L0 => format!("l0:{first}"),
            SstTraceLevel::SortedRun(id) => format!("sr{id}:{first}"),
        }
    }

    fn labels(ssts: &[PlanSst]) -> Vec<(String, Vec<usize>)> {
        ssts.iter()
            .map(|sst| (label(sst), sst.keys.iter().map(|pk| pk.idx).collect()))
            .collect()
    }

    fn filters_of(keys: &[&str]) -> Arc<[NamedFilter]> {
        let policy = BloomFilterPolicy::new(10);
        let mut builder = policy.builder();
        for key in keys {
            builder.add_entry(&RowEntry::new_value(key.as_bytes(), b"", 0));
        }
        let filter = NamedFilter {
            name: policy.name().to_string(),
            filter: builder.build(),
        };
        Arc::from(vec![filter])
    }

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
        for (u, key) in batch.keys.iter().enumerate() {
            assert_eq!(batch.position(key), Some(u));
        }
        assert_eq!(batch.position(b"no such key"), None);
    }

    #[rstest]
    #[case::empty_run(&[], &["a", "b"])]
    #[case::before_first_view(&[("b", "d")], &["a"])]
    #[case::gap_and_after_last_view(&[("b", "d"), ("f", "h")], &["e", "z"])]
    #[case::many_keys_in_one_view(&[("b", "d"), ("f", "h")], &["b", "c", "d"])]
    // Two views share the border key `h`.
    #[case::shared_border_key(&[("b", "d"), ("f", "h"), ("h", "k")], &["g", "h", "j"])]
    #[case::all(&[("b", "d"), ("f", "h"), ("h", "k")], &["a", "b", "c", "e", "h", "j", "z"])]
    fn should_merge_join_like_a_binary_search_per_key(
        #[case] ranges: &[(&str, &str)],
        #[case] keys: &[&str],
    ) {
        let run = run(1, ranges);
        let keys = pending(&sorted_keys(keys));
        let mut expected: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for pk in &keys {
            for vi in run.point_table_idx_covering_key(pk.key.as_ref()) {
                expected.entry(vi).or_default().push(pk.idx);
            }
        }

        let joined: BTreeMap<usize, Vec<usize>> = merge_join(&keys, &run)
            .into_iter()
            .map(|(vi, keys)| (vi, keys.iter().map(|pk| pk.idx).collect()))
            .collect();

        assert_eq!(joined, expected);
    }

    const RUN_2: (u32, &[(&str, &str)]) = (2, &[("a", "f"), ("g", "z")]);
    const RUN_1: (u32, &[(&str, &str)]) = (1, &[("a", "z")]);

    #[rstest]
    #[case::no_ssts(&[], &[], &["a"], &[false], vec![])]
    #[case::l0_range_prune(
        &[("a", "c"), ("m", "p")], &[], &["b", "n", "z"], &[false; 3],
        vec![("l0:a", vec![0]), ("l0:m", vec![1])],
    )]
    #[case::l0_first_then_runs_in_order(
        &[("a", "z")], &[RUN_2, RUN_1], &["b", "h"], &[false; 2],
        vec![("l0:a", vec![0, 1]), ("sr2:a", vec![0]), ("sr2:g", vec![1]), ("sr1:a", vec![0, 1])],
    )]
    #[case::resolved_key_has_no_candidates(
        &[("a", "z")], &[RUN_2], &["b", "h"], &[true, false],
        vec![("l0:a", vec![1]), ("sr2:g", vec![1])],
    )]
    #[tokio::test]
    async fn should_list_candidate_ssts_newest_first(
        #[case] l0: &[(&str, &str)],
        #[case] runs: &[(u32, &[(&str, &str)])],
        #[case] keys: &[&str],
        #[case] resolved: &[bool],
        #[case] expected: Vec<(&str, Vec<usize>)>,
    ) {
        let mut core = ManifestCore::new();
        core.tree = tree(l0, runs);

        let ssts = candidate_ssts(&core, &key_reads(keys, resolved)).await;

        let expected: Vec<(String, Vec<usize>)> = expected
            .into_iter()
            .map(|(label, keys)| (label.to_string(), keys))
            .collect();
        assert_eq!(labels(&ssts), expected);
        assert!(ssts.iter().all(|sst| sst.filters.is_none()));
    }

    #[tokio::test]
    async fn should_plan_each_key_inside_its_segment() {
        let mut core = ManifestCore::new();
        // The default tree must not take part when segments are set.
        core.tree = tree(&[("a", "z")], &[]);
        core.segments = vec![
            Segment {
                prefix: Bytes::from_static(b"a/"),
                tree: tree(&[("a/1", "a/9")], &[]),
            },
            Segment {
                prefix: Bytes::from_static(b"b/"),
                tree: tree(&[], &[(7, &[("b/1", "b/9")])]),
            },
        ];

        let keys = key_reads(&["a/5", "b/5", "c/5"], &[false; 3]);
        let ssts = candidate_ssts(&core, &keys).await;

        assert_eq!(
            labels(&ssts),
            [
                ("l0:a/1".to_string(), vec![0]),
                ("sr7:b/1".to_string(), vec![1])
            ]
        );
        let segments: Vec<&[u8]> = ssts.iter().map(|sst| sst.segment.as_ref()).collect();
        assert_eq!(segments, [b"a/", b"b/"]);
    }

    /// An SST of a `Plan::new` case: the keys in its cached filter (`None` for
    /// filters that are not cached), and the indexes of its candidate keys.
    type SstCase = (Option<Vec<&'static str>>, Vec<usize>);

    #[rstest]
    #[case::not_cached_is_unknown(
        vec![(None, vec![0, 1])], 1,
        vec![vec![(0, Unknown)], vec![(0, Unknown)], vec![]],
    )]
    #[case::filter_pass_is_positive(
        vec![(Some(vec!["a", "b"]), vec![0, 1])], 1,
        vec![vec![(0, Positive)], vec![(0, Positive)], vec![]],
    )]
    #[case::filter_reject_removes_the_candidate(
        vec![(Some(vec!["a"]), vec![0, 1])], 1,
        vec![vec![(0, Positive)], vec![], vec![]],
    )]
    // SST 0 leaves the plan, so the next SST gets its index.
    #[case::rejected_sst_leaves_the_plan(
        vec![(Some(vec!["x"]), vec![0, 1]), (None, vec![1, 2])], 1,
        vec![vec![], vec![(0, Unknown)], vec![(0, Unknown)]],
    )]
    #[case::newest_first_per_key(
        vec![(None, vec![0]), (Some(vec!["a", "c"]), vec![0, 2]), (None, vec![2])], 3,
        vec![vec![(0, Unknown), (1, Positive)], vec![], vec![(1, Positive), (2, Unknown)]],
    )]
    fn should_mark_candidates_with_filter_states(
        #[case] ssts: Vec<SstCase>,
        #[case] expected_ssts: usize,
        #[case] expected: Vec<Vec<(usize, FilterState)>>,
    ) {
        let keys = sorted_keys(&["a", "b", "c"]);
        let ssts: Vec<PlanSst> = ssts
            .iter()
            .map(|(filter_keys, sst_keys)| {
                let filters = filter_keys.as_deref().map(filters_of);
                plan_sst(filters, &keys, sst_keys)
            })
            .collect();

        let plan = Plan::new(ssts, keys.len(), &None, None);

        assert_eq!(plan.ssts.len(), expected_ssts);
        let expected: Vec<Vec<Candidate>> = expected.iter().map(|c| candidates(c)).collect();
        assert_eq!(plan.candidates, expected);
    }

    #[test]
    fn should_treat_an_sst_with_no_filter_as_positive() {
        let keys = sorted_keys(&["a"]);
        let sst = plan_sst(Some(Arc::from(Vec::new())), &keys, &[0]);

        let plan = Plan::new(vec![sst], 1, &None, None);

        assert_eq!(plan.candidates, [candidates(&[(0, Positive)])]);
    }

    #[rstest]
    #[case::pass_becomes_positive(vec!["a", "b"], [false, false], vec![(1, Positive)], vec![(1, Positive)])]
    #[case::reject_removes_the_candidate(vec!["b"], [false, false], vec![], vec![(1, Positive)])]
    // Key `a` is resolved, so the filters do not probe it.
    #[case::resolved_key_is_skipped(vec!["b"], [true, false], vec![(1, Unknown)], vec![(1, Positive)])]
    fn should_apply_loaded_filters(
        #[case] filter_keys: Vec<&str>,
        #[case] resolved: [bool; 2],
        #[case] expected_a: Vec<(usize, FilterState)>,
        #[case] expected_b: Vec<(usize, FilterState)>,
    ) {
        let keys = sorted_keys(&["a", "b"]);
        // SST 0 holds only `b` and stays UNKNOWN. SST 1 gets the filters.
        let ssts = vec![plan_sst(None, &keys, &[1]), plan_sst(None, &keys, &[0, 1])];
        let mut plan = Plan::new(ssts, keys.len(), &None, None);

        plan.apply_filters(1, filters_of(&filter_keys), |u| resolved[u], &None, None);

        assert!(plan.ssts[1].filters.is_some());
        assert_eq!(plan.candidates[0], candidates(&expected_a));
        let mut expected = candidates(&[(0, Unknown)]);
        expected.extend(candidates(&expected_b));
        assert_eq!(plan.candidates[1], expected);
    }

    const U: (usize, FilterState) = (0, Unknown);
    const P: (usize, FilterState) = (0, Positive);

    #[rstest]
    #[case::empty(&[], 1, 0)]
    #[case::stops_at_the_first_positive(&[P, P, P], 1, 1)]
    #[case::unknown_is_free(&[U, U, P, P], 1, 3)]
    #[case::all_unknown_picks_all(&[U, U, U], 1, 3)]
    #[case::limit_counts_positives(&[P, U, P, U, P], 2, 3)]
    #[case::limit_above_the_list(&[P, U], 4, 2)]
    #[case::zero_acts_as_one(&[P, P], 0, 1)]
    fn should_pick_up_to_the_limit(
        #[case] list: &[(usize, FilterState)],
        #[case] limit: usize,
        #[case] expected: usize,
    ) {
        assert_eq!(pick(&candidates(list), limit), expected);
    }

    #[rstest]
    #[case::first_pick_reads_one(true, false, 4, 1)]
    #[case::first_pick_with_operand_reads_all(true, true, 4, 6)]
    #[case::later_pick_reads_lookahead(false, false, 4, 4)]
    #[case::later_pick_with_lookahead_1(false, false, 1, 1)]
    #[case::later_pick_with_lookahead_0(false, false, 0, 1)]
    #[case::later_pick_with_operand_reads_all(false, true, 1, 6)]
    fn should_limit_each_pick(
        #[case] first_pick: bool,
        #[case] has_operand: bool,
        #[case] lookahead: usize,
        #[case] expected: usize,
    ) {
        let list = candidates(&[P; 6]);
        let picked = match first_pick {
            true => pick_first(&list, has_operand),
            false => pick_next(&list, has_operand, lookahead),
        };
        assert_eq!(picked, expected);
    }

    /// An SST over all keys of the range `a..z`, with the keys `sst_keys`.
    fn plan_sst(
        filters: Option<Arc<[NamedFilter]>>,
        keys: &[Bytes],
        sst_keys: &[usize],
    ) -> PlanSst {
        PlanSst {
            view: view(&("a", "z")),
            segment: Bytes::new(),
            level: SstTraceLevel::L0,
            filters,
            index: None,
            keys: pending(keys)
                .into_iter()
                .filter(|pk| sst_keys.contains(&pk.idx))
                .collect(),
        }
    }

    fn candidates(list: &[(usize, FilterState)]) -> Vec<Candidate> {
        list.iter()
            .map(|&(sst, state)| Candidate { sst, state })
            .collect()
    }
}
