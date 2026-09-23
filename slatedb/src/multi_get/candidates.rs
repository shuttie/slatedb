//! Which SSTs can hold which keys of a `multi_get` batch.
//!
//! The walk reads only the manifest. The filters load later, when a key
//! reaches an SST (see [`super::key::KeyRead::pick`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;

use crate::bytes_range::BytesRange;
use crate::db_state::{SsTableHandle, SsTableView};
use crate::db_stats::DbStats;
use crate::filter_policy::{FilterContext, FilterQuery, NamedFilter};
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::manifest::{ManifestCore, Segment};
use crate::reader::{ReadTrace, SstTraceLevel};

use super::key::KeyRead;
use super::KEYS_PER_YIELD;

/// The filters of one SST of the batch.
pub(crate) enum Filters {
    NotLoaded,
    /// A load is in flight. The keys wait for it.
    Loading(Vec<usize>),
    /// Empty when the SST has no filter.
    Loaded(Arc<[NamedFilter]>),
}

/// What a read of one SST needs to know about it.
#[derive(Clone)]
pub(crate) struct SstTarget {
    pub(crate) handle: SsTableHandle,
    pub(crate) segment: Bytes,
    pub(crate) level: SstTraceLevel,
}

/// One SST that can hold keys of the batch.
pub(crate) struct BatchSst {
    pub(crate) target: SstTarget,
    pub(crate) filters: Filters,
    /// The index after the first read of the batch. It saves a load when the
    /// store has no cache.
    pub(crate) index: Option<Arc<SsTableIndexOwned>>,
    /// The span of the filter probes, from the first probe on.
    filter_span: Option<tracing::Span>,
    /// The keys that the filters checked, and the keys that they passed.
    probes: u64,
    positives: u64,
}

impl BatchSst {
    pub(crate) fn new(view: &SsTableView, segment: Bytes, level: SstTraceLevel) -> Self {
        Self {
            target: SstTarget {
                handle: view.sst.clone(),
                segment,
                level,
            },
            filters: Filters::NotLoaded,
            index: None,
            filter_span: None,
            probes: 0,
            positives: 0,
        }
    }

    /// Whether the filters pass `key`, or `None` when they are not loaded.
    /// Records the filter stats, as `get` does. One span covers all probes
    /// of the SST, with the counts of the probes and the passes so far.
    pub(crate) fn might_hold(
        &mut self,
        key: &Bytes,
        filter_context: &Option<FilterContext>,
        db_stats: &DbStats,
        read_trace: &ReadTrace,
    ) -> Option<bool> {
        let Filters::Loaded(filters) = &self.filters else {
            return None;
        };
        if filters.is_empty() {
            return Some(true);
        }
        let span = self.filter_span.get_or_insert_with(|| {
            read_trace.new_evaluate_filters_span(self.target.handle.id, Some(&self.target.level))
        });
        let _guard = span.enter();
        let query = FilterQuery::point(key.clone()).with_context(filter_context.clone());
        let pass = filters.iter().all(|nf| nf.filter.might_match(&query));
        match pass {
            true => db_stats.sst_filter_point_positives.increment(1),
            false => db_stats.sst_filter_point_negatives.increment(1),
        }
        self.probes += 1;
        self.positives += u64::from(pass);
        span.record("keys", self.probes);
        span.record("positives", self.positives);
        Some(pass)
    }

    /// The SST has loaded filters. A key that they pass and that the SST
    /// does not hold is a false positive.
    pub(crate) fn has_filters(&self) -> bool {
        matches!(&self.filters, Filters::Loaded(filters) if !filters.is_empty())
    }
}

/// The SSTs that can hold the open keys. Each key gets its candidates newest
/// first, as `get` reads them: the L0 SSTs, then the sorted runs.
pub(crate) async fn candidate_ssts(core: &ManifestCore, keys: &mut [KeyRead]) -> Vec<BatchSst> {
    let mut ssts: Vec<BatchSst> = Vec::new();
    let add = |ssts: &mut Vec<BatchSst>, view: &SsTableView, segment: &Segment, level| {
        ssts.push(BatchSst::new(view, segment.prefix.clone(), level));
        ssts.len() - 1
    };
    for (segment, group) in group_by_segment(core, keys).await {
        let tree = &segment.tree;
        // An L0 SST can hold each key of its range.
        for view in tree.l0.iter() {
            let mut sst = None;
            for chunk in group.chunks(KEYS_PER_YIELD) {
                for &u in chunk {
                    if view_holds_key(view, &keys[u].key) {
                        let sst = *sst.get_or_insert_with(|| {
                            add(&mut ssts, view, &segment, SstTraceLevel::L0)
                        });
                        keys[u].add_candidate(sst);
                    }
                }
                // Keep the candidate walk cooperative.
                tokio::task::coop::consume_budget().await;
            }
        }
        for run in tree.compacted.iter() {
            // A key can have more than one view in a run: its versions can
            // cross the border of two views.
            let mut sst_of_view: BTreeMap<usize, usize> = BTreeMap::new();
            for chunk in group.chunks(KEYS_PER_YIELD) {
                for &u in chunk {
                    for vi in run.point_table_idx_covering_key(&keys[u].key) {
                        let view = &run.sst_views()[vi];
                        // The last view of a run has no upper bound. `get`
                        // drops it for a key outside the view's range, so
                        // skip it too.
                        if !view_holds_key(view, &keys[u].key) {
                            continue;
                        }
                        let sst = *sst_of_view.entry(vi).or_insert_with(|| {
                            let level = SstTraceLevel::SortedRun(run.id);
                            add(&mut ssts, view, &segment, level)
                        });
                        keys[u].add_candidate(sst);
                    }
                }
                // Keep the candidate walk cooperative.
                tokio::task::coop::consume_budget().await;
            }
        }
    }
    ssts
}

/// Visible-range projection (segments / clones). For identity views this
/// also prunes keys outside the SST's physical key range.
fn view_holds_key(view: &SsTableView, key: &[u8]) -> bool {
    view.calculate_view_range(BytesRange::from_slice(key..=key))
        .is_some()
}

/// Group the open keys by the segment that covers them. Trees cover disjoint
/// keys, so the order of the groups has no effect on a key.
async fn group_by_segment(core: &ManifestCore, keys: &[KeyRead]) -> Vec<(Segment, Vec<usize>)> {
    let default_segment = core.default_segment();
    // The map key is the tree's Arc pointer cast to `usize` (a raw pointer
    // would make the enclosing future `!Send`).
    let mut group_of: HashMap<usize, usize> = HashMap::new();
    let mut groups: Vec<(Segment, Vec<usize>)> = Vec::new();
    for (idx, KeyRead { key, done, .. }) in keys.iter().enumerate() {
        if idx % KEYS_PER_YIELD == 0 {
            // Keep the grouping cooperative.
            tokio::task::coop::consume_budget().await;
        }
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
        groups[group].1.push(idx);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::ops::Bound::{Excluded, Unbounded};

    use rstest::rstest;

    use slatedb_common::metrics::MetricsRecorderHelper;

    use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use crate::filter_policy::{BloomFilterPolicy, FilterPolicy};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::LsmTreeState;
    use crate::multi_get::key::BatchKeys;
    use crate::types::RowEntry;

    /// An SST that holds the keys `first..=last`. A clone sees only the keys
    /// before `visible_end`.
    #[derive(Clone, Copy)]
    struct Sst {
        first: &'static str,
        last: &'static str,
        visible_end: Option<&'static str>,
    }

    const fn sst(first: &'static str, last: &'static str) -> Sst {
        Sst {
            first,
            last,
            visible_end: None,
        }
    }

    const fn clone_of(sst: Sst, visible_end: &'static str) -> Sst {
        Sst {
            visible_end: Some(visible_end),
            ..sst
        }
    }

    /// A view with no SST behind it. The walk reads only the key range.
    fn view(sst: &Sst) -> SsTableView {
        let info = SsTableInfo {
            first_entry: Some(Bytes::from_static(sst.first.as_bytes())),
            last_entry: Some(Bytes::from_static(sst.last.as_bytes())),
            ..Default::default()
        };
        let ulid = ulid::Ulid::new();
        let handle = SsTableHandle::new(SsTableId::from(ulid), SST_FORMAT_VERSION_LATEST, info);
        match sst.visible_end {
            None => SsTableView::identity(handle),
            Some(end) => {
                let visible =
                    BytesRange::new(Unbounded, Excluded(Bytes::from_static(end.as_bytes())));
                SsTableView::new_projected(ulid, handle, Some(visible))
            }
        }
    }

    fn run(id: u32, ssts: &[Sst]) -> SortedRun {
        SortedRun::new(id, ssts.iter().map(view))
    }

    fn tree(l0: &[Sst], runs: &[(u32, &[Sst])]) -> Arc<LsmTreeState> {
        Arc::new(LsmTreeState {
            l0: l0.iter().map(view).collect::<VecDeque<_>>(),
            compacted: runs.iter().map(|(id, ssts)| run(*id, ssts)).collect(),
            ..Default::default()
        })
    }

    fn key_reads(keys: &[&str], done: &[bool]) -> Vec<KeyRead> {
        let keys = BatchKeys::new(keys).keys.into_iter().zip(done);
        let reads = keys.map(|(key, &done)| {
            let mut read = KeyRead::new(key);
            read.done = done;
            read
        });
        reads.collect()
    }

    /// `l0:a` is the L0 SST whose first key is `a`. `sr2:a` is in run 2.
    fn label(sst: &BatchSst) -> String {
        let first = sst.target.handle.info.first_entry.clone().unwrap();
        let first = String::from_utf8(first.to_vec()).unwrap();
        match sst.target.level {
            SstTraceLevel::L0 => format!("l0:{first}"),
            SstTraceLevel::SortedRun(id) => format!("sr{id}:{first}"),
        }
    }

    /// Each SST with the keys that have it as a candidate.
    fn labels(ssts: &[BatchSst], keys: &[KeyRead]) -> Vec<(String, Vec<usize>)> {
        let keys_of = |sst: usize| -> Vec<usize> {
            let keys = keys.iter().enumerate();
            let keys = keys.filter(|(_, key)| key.candidate_ssts().contains(&sst));
            keys.map(|(u, _)| u).collect()
        };
        let ssts = ssts.iter().enumerate();
        ssts.map(|(sst, batch_sst)| (label(batch_sst), keys_of(sst)))
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

    const RUN_2: (u32, &[Sst]) = (2, &[sst("a", "f"), sst("g", "z")]);
    const RUN_1: (u32, &[Sst]) = (1, &[sst("a", "z")]);
    const RUN_3: (u32, &[Sst]) = (3, &[sst("b", "d"), sst("f", "h"), sst("h", "k")]);
    const RUN_4: (u32, &[Sst]) = (4, &[clone_of(sst("a", "z"), "h")]);

    #[rstest]
    #[case::no_ssts(&[], &[], &["a"], &[false], vec![])]
    #[case::l0_range_prune(
        &[sst("a", "c"), sst("m", "p")], &[], &["b", "n", "z"], &[false; 3],
        vec![("l0:a", vec![0]), ("l0:m", vec![1])],
    )]
    #[case::l0_first_then_runs_in_order(
        &[sst("a", "z")], &[RUN_2, RUN_1], &["b", "h"], &[false; 2],
        vec![("l0:a", vec![0, 1]), ("sr2:a", vec![0]), ("sr2:g", vec![1]), ("sr1:a", vec![0, 1])],
    )]
    #[case::resolved_key_has_no_candidates(
        &[sst("a", "z")], &[RUN_2], &["b", "h"], &[true, false],
        vec![("l0:a", vec![1]), ("sr2:g", vec![1])],
    )]
    // Two views share the border key `h`, and the key reads both. `z` is
    // past the last key of the run, so it gets no view.
    #[case::border_key_has_two_views(
        &[], &[RUN_3], &["a", "c", "h", "z"], &[false; 4],
        vec![("sr3:b", vec![1]), ("sr3:f", vec![2]), ("sr3:h", vec![2])],
    )]
    // The clone holds `i`, but its view ends before `h`, so `get` skips it.
    #[case::clone_hides_keys_past_its_view(
        &[], &[RUN_4], &["c", "i"], &[false; 2],
        vec![("sr4:a", vec![0])],
    )]
    #[tokio::test]
    async fn should_list_candidate_ssts_newest_first(
        #[case] l0: &[Sst],
        #[case] runs: &[(u32, &[Sst])],
        #[case] keys: &[&str],
        #[case] resolved: &[bool],
        #[case] expected: Vec<(&str, Vec<usize>)>,
    ) {
        let mut core = ManifestCore::new();
        core.tree = tree(l0, runs);
        let mut keys = key_reads(keys, resolved);

        let ssts = candidate_ssts(&core, &mut keys).await;

        let expected: Vec<(String, Vec<usize>)> = expected
            .into_iter()
            .map(|(label, keys)| (label.to_string(), keys))
            .collect();
        assert_eq!(labels(&ssts, &keys), expected);
        // The SSTs are in walk order, so each list is newest first.
        for key in &keys {
            assert!(key.candidate_ssts().is_sorted());
        }
        let not_loaded = |sst: &BatchSst| matches!(sst.filters, Filters::NotLoaded);
        assert!(ssts.iter().all(not_loaded));
    }

    #[tokio::test]
    async fn should_list_each_key_inside_its_segment() {
        let mut core = ManifestCore::new();
        // The default tree must not take part when segments are set.
        core.tree = tree(&[sst("a", "z")], &[]);
        core.segments = vec![
            Segment {
                prefix: Bytes::from_static(b"a/"),
                tree: tree(&[sst("a/1", "a/9")], &[]),
            },
            Segment {
                prefix: Bytes::from_static(b"b/"),
                tree: tree(&[], &[(7, &[sst("b/1", "b/9")])]),
            },
        ];

        let mut keys = key_reads(&["a/5", "b/5", "c/5"], &[false; 3]);
        let ssts = candidate_ssts(&core, &mut keys).await;

        assert_eq!(
            labels(&ssts, &keys),
            [
                ("l0:a/1".to_string(), vec![0]),
                ("sr7:b/1".to_string(), vec![1])
            ]
        );
        let segments: Vec<&[u8]> = ssts.iter().map(|sst| sst.target.segment.as_ref()).collect();
        assert_eq!(segments, [b"a/", b"b/"]);
    }

    #[rstest]
    #[case::not_loaded(None, None)]
    #[case::no_filter(Some(vec![]), Some(true))]
    #[case::pass(Some(vec!["a", "b"]), Some(true))]
    #[case::reject(Some(vec!["b"]), Some(false))]
    fn should_check_key_against_loaded_filters(
        #[case] filter_keys: Option<Vec<&str>>,
        #[case] expected: Option<bool>,
    ) {
        let mut sst = BatchSst::new(&view(&sst("a", "z")), Bytes::new(), SstTraceLevel::L0);
        sst.filters = match filter_keys {
            None => Filters::NotLoaded,
            Some(keys) if keys.is_empty() => Filters::Loaded(Arc::from([])),
            Some(keys) => Filters::Loaded(filters_of(&keys)),
        };
        let db_stats = DbStats::new(&MetricsRecorderHelper::noop());
        let read_trace = ReadTrace::new(None);

        let key = Bytes::from_static(b"a");
        assert_eq!(
            sst.might_hold(&key, &None, &db_stats, &read_trace),
            expected
        );
    }
}
