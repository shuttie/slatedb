//! The read phase of `multi_get`: pipelined reads of the candidate SSTs.
//!
//! Each open key reads its candidates newest first, in rounds. One round is
//! one pick ([`KeyRead::pick`]) and the reads of the picked SSTs. The keys do
//! not wait for each other: when the read of one SST returns, the keys of
//! that read make their next pick at once. A batch that reads in lockstep
//! pays the tail latency of the object store one time per step. A pipeline
//! pays it about one time.
//!
//! The [`Pipeline`] drives an event loop over the futures of its filter
//! loads and reads. The futures run on the task of the batch, so a drop of
//! the batch cancels them.

use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::error::SlateDBError;
use crate::filter_policy::NamedFilter;
use crate::types::RowEntry;

use super::candidates::{BatchSst, Filters};
use super::key::{KeyRead, Pick};
use super::sst::{PendingKey, SstEntries, SstReader};

/// What one future of the read phase returns.
enum Event {
    /// The filters of one SST.
    Filters {
        /// Index into [`Pipeline::ssts`].
        sst: usize,
        filters: Arc<[NamedFilter]>,
    },
    /// The read of one SST for some of its keys.
    Read {
        /// Index into [`Pipeline::ssts`].
        sst: usize,
        /// The keys that the read got. A key that the SST does not hold has
        /// no entries, but it still arrives.
        keys: Vec<usize>,
        entries: SstEntries,
    },
}

/// The read phase of one batch. It owns the state of the SSTs and borrows
/// the state of the keys.
pub(crate) struct Pipeline<'a> {
    reader: SstReader<'a>,
    keys: &'a mut [KeyRead],
    /// The SSTs that can hold keys. A key names them by index.
    ssts: Vec<BatchSst>,
    max_seq: Option<u64>,
    /// The keys that wait for a read of each SST. The reads start at the end
    /// of each turn of the loop, so the keys of one turn share a read.
    ready: BTreeMap<usize, Vec<PendingKey>>,
    /// The filter loads and the reads in flight.
    events: FuturesUnordered<BoxFuture<'a, Result<Event, SlateDBError>>>,
}

impl<'a> Pipeline<'a> {
    pub(crate) fn new(
        reader: SstReader<'a>,
        keys: &'a mut [KeyRead],
        ssts: Vec<BatchSst>,
        max_seq: Option<u64>,
    ) -> Self {
        Self {
            reader,
            keys,
            ssts,
            max_seq,
            ready: BTreeMap::new(),
            events: FuturesUnordered::new(),
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
        self.start_reads();
        while let Some(event) = self.events.next().await {
            match event? {
                Event::Filters { sst, filters } => self.on_filters(sst, filters).await,
                Event::Read { sst, keys, entries } => self.on_read(sst, keys, entries).await,
            }
            self.start_reads();
        }
        Ok(self.keys.iter().map(|key| key.rounds).max().unwrap_or(0))
    }

    /// Make the next pick of an idle open key. The key waits for filter
    /// loads, or its SSTs go to `ready`.
    fn schedule(&mut self, u: usize) {
        let (options, db_stats) = (self.reader.options, self.reader.db_stats);
        let ssts = &self.ssts;
        let pick = self.keys[u].pick(options.lookahead, |sst, key| {
            ssts[sst].might_hold(key, &options.filter_context, db_stats)
        });
        match pick {
            // The key is absent, or it has only merge operands.
            Pick::Done => self.keys[u].done = true,
            Pick::Load(ssts) => {
                self.keys[u].waiting = ssts.len();
                for sst in ssts {
                    self.load_filters(sst, u);
                }
            }
            Pick::Read(ssts) => {
                self.keys[u].send(&ssts);
                for sst in ssts {
                    let key = self.keys[u].key.clone();
                    let pending = PendingKey { idx: u, key };
                    self.ready.entry(sst).or_default().push(pending);
                }
            }
        }
    }

    /// Start the filter load of one SST for the key `u`, unless one is in
    /// flight.
    fn load_filters(&mut self, sst: usize, u: usize) {
        let batch_sst = &mut self.ssts[sst];
        match &mut batch_sst.filters {
            Filters::Loading(waiters) => waiters.push(u),
            Filters::NotLoaded => {
                batch_sst.filters = Filters::Loading(vec![u]);
                let load = self.reader.load_filters(batch_sst.target.clone());
                let event = async move {
                    let filters = load.await?;
                    Ok(Event::Filters { sst, filters })
                };
                self.events.push(event.boxed());
            }
            Filters::Loaded(_) => unreachable!("a pick loads only filters that are not loaded"),
        }
    }

    /// Start one read per SST of `ready`.
    fn start_reads(&mut self) {
        for (sst, keys) in mem::take(&mut self.ready) {
            let batch_sst = &self.ssts[sst];
            let target = batch_sst.target.clone();
            let index = batch_sst.index.clone();
            let idxs: Vec<usize> = keys.iter().map(|pk| pk.idx).collect();
            let read = self
                .reader
                .read(target, index, batch_sst.has_filters(), keys);
            let event = async move {
                let entries = read.await?;
                Ok(Event::Read {
                    sst,
                    keys: idxs,
                    entries,
                })
            };
            self.events.push(event.boxed());
        }
    }

    /// Apply the loaded filters of one SST, then make the pick of each key
    /// that waits for no other load.
    async fn on_filters(&mut self, sst: usize, filters: Arc<[NamedFilter]>) {
        let loaded = Filters::Loaded(filters);
        let Filters::Loading(waiters) = mem::replace(&mut self.ssts[sst].filters, loaded) else {
            unreachable!("only a load in flight gets filters");
        };
        for u in waiters {
            let key = &mut self.keys[u];
            key.waiting -= 1;
            if !key.done && key.is_idle() {
                self.schedule(u);
            }
            // Keep the pick pass cooperative.
            tokio::task::coop::consume_budget().await;
        }
    }

    /// Apply the read of one SST to its keys. A key whose pick is complete
    /// makes its next pick.
    async fn on_read(&mut self, sst: usize, keys: Vec<usize>, entries: SstEntries) {
        self.ssts[sst].index = Some(entries.index);
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
}
