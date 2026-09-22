# Batched Point Reads (`multi_get`)

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
  - [What a `get` in a loop repeats](#what-a-get-in-a-loop-repeats)
  - [Example](#example)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Overview](#overview)
  - [Public API](#public-api)
  - [Options](#options)
  - [Plan phase](#plan-phase)
  - [Read phase](#read-phase)
  - [Value resolution](#value-resolution)
  - [Transactions](#transactions)
  - [Failure handling](#failure-handling)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Roman Grebennikov](https://github.com/shuttie)

## Summary

This RFC adds `multi_get`, a call that reads a batch of keys at one time. Today
an application that needs 1000 keys calls `get` 1000 times. Each call repeats
the same setup, and reads the same filters and indexes again.

`multi_get` works per SST and not per key:

- It answers what it can from memory. Then it groups the other keys by the
  SSTs that can hold them.
- It reads each filter, each index, and each data block at most one time per
  batch, and it reads all SSTs in parallel.
- It reads newest SST first. A key reads an older SST only when the newer
  one did not answer it, and no key waits for the reads of another key. A
  batch takes about one tail latency of the object store, and it sends no
  more requests than the loop.

Each key returns the same value as a `get`, and all keys of a batch read from
one state view. The change is additive: `get`, the SST format, and the
manifest stay as they are. It has one breaking change: external types that
implement `DbReadOps` must add the new methods.

## Motivation

A typical read pattern of an ML feature store is to fan out one batch request
into many point reads. A ranking service is a good example:
* It gets a list of random candidate keys, usually 100 to 1000 of them.
* It loads a set of features for each candidate. The dataset is usually
  100+ GB.
* It runs the ML inference.

In practice, the keys are often not 100% random:
* Hot keys follow a Pareto-style distribution. In theory, they can stay warm
  in RAM or in a local disk cache.
* A single offline precompute job usually updates the keys in one large batch.

Other stores have a call for this pattern: `MultiGet` in RocksDB, `MGET` in
Redis, and `BatchGetItem` in DynamoDB. SlateDB does not, so the application
must call `get` in a tight loop. The RFC author maintains
[murrdb](https://github.com/murrdb/murr), which uses this pattern on RocksDB.

### What a `get` in a loop repeats

Random keys usually spread over the whole key space, so the SSTs at the top of
the tree serve many keys of the same batch. An L0 SST covers the whole key
space, and it is a candidate for each key. A simple loop cannot use this fact.
It must repeat all the shared work on each iteration.

In the following table, N is the number of keys, M is the number of memtables,
and S is the number of candidate SSTs for a key:

| Step of `get`                     | Loop of N gets | One batch needs      |
|-----------------------------------|----------------|----------------------|
| State view, `max_seq`, trace span | N              | 1                    |
| Iterator per memtable and SST     | N × (M + S)    | 0                    |
| Binary search in each sorted run  | N per run      | 1 pass per run       |
| Filter read (cache or GET)        | N × S          | S                    |
| Index read (cache or GET)         | up to N × S    | up to S              |
| Block read (task, cache or GET)   | N              | 1 per distinct block |
| Walk through the layers           | N walks        | 1 fan-out            |

### Example

For a batch of 1000 keys with 2 memtables and 20 candidate SSTs, the loop:
* Builds 22k iterators.
* Does 20k filter reads to reach 20 filters.
* Does up to 20k index reads to reach 20 indexes.
* Starts 1k block reads, each in its own task.

A warm cache makes most of these steps quite cheap, but it still makes no sense
to repeat this work for each key:
* Block reads are never merged. A batch can read adjacent blocks with one S3
  GET request. Independent gets cannot, because no `get` knows about the
  others.
* Metadata reads scale with the number of keys, and not with the number of
  SSTs.
* Concurrency (for example, a loop of parallel `get` calls) hides the latency
  but not the cost.
* A loop has no consistent view. Each `get` takes its own state view, so the
  keys of one batch can see different DB states.

## Goals

- Return the same results as a `get` for each key, with all keys reading from one state view.
- Share the work that a `get` in a loop repeats. Do the setup once per batch.
- Never cost more than the loop. A batch read sends no more object store requests than a `get` loop, plus at most one filter load per SST whose filter is not in the cache.
- Bound the concurrency and memory of one batch.

## Non-Goals

- Change `get`, the SST format, the manifest, or the cache behavior. This is purely additive change.
- Add new result shapes. Results come back in input order, one slot per key.
- Batch across calls or across operations. Separate `multi_get` calls do not share work, and range scans stay as they are.

## Design

### Overview

`multi_get` has two phases:

- The plan phase uses only things which are kept in memory. It answers what it can from the write batch
  and the memtables. For each key that is still open, it builds the list of
  SSTs that can hold the key, newest first.
- The read phase fetches data in rounds, per key. In its first round, each
  open key reads only the newest SST that can hold it. A key goes to its
  next round only when that SST did not answer it. The keys do not wait for
  each other: when the read of one SST returns, its keys make their next
  pick at once.

The motivation of such iterative design is that it's not possible to build a full deterministic plan
of the multi_get batch read.

```text
keys --> PLAN (memory only) ------------> READ (object store)
         1. state view, max_seq            round 1: newest candidate per key
         2. dedup, sort                    round 2: a key that is still open
         3. write batch, memtables         ...
         4. group keys per SST
         5. probe the cached filters
```

The unit of work is the SST and not the key. One SST serves all of its keys
with one filter, one index, and one read per distinct block.

In other simpler words:
* we plan the first read of each key
* then each key that is still open reads on, as soon as its last read
  returns.

### Public API

`DbReadOps` gets four methods that mirror the `get` family. `Db`, `DbReader`,
`DbSnapshot`, and `DbTransaction` implement them.

As in the `get` family, the two `_with_options` methods have no default body.
The two short forms call them with default options. This breaks code outside
SlateDB that implements `DbReadOps`. See [Compatibility](#compatibility).

```rust
async fn multi_get<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K],
) -> Result<Vec<Option<Bytes>>, Error>;

async fn multi_get_with_options<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K], options: &MultiGetOptions,
) -> Result<Vec<Option<Bytes>>, Error>;

async fn multi_get_key_value<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K],
) -> Result<Vec<Option<KeyValue>>, Error>;

async fn multi_get_key_value_with_options<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K], options: &MultiGetOptions,
) -> Result<Vec<Option<KeyValue>>, Error>;
```

`K` needs `Sync`, and `get` does not. The reason is the borrowed slice:

- `DbReadOps` uses `#[async_trait]`, so each method returns a `Send` future.
- The future holds `keys: &[K]`, and `&[K]` is `Send` only when `K` is `Sync`.
- `get` takes its key by value, so `Send` is enough there.

The common key types (`Vec<u8>`, `Bytes`, `&[u8]`, `String`) are all `Sync`, so
callers do not see the bound. A hand-written signature with a boxed future can
drop it, but each implementor of the trait then has to write that signature.

The result has one slot per input key, in input order:

- `None` means that the key is absent or deleted.
- A duplicate key is read one time. Its result is copied to each of its slots.
- An empty input returns an empty vector.
- There is no limit on the number of keys. The caller owns the batch size, as
  it does for `WriteBatch` and for scans.

### Options

`MultiGetOptions` is a separate struct. It repeats the fields of `ReadOptions`,
in the same way that `ScanOptions` does, and adds four fields for the batch.

```rust
pub struct MultiGetOptions {
    // Same meaning as in ReadOptions.
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
    pub filter_context: Option<FilterContext>,
    pub tracing_options: Option<TracingOptions>,

    /// Max object store requests of one batch in flight. Default: 256.
    pub max_fetch_tasks: usize,
    /// Max known-positive SSTs that a key reads in its second round and
    /// later. The first round always reads one. Default: 4, the lookahead
    /// of `get`.
    pub lookahead: usize,
    /// Two blocks go into one ranged GET when the gap between them is at
    /// most this many bytes. With 0, only adjacent blocks merge.
    /// Default: 64 KiB.
    pub coalesce_gap_bytes: usize,
    /// Upper size of one merged ranged GET. Default: 4 MiB.
    pub max_coalesced_bytes: usize,
}
```

Notes on the fields:

- `max_fetch_tasks` counts requests and not SSTs. A batch over a large bottom
  run touches about 100 SSTs and sends about 1000 block reads. A low limit on
  SSTs makes such a batch slower than `join_all` over gets.
- `lookahead` trades requests for latency. A value of 1 gives the fewest
  requests. It matters only for keys with more than one positive filter.
- Block merging helps when the keys of a batch cluster, for example under one
  prefix. Random keys in a 1 GiB SST almost never share a range.

### Plan phase

```text
# 1. Setup, one time per batch
view      = db.state_view()
max_seq   = prepare_max_seq(options)
open_keys = sort(dedup(keys))             # copied into Bytes

# 2. Memory: the write batch, then the memtables, newest first
for key in open_keys:
    for table in [write_batch, memtable, *immutable_memtables]:
        entry = table.get(key, max_seq)
        if entry is a value or a tombstone:
            results[key] = entry          # done, the key needs no SST
            break
        if entry is a merge operand:
            operands[key].push(entry)     # the key still needs a base value
open_keys -= keys in results

# 3. Candidates: the SSTs that can hold each key, newest first
for sst in view.l0:                       # an L0 SST can hold any key
    for key in open_keys inside sst.key_range:
        candidates[key].push(sst)
for run in view.sorted_runs:              # a run has one SST per key
    for (key, sst) in merge_join(open_keys, run.ssts):
        candidates[key].push(sst)

# 4. Filters: use what is in the cache, never load
for sst in all SSTs in candidates:
    filter = cache.peek_filter(sst)       # no object store request
    for key in keys that have sst as a candidate:
        if filter is not in the cache:    mark (key, sst) as UNKNOWN
        elif filter.might_match(key):     mark (key, sst) as POSITIVE
        else:                             remove sst from candidates[key]

# 5. A key with no candidates and no operands is absent
for key in open_keys:
    if candidates[key] is empty and operands[key] is empty:
        results[key] = None
```

With segments, steps 3 and 4 run inside the segment that covers the key.

The reasons behind the steps:

- The keys are copied into `Bytes` and sorted. The copy lets spawned tasks
  share the keys with no borrow of the input slice. The sort turns N binary
  searches in a sorted run into one forward pass.
- The plan phase builds no iterators. A `get` builds one iterator per memtable
  and per candidate SST before its first lookup.
- The plan phase never loads a filter. A `get` reads the filter of an older
  SST only when the newer SSTs did not answer. A batch that loads all filters
  up front sends more requests than a loop when the cache is cold.
- The loops over the keys yield to the runtime at a fixed interval, with
  `consume_budget`. This holds for the plan phase and for each key pass of
  the read phase. A `get` yields one time per entry. A loop over thousands of keys
  with no yield blocks the other tasks of the thread.

### Read phase

The read phase is an event loop. Each open key makes a pick, and the picked
SSTs are read. When a read returns, the keys of that read apply its entries
and make their next pick. A key that is done leaves the loop. The loop ends
when no read is in flight.

```text
loaded = {}                               # filters and indexes of this batch
tasks  = {}                               # one JoinSet for the whole batch

schedule(key):                            # the key is open and idle
    picked = pick_next(candidates[key])   # see the rules below
    if picked has an UNKNOWN sst:
        for sst in these UNKNOWN ssts, not yet loading:
            spawn load_filter(sst)        # -> Filters event
        mark the key as waiting
        return
    remove picked from candidates[key]
    inflight[key] = picked                # newest first
    for sst in picked:
        ready[sst].push(key)              # group the keys per SST

flush():                                  # ready -> cache pass -> tasks
    while ready is not empty:
        for (sst, keys) in take(ready):
            found = read_from_cache(sst, keys)     # inline, no I/O
            misses = keys - keys in found
            if misses is not empty:
                spawn read_sst(sst, misses)       # -> Read event
            arrive(sst, keys - misses, found)     # can call schedule

arrive(sst, keys, found):
    for key in keys:
        inflight[key][sst] = found entries of key, or none
        # apply the entries newest SST first. An older SST of the pick
        # waits until every newer SST of the pick arrived.
        for sst in the arrived prefix of inflight[key]:
            for entry in its entries:
                if entry is a value or a tombstone:
                    results[key] = entry  # done, drop the rest of the pick
                    break
                if entry is a merge operand:
                    operands[key].push(entry)   # the key needs a base value
        if key is open and inflight[key] is empty:
            schedule(key)                 # or: no candidates left, done

run():
    for key in open_keys: schedule(key)
    flush()
    for event in tasks, as each one ends:
        Filters(sst, filter):             # kept in loaded
            for key in open keys of sst:
                mark (key, sst) as POSITIVE, or remove sst from candidates[key]
            for key in waiting keys: schedule(key)
        Read(sst, keys, found):
            arrive(sst, keys, found)
        flush()


read_sst(sst, keys):                      # runs as a spawned task
    index  = load_index(sst)              # kept in loaded
    blocks = the block of each key, from the index
    ranges = merge_adjacent(blocks)       # coalesce_gap_bytes
    for range in ranges, all in parallel: # not one after the other
        data = GET range                  # semaphore: max_fetch_tasks
        for key in keys inside range:
            seek the key, collect its entries
```

Why an event loop and not a loop of steps over all keys:

- A batch that reads in lockstep pays the tail latency of the object store
  one time per step. A step of 100 keys waits for the slowest of about 50
  GETs, which is near the p98 of one GET. With bloom filters at 1% false
  positives, almost every batch of 100 keys needs a second step for a few
  keys, and it pays a second tail.
- A loop of concurrent gets pays one tail. The second round trip of the few
  keys with a false positive overlaps with the tail of the others.
- The event loop sends the same requests as the lockstep loop, so goal 3
  holds as before. On a 10M key data set with S3 latency, the lockstep
  version was 25 to 40 percent slower than the loop of gets. The event loop
  is the fix.
- The cached case does not change. A cache hit is handled inline in `flush`,
  with no task, and a batch with all blocks in the cache spawns nothing.

Two limits of the loop:

- Two tasks of one SST can overlap, when its keys become ready at different
  times. Each task reads the index when the meta cache has none. A loop of
  gets reads it per key, so goal 3 still holds.
- The loaded filters and indexes are kept per batch, so a key that reads on
  after another key of the same SST sends no metadata request again.

#### How many SSTs a key reads in one round

In short: in each round, a key reads down its list of candidate SSTs and
stops at the first SST whose filter says "the key is here". It reads further
only when it has to.

The rule is a trade between requests and round trips:

- One SST per round never reads a block for nothing, but each miss costs one
  more round trip.
- All SSTs at once need one round trip, but they download blocks of old
  versions that the newest SST already answers.

`pick_next` reads as few SSTs as it can, with two exceptions:

```text
pick_next(candidates):                    # newest first
    limit = 1 if this is the first round of the key else options.lookahead
    if the key has a merge operand:
        limit = no limit                  # exception 2
    picked = []
    for sst in candidates:
        picked.push(sst)
        if sst is POSITIVE:               # exception 1: UNKNOWN is free
            limit -= 1
        if limit == 0:
            break
    return picked
```

An example for one key K with three candidates. A is a new L0 SST, and B and C
are SSTs of two sorted runs:

```text
candidates of K, newest first:   A          B          C
filter state from the plan:      UNKNOWN    POSITIVE   POSITIVE

round 1 picks A and B, and stops at the first POSITIVE:
    A: load the filter -> negative, A leaves the list
    B: read one block  -> K is there, done

if the filter of A is positive, A is the first POSITIVE:
    A: read one block, and B waits for round 2

round 2 runs only if B was a false positive:
    C: read one block
```

The reasons:

- The first round stops at the first POSITIVE SST. A key with frequent updates has old
  versions in older SSTs, and their filters are right to answer "present". C
  in the example can hold an old version of K. To read it is a waste, because
  B has the newer one.
- Exception 1: an UNKNOWN SST does not count. Its filter is not in the cache,
  so the batch must load it first, and the answer is "negative" 99 times out
  of 100. The common case is a new L0 SST that a `DbReader` has never seen.
  This SST is the newest candidate of each key of the batch. If it counted,
  the first round only loaded one filter, and each new L0 SST cost one more
  round trip. Only the filter load is free. The key loads the filters of its
  UNKNOWN SSTs first, then picks again, and an SST that passes counts as
  POSITIVE. So a cold batch reads no block of a shadowed version. The price is at most one filter load
  per uncached SST above the requests of a `get` loop.
- Exception 2: a merge operand removes the limit. The key needs its base
  value, so it must read each SST down to the base in any case. A counter
  with operands in 10 SSTs takes 2 rounds and not 4.
- A `get` makes the same choices. It loads the filter of each SST above the
  one that answers, and after the first miss it reads 4 sources at a time. So
  the batch sends almost the same requests as a loop.

How many rounds a key takes:

- A key needs a second round only after a false positive or a merge operand.
- With 1000 keys, some key almost always has a false positive. So the
  slowest key of a typical batch takes two rounds, and the second round is
  small. The other keys do not wait for it.
- The number of rounds depends on the depth of the tree and not on the batch
  size.

#### How one SST is read: the cache first, then a task

In short: for each SST that is ready, the batch first answers the keys whose
data is in the block cache. It does this on the task of the caller, with no I/O.
Only the keys that miss go to a spawned task, which reads from the object
store.

```text
(sst, keys) --> read_from_cache --> hits:   entries, right now
                      |
                      +-----------> misses: spawn read_sst --> entries, later
```

`read_from_cache`, the cheap path:

- It needs the index and the block of a key in the block cache. If one of them
  is absent, the key is a miss.
- It never sends an object store request.
- Hot keys are the reason for it. With a Pareto-style skew, most blocks of a
  batch are in the cache. A spawned task for them is pure overhead.

`read_sst`, the I/O path:

- One task per SST does the whole chain for its keys: index, block reads, and
  seeks. The filter is there before the task starts, from the cache or from
  the filter load of the key.
- The tasks run in parallel on the runtime. The decode work of a large batch
  does not pile up on the task of the caller.
- Two keys in one block cause one read. The task reuses
  `read_blocks_using_index` for the block reads.
- The task sends the GETs of all its ranges at the same time. A batch with 40
  scattered blocks in one SST takes one round trip and not 40.
- With `cache_blocks: false`, the task reads from the cache but does not fill
  it, as `get` does.

Limits and cleanup:

- One semaphore per batch bounds the object store requests of all tasks
  (`max_fetch_tasks`).
- The tasks live in a `JoinSet`. If the caller drops the future of the batch,
  the `JoinSet` aborts them.

`loaded`, the memory of the batch:

- It keeps each filter and index that the batch loaded, until the batch ends.
- The "one read per SST" rule then does not depend on the block cache. It
  holds with no cache, and with eviction in the middle of a batch.

### Value resolution

- The entries of a key are collected in newest-first order: write batch,
  memtables, then SSTs.
- The final value comes from the same code that `get` uses: the `max_seq`
  filter, then the merge operator iterator.
- A tombstone gives `None`.

There is no second copy of these rules, so the results cannot drift from
`get`.

### Transactions

`DbTransaction::multi_get` follows `get`:

- It reads the write batch first. Entries of the write batch skip the
  `max_seq` filter.
- It looks up each key in the write batch under the read guard, as `get` does.
  It never clones the write batch, which can be large.
- It records each key of the batch with `track_read_keys`, including the keys
  that return `None`.

### Failure handling

- An error in a filter, index, or block read fails the batch with that error.
- The `JoinSet` aborts the other tasks.
- There are no partial results.
- A retry of the batch is safe. `multi_get` has no side effects except cache
  fills.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

`DbReadOps` gets four new methods and a new `MultiGetOptions` struct. `get`
does not change. There are no new error kinds: a batch fails with the same
errors as a `get`.

### Consistency, Isolation, and Multi-Versioning

- [x] Transactions
- [x] Snapshots
- [x] Sequence numbers

- A batch computes `max_seq` one time and reads all keys from one state view.
- `DbSnapshot::multi_get` reads at the sequence number of the snapshot.
- `DbTransaction::multi_get` reads the write batch first and records each key
  for conflict detection.

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [x] Merge operator
- [ ] Change Data Capture (CDC)

- A key with a merge operand stays open until the batch finds its base value.
  The batch then runs the same merge operator iterator as `get`.
- TTL is not affected. `get` does not filter expired rows at read time, and
  `multi_get` follows it.

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [x] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

- The plan phase reads filters from the block cache and never loads them.
- The read phase fills the cache as `get` does, and it respects
  `cache_blocks`.
- The batch reads each filter and index one time per SST, not one time per
  key. The formats do not change.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

- New metrics count the batches and the keys. The `slatedb.read` span gets
  new fields.
- The language bindings come in a later phase. See [Rollout](#rollout).

## Operations

### Performance & Cost

Measured with `slatedb-bencher` (the `mget` command) on a 16 core machine
with an NVMe disk. Keys of 8 bytes, values of 64 bytes, all keys read at
random, one reader task, batches of 100 keys, `lookahead` 4. The three
readers: `seq` is a loop of `get`, `concurrent` is 100 `get` calls in
flight at once, `multi-get` is one batch. The numbers are the last window
of a 60 or 120 second run.

**Reads from RAM.** Local disk, block cache 2 GiB for 1M rows, 4 GiB for
10M, 16 GiB for 100M. All blocks are cached after the warm-up, so this
measures the CPU cost per key.

| Rows | Shape (L0 / runs / SSTs) | Reader     | p50 per batch | Keys per second | Rounds |
|------|--------------------------|------------|---------------|-----------------|--------|
| 1M   | 2 / 0 / 2                | seq        | 0.87 ms       | 110k            |        |
|      |                          | concurrent | 1.09 ms       | 90k             |        |
|      |                          | multi-get  | 0.36 ms       | 269k            | 1.41   |
| 10M  | 2 / 3 / 14               | seq        | 1.57 ms       | 53k             |        |
|      |                          | concurrent | 1.84 ms       | 54k             |        |
|      |                          | multi-get  | 0.59 ms       | 162k            | 1.89   |
| 100M | 1 / 3 / 44               | seq        | 3.72 ms       | 26k             |        |
|      |                          | concurrent | 1.77 ms       | 54k             |        |
|      |                          | multi-get  | 0.80 ms       | 122k            | 1.86   |

A batch is 2 to 3 times faster than a loop of `get` per key. The gain is
the per key overhead that a batch pays one time: the snapshot, the
memtable walk, the plan, and the task per SST.

**Reads with object store latency.** The 10M dataset with a delay profile
in the object store client, both from measured GET latencies: `s3` (p50
27 ms, p99 113 ms) and `s3x` for S3 Express One Zone (p50 2.5 ms, p99
9.3 ms). Block cache, meta cache, and disk cache at 256 MiB each, about
30 percent of the data.

| Profile | Reader     | p50 per batch | p99 per batch | GETs per batch | KiB per batch | Rounds |
|---------|------------|---------------|---------------|----------------|---------------|--------|
| s3      | seq        | 3104 ms       | 3353 ms       | 90.3           | 5781          |        |
|         | concurrent | 120 ms        | 179 ms        | 57.2           | 3660          |        |
|         | multi-get  | 115 ms        | 183 ms        | 54.6           | 3496          | 1.93   |
| s3x     | seq        | 296 ms        | 330 ms        | 70.3           | 4501          |        |
|         | concurrent | 12.1 ms       | 19.3 ms       | 55.7           | 3566          |        |
|         | multi-get  | 12.2 ms       | 20.4 ms       | 53.9           | 3449          | 1.89   |

With a cold cache, the batch time is the object store tail latency, as for
the concurrent loop. The batch sends 3 to 5 percent fewer GETs and bytes,
because keys in the same block share one read. The bytes per batch are
the same order as for the loop: each key still reads its blocks.

**Requests and cost.** A batch never sends more GETs than a loop of `get`
on the same snapshot (goal 3), plus at most one filter load per SST with
no cached filter. A batch sends no LIST and no PUT. Almost every batch of
100 keys needs a second round for a few keys, because a bloom filter with
10 bits per key gives about 1 percent false positives. The event loop
overlaps that round with the first, so a batch pays about one tail
latency.

**Amplification.** No change to space or write amplification: a batch
writes nothing and does not change the SST format or compaction. Read
amplification per key is the same as for `get`, or lower when keys share
a block. Merged block ranges (`max_coalesced_bytes`) trade some extra
bytes for fewer GETs.

**Known cost.** In the all-cached case, the event loop is 3 to 5 percent
slower per batch than a loop that reads all keys in lockstep, because it
schedules each key on its own. This is the price of the one tail latency
above.

### Observability

In short: three new counters, two new fields on the read span, and no new
configuration outside `MultiGetOptions`.

Metrics. The model is the write path, which counts batches
(`write_batch_count`) and operations (`write_ops`) apart:

| Metric                                      | Grows by                      |
|---------------------------------------------|-------------------------------|
| `slatedb.db.request_count{op="multi_get"}`  | 1 per call                    |
| `slatedb.db.multi_get_keys`                 | Number of input keys per call |
| `slatedb.db.multi_get_rounds`               | Rounds of the slowest key     |

- `request_count{op="get"}` does not change. A batch is not N gets, so
  dashboards for point reads do not jump.
- `multi_get_keys / request_count` gives the mean batch size.
- `multi_get_rounds / request_count` gives the mean number of rounds of the
  slowest key of a batch. A value well above 2 is a sign of cold filters, or
  of a `lookahead` that is too low.
- The filter counters (`sst_filter_positive_count` and the others, with
  `kind="point"`) count one probe per (key, SST) pair, as in `get`.

Tracing. A batch with `tracing_options` opens one `slatedb.read` span, as `get`
does:

- The span gets two new fields: `keys` and `rounds`.
- `slatedb.read.read_filters` and `slatedb.read.read_index` stay one span per
  SST.
- `slatedb.read.evaluate_filter` becomes one span per SST with a `keys` field.
  A `get` opens one such span per key and SST. A batch of 1000 keys over 20
  SSTs then opened 20,000 spans.

The rest:

- Configuration: only the new `MultiGetOptions` struct. `Settings` does not
  change.
- There are no new components and no new log lines.

### Compatibility

- Data on object storage does not change, and mixed versions are safe.
- `get`, `scan`, and the language bindings do not change.
- `DbReadOps` gets two methods with no default body. This breaks each type
  outside SlateDB that implements the trait. Code that only calls the trait is
  not affected.

A GitHub code search finds one such project,
[HelixDB](https://github.com/HelixDB/helix-db), with one production type and
four test doubles. The fix is a few lines per type: a wrapper forwards to the
`multi_get` of its inner type, and a test double calls its own `get` in a
loop.

A default body can avoid the break. See [Open Questions](#open-questions).

## Testing

`get` is the oracle. A batch is correct when each slot holds what a `get`
returns for that key on the same snapshot. The tests reuse the tools that
SlateDB already has.

- Unit tests: the plan phase, `pick_next`, and `read_sst`, as `rstest` tables
  next to the code.
- Integration tests: one differential test that compares a batch with a `get`
  loop. The fixture follows `tests/scan_model.rs` and forces a compaction, so
  the keys spread over L0 and sorted runs.
- Request count tests: a counting object store proves goal 3. The batch must
  send no more GETs than the loop, with a warm and with a cold cache.
- Fault-injection tests: a failed block read fails the batch, and a dropped
  future leaves no running tasks.
- Deterministic simulation tests: the `slatedb-dst` workload gets a `MultiGet`
  operation that checks each slot, as `verify_get` does.
- Formal methods verification: none.
- Performance tests: `benches/db_operations.rs` compares a batch with a `get`
  loop.

## Rollout

- Milestones / phases:
  - Initial PR with core implementation.
  - Separate PR for each language binding update.
- Feature flags / opt-in: none.
- Docs updates:
  - Separate PR with doc updates.

## Alternatives

The alternatives fall into two groups: what the caller sees, and how the batch
reads.

### What the caller sees

#### Keep `get` in a loop (status quo)

- For: no new API. `join_all` over gets hides most of the latency.
- Against: it removes no work. See the table in Motivation. The batch also has
  no single state view unless the caller opens a snapshot first.

#### A prepared reader: plan one time, fetch many times

LightGBM does this for `predict`: a `FastInit` call allocates the buffers and
parses the configuration, and each later call only does the work. Here, a
`db.prepare_multi_get(options)` call returns a handle, and each
`handle.get(keys)` reuses it.

- For: the buffers, the maps, and the loaded filters and indexes live across
  batches. A service that sends the same batch shape each time allocates
  nothing per request.
- Against: the plan depends on the keys, and the keys differ per batch. So
  the handle can reuse memory, but not the plan.
- Against: a handle that keeps a state view pins old SSTs and returns stale
  data. A handle without a view saves only allocations, and the plan phase is
  microseconds next to milliseconds of I/O.
- Against: the block cache already keeps filters and indexes across batches.

A buffer pool inside SlateDB gives the same saving with no new API. It can
come later, if a profile shows that allocations matter.

#### Merge concurrent gets behind the `get` API

A layer under `get` collects the calls of a short time window and runs them as
one batch. `join_all` over gets then gets the batch path for free.

- For: no new API, and old code gets faster.
- Against: each `get` waits for the window, so a single read gets slower.
- Against: the calls come from different callers with different options, and
  they cannot share one state view.
- Against: the behavior is hidden. A latency change in `get` is hard to explain
  to a user who did not ask for batches.

#### Sorted seeks on one scan iterator

This works today: open one `scan` over the key range and call `seek` for each
key in sorted order.

- For: one state view and one set of iterators for the whole batch.
- Against: a scan cannot use point filters, so it reads a block from each
  layer for each key.
- Against: the seeks run one after the other. The latency is a sum again.

### How the batch reads

#### Layer walk, as in the classic RocksDB `MultiGet`

Read all L0 SSTs, then each sorted run in order, with a key set that shrinks.

- For: simple, and it never reads a block for nothing.
- Against: one round trip per layer. This is about 100 µs on a local disk and
  tens of milliseconds on S3.

#### Full fan-out

Read each filter-positive SST of each key in one round trip. This was the
first version of this RFC.

- For: one round trip.
- Against: a key with frequent updates has old versions in older SSTs. The
  fan-out downloads one block per version. `lookahead: usize::MAX` still
  gives this behavior from the second round of a key on.

#### Read in lockstep

All open keys pick, all picked SSTs are read, and the batch waits for every
read before the next pick. This was the second version of this RFC.

- For: a simpler loop, and the same requests as the event loop.
- Against: each step pays the tail latency of the object store. A batch of
  100 keys almost always needs two steps, so it pays two tails. On S3 the
  batch was 25 to 40 percent slower than a loop of concurrent gets, with the
  same requests.

#### Load all filters in the plan phase

- For: no UNKNOWN state, and a simpler `pick_next`.
- Against: with a cold cache, it sends more requests than a loop and breaks
  goal 3. RocksDB also probes each filter only when the walk reaches the file.

### Smaller choices

- More fields in `ReadOptions` and no `MultiGetOptions`: `get` ignores each of
  them. `ScanOptions` is the precedent for a separate struct.
- An error per key, as in RocksDB: most errors in SlateDB hit a whole block or
  a whole SST. The public `Error` is not `Clone`, so one failed block cannot
  give its error to each of its keys without an API change.
- A limit on the batch size: SlateDB sets no such limit on `WriteBatch` or on
  scans. The caller owns the size.

## Open Questions

- Do the new `DbReadOps` methods get a default body that calls `get` in a loop for each
  key? The draft has none.
  - For: no breaking change for external implementors.
  - Against: a wrapper around a `Db` reads each key from a different state
    view, and no compiler error tells the author.
  - Against: the default body cannot pin one view. `ReadOptions` has no public
    `max_seq`, and a bare `max_seq` does not protect old versions from
    compaction.

- Are the defaults of `MultiGetOptions` right? They are authors estimates, and no
  benchmark on S3 backs them yet.
  - `max_fetch_tasks: 256`. S3 accepts about 5500 GETs per second per prefix,
    and SlateDB keeps all SSTs under one prefix.
  - `coalesce_gap_bytes: 64 KiB`. The `object_store` crate merges ranges with
    a gap of 1 MiB, which looks too large for 4 KiB blocks.
  - `lookahead: 4` copies `get`.

- How much memory can `loaded` take? It keeps each index until the batch ends.
  - The index of a 1 GiB SST can be several MiB, and a batch over a large
    bottom run touches about 100 SSTs.
  - Option: drop an index after the last read that needs it, or keep it only
    in the block cache when there is one.

- Do the gap blocks of a merged read go into the block cache?
  - For: it is a free prefetch for keys that cluster.
  - Against: blocks that no key asked for can push hot blocks out.

- Does `get` become a `multi_get` of one key later? It is a non-goal here, but
  two read paths cost more to maintain than one. The answer depends on a
  benchmark of a batch of one key against `get`.

- Are the two observability choices fine?
  - The `multi_get_rounds` counter is new in kind. No other read metric
    describes the inner work of a call.
  - `slatedb.read.evaluate_filter` is one span per SST in a batch, and one
    span per key and SST in `get`.

## References

- an older draft slop-grenade PR (by the same author): https://github.com/slatedb/slatedb/pull/1810
- Issue: https://github.com/slatedb/slatedb/issues/301

## Updates

* v1: (xx.07.2026) initial draft
* v2: (21.09.2026) major update
* v3: (22.09.2026) the read phase is an event loop, not a loop of steps
* v3.1: (22.09.2026) measured numbers in Performance & Cost
