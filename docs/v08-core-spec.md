# rti-db v0.8 — Core Scalability SPEC

*Status: proposed · Target: v0.8.0 · Supersedes: none · Builds on: v0.7.0 (`docs/v07-ai-integration-spec.md`)*

This is the single source of truth for the v0.8 core wave selected for implementation:
segment compaction, multi-shard Raft, and a public O(1) `latest` read. The wave makes
the core scale in space (compaction), scale in replication domains (multi-shard Raft),
and serve S1-style consumers without scanning history (O(1) latest), while preserving
v0.7's AI integration contracts and v0.6's real-time performance envelope.

## 0. Invariants (binding on every v0.8 change)

1. **The reflex path never regresses.** `put()` / `put_durable()` hot-path code remains
   free of new heap allocations and blocking I/O. v0.7 benchmark parity is a release
   validation gate: group-commit p50/p999, batch ingest, scan throughput, and on-disk
   bytes must remain within 5% of the archived v0.7 result unless a metric is
   explicitly improved by the new feature.
2. **Backward compatibility.** The `RTISEG01` segment format, WAL frame format,
   `archive.catalog` format, single-shard Raft APIs and message tags, and the frozen
   `rti-vla` C ABI all remain readable and behavior-compatible. Existing v0.7 data
   directories must open under v0.8 without migration.
3. **Crash safety.** Every on-disk replacement uses the existing write-temp,
   atomic-rename, policy-driven fsync discipline. A crash at any point in compaction
   must leave either the old segment set, the new segment set, or both; the scan
   layer's timestamp dedup is the final correctness net and must not be removed.
4. **Layering stays clean.** `rti-db` does not depend on Raft; Raft grouping stays in
   `rti-raft` / `rti-edge`. Compaction and latest are core database capabilities, not
   AI-specific features. No new `unsafe` is introduced.
5. **Deterministic mode remains deterministic.** With `Profile::Deterministic`, v0.8
   features must not touch the filesystem, must not introduce background threads, and
   must keep the existing zero-allocation hot-path proof green.
6. **Existing scan duplicate-timestamp semantics are preserved.** The current scan
   path is stable-sort plus first-writer-wins dedup: memtable entries outrank
   segments, and older segment catalog entries outrank newer ones at the same
   timestamp. v0.8 must not silently change scan behavior. The new `latest()` API has
   a separate equal-timestamp rule documented in §3 because the existing cross-layer
   scan result is not itself stable across a seal boundary.

## 1. Segment compaction

### Goal

Bound the number and total size of immutable segments under overwrite-heavy or
long-lived workloads, reduce open/recovery memory pressure, and improve scan locality
without changing the segment file format.

### Public API

```rust
impl Db {
    /// Compact eligible local segments. Returns the number of input segments removed.
    /// Deterministic profile: Ok(0), no filesystem access.
    pub fn compact(&self) -> Result<usize>;

    /// Compact eligible local segments for one series.
    pub fn compact_series(&self, series: SeriesId) -> Result<usize>;

    /// Cumulative compaction counters for observability. Process-local: the counters
    /// accumulate since `Db::open` and reset on restart (nothing is persisted).
    pub fn compaction_stats(&self) -> CompactionStats;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub runs: u64,
    pub input_segments: u64,
    pub output_segments: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
}
```

### Internal contract

- Add `crates/rti-db/src/compact.rs` with a merge executor and a small deterministic
  policy object. The executor opens the selected segment files by name, consumes their
  `SegmentReader` iterators, performs a k-way merge by timestamp, and emits one
  same-series `RTISEG01` segment through the existing `SegmentWriter` path.
- Duplicate-timestamp rule: process input segments in existing catalog order and emit samples
  ordered by `(timestamp, input catalog index)`. The output segment **preserves duplicates in
  scan order** — it does not deduplicate identical timestamps. Scan collects entries in catalog
  order, applies predicates at the decode layer, then stable-sorts by timestamp and dedups, so
  a plain scan still yields the first writer and a predicated scan still yields the first
  *matching* writer; both are sample-identical before and after a merge under arbitrary
  predicates. This is the current scan behavior and is not a last-writer-wins change.
- Only **local, never-archived** segments participate in v0.8. Any segment whose name
  appears in `archive.catalog` is skipped. This avoids append-only catalog rows
  resurrecting a cold-tier copy after compaction deletes a local superseded file.
- Prefer time-adjacent, same-series segments. The v0.8 policy merges only each series'
  **complete eligible tail run** (walk the series' catalog subsequence backwards from its last
  entry, collecting local, never-cataloged segments, stopping at the first ineligible entry;
  a tail shorter than `min_segments = 2` is not merged). Tail-only merging is a correctness
  requirement, not just locality: the swap installs the output at the run's first-input
  position, while reopen orders segments by file name, and the output's fresh maximal sequence
  number sorts it last. Only when no same-series entry follows the merged run do the live
  catalog order and the reopen name order agree, keeping duplicate-timestamp winners (plain
  and predicated) stable across a restart. Behavior must be deterministic.
- Compaction must not run inside the ingest batch loop. `Db::compact*()` is an
  explicit operations API. Future background scheduling is out of scope.
- Before swapping the in-memory segment list, re-check that every selected input is
  still present and unchanged **and that the run is still the series' tail** (no same-series
  entry after the last input). A concurrent seal must not be compacted away; a compaction
  run that races a concurrent same-series append aborts safely — no swap, the uninstalled
  output file is rolled back, no stats move — and the caller may retry later. This is a
  deliberately conservative correctness policy.

### Required pre-fix

`Db::open` currently derives the next segment sequence from the number of files. This
is unsafe once compaction deletes files. Before compaction can land, segment sequence
allocation must parse both:

1. local `seg-NNNNNN-sSSSSSS.seg` names, and
2. segment names recorded in `archive.catalog`,

then use `max(NNNNNN)+1`. This prevents collisions with cold-tier object keys as well
as local files.

### Crash and concurrency semantics

1. Write the merged output to `*.seg.tmp`.
2. Rename it into place and honor `SyncPolicy` directory/fsync behavior.
3. Swap the in-memory `SegEntry` set in one short critical section.
4. Best-effort delete superseded input files after the swap, **newest first** (reverse
   catalog order), with a directory fsync after each delete per `SyncPolicy`
   (`Group`/`Always` fsync, giving a durable newest-first delete order; `None` skips it —
   the same durability tier as `SegmentWriter::write_unsynced`, i.e. process-crash safe via
   the page cache but with no deletion-order promise after machine power loss).

A crash before step 3 leaves old data only. A crash after step 3 but before deletes
leaves duplicate old/new files; reopen must tolerate this and scan dedup must preserve
correctness. A crash during step 4 can only have durably removed a suffix of the newest
inputs: the remaining oldest inputs still precede the output segment in file-name order,
and the output preserves the inputs' `(timestamp, input catalog index)` order, so
duplicate-timestamp winners (plain and predicated) are identical to the pre-compaction
scan. Deleting oldest first is forbidden: a crash could leave only a newer input ahead of
the output, promoting its duplicate-losing samples to winners on reopen. Compaction and
`scan` may run concurrently; readers must never observe a partially installed output
segment.

## 2. Multi-shard Raft

### Goal

Allow independent Raft groups to replicate disjoint series shards while preserving the
existing single-group protocol state machine byte-for-byte.

### Public API

Use an endpoint-adapter design: each group gets an endpoint that implements the
existing `Transport` trait, while a multi-group network keys delivery by
`(GroupId, NodeId)`. `Node` remains group-unaware.

```rust
pub type GroupId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMsg {
    pub group: GroupId,
    pub from: NodeId,
    pub inner: Msg,
}

/// Deterministic multi-group memory network. Queues are keyed by (group, node).
pub struct MultiNetwork { /* opaque */ }

/// Per-group endpoint implementing the existing Transport trait.
pub struct GroupEndpoint { /* opaque */ }

/// Local router owning this physical node's protocol Node for each group.
pub struct Router { /* opaque */ }

impl Router {
    pub fn new(net: MultiNetwork) -> Self;
    pub fn add_group(
        &mut self,
        group: GroupId,
        id: NodeId,
        peers: Vec<NodeId>,
        election_min: u64,
        election_span: u64,
        heartbeat: u64,
        base_seed: u64,
    ) -> Result<()>;
    pub fn tick(&mut self, now: u64);
    pub fn pump(&mut self) -> usize; // returns messages handled
    pub fn node(&self, group: GroupId) -> Option<&Node<GroupEndpoint>>;
    pub fn node_mut(&mut self, group: GroupId) -> Option<&mut Node<GroupEndpoint>>;
    pub fn propose(&mut self, group: GroupId, records: Vec<Record>) -> Result<u64>;
    pub fn take_durable(&mut self, group: GroupId) -> Result<Vec<Record>>;
}

/// Derive a deterministic per-group seed before constructing/adding a group.
pub fn group_seed(base_seed: u64, group: GroupId) -> u64;
```

A routing helper maps `SeriesId -> GroupId` using an injected pure function. Routing a
record to an unknown group returns `Error::NotFound`. The default single-shard path
remains equivalent to group `0`.

### Compatibility contract

- Do not change `Node::new`, `Msg`, `Entry`, `Snapshot`, `ConfChange`, `CONF_SERIES`,
  codec tags 0–6, or the commit/PreVote semantics in `node.rs`.
- Existing `MemoryNetwork`, `MemoryTransport`, and `TcpTransport` remain valid for
  single-group use and their tests must pass unchanged.
- `MultiNetwork` must prevent equal `NodeId`s in different groups from
  cross-delivering. Per-group partition/heal controls are required for deterministic
  tests.
- v0.8 ships multi-group Raft over the deterministic memory network used by tests and
  `rti-edge` simulation. Versioned TCP group envelopes are deferred to a later wave;
  existing TCP behavior must not change.
- `Router::add_group` derives the node seed through `group_seed(base_seed, group)` so
  groups do not campaign in lockstep.
- Proposing to a non-leader keeps the existing retryable error behavior in v0.8; a
  structured `NotLeader` variant may be introduced only if call sites and tests are
  updated in the same wave.
- Shard split/merge, cross-shard transactions, and Raft log persistence are explicit
  non-goals for v0.8.

## 3. Public O(1) latest

### Goal

Expose the newest visible sample for a series without flushing, scanning, decoding
segments, sorting, or allocating on the read path, then switch `rti-vla::latest()` to
that API without changing its signature or C ABI.

### Public API

```rust
impl Db {
    /// Newest visible sample for `series`, or `None` if the series is absent.
    /// O(1), no filesystem access, no allocation on the read path.
    pub fn latest(&self, series: SeriesId) -> Result<Option<Sample>>;
}

/// Optional free-function convenience, matching the existing facade style.
pub fn latest(db: &Db, series: SeriesId) -> Result<Option<Sample>>;
```

### Semantics

- “Latest” means a sample with the maximum timestamp. The value at an equal maximum
  timestamp is **unspecified** across layer boundaries: `latest()` keeps the first
  known sample at the current maximum timestamp and updates only on a strictly larger
  timestamp. This is deliberate. The existing scan rule can change which duplicate is
  visible when a memtable seals, so no O(1) per-series head can match scan's value in
  that pathological case without an expensive duplicate-index. Unique-timestamp
  workloads — the real-time contract — must match `scan(...).last()` exactly.
- Duplicate timestamps within the same storage layer keep scan-compatible
  first-writer-wins behavior. Cross-layer duplicate values may differ between
  `latest()` and `scan()`, and tests must pin timestamp/visibility rather than value
  equality for that case.
- Visibility is tied to the ingest pipeline, not merely to enqueue: after
  `put_durable()` returns, the sample is guaranteed to be visible to `latest()`. A
  bare `put()` becomes visible after the ingest thread applies it; `latest()` must not
  block waiting for the queue or WAL sync.
- The latest index survives memtable sealing and reopen/recovery for persistent
  profiles. Rebuilding it at open may decode each pre-read segment forward once: this
  is O(total persisted samples) at open, but no extra file I/O beyond the existing
  segment preload, and steady-state reads remain O(1). Archived segments recorded only
  in `archive.catalog` are not pre-read at open, so a series whose samples are
  exclusively in the cold tier may return `None` from `latest()` after a reopen even
  though `scan` reads them back transparently; this limitation must be disclosed in
  the `Db::latest` and `rti-vla` rustdoc.
- In `Profile::Deterministic`, if LRU eviction removes an entire series, `latest()`
  follows current scan visibility and returns `None` for that series. Add a reporting
  LRU API in `rti-store` rather than changing the existing method signature:

```rust
impl MemTable {
    /// Returns the evicted series, if any.
    pub fn insert_lru_report(&mut self, series: SeriesId, sample: Sample)
        -> Result<Option<SeriesId>>;

    /// Existing compatibility wrapper: true if an eviction happened.
    pub fn insert_lru(&mut self, series: SeriesId, sample: Sample) -> Result<bool>;
}
```

- The index is per-series and bounded by the number of visible series. It must not
  require reverse decoding of the compressed segment format.

### `rti-vla` integration

`rti_vla::latest(db, series)` keeps its signature and delegates to `Db::latest`. The
thread-local scan buffer and the “≤256-point envelope” limitation are removed. The C
ABI functions and generated header remain byte-identical; `scripts/check-c-abi.sh`
must pass without header changes.

## 4. Acceptance tests

### CI gates

| Area | Gate |
|---|---|
| Compaction correctness | Merge N overlapping same-series segments; scan before/after is sample-identical, including predicates (also discriminating predicates where only the duplicate loser matches), aggregations, and duplicate timestamps (outputs preserve duplicates in scan order) |
| Compaction crash safety | Crash windows: before rename, after rename before swap, after swap before delete, and mid-delete (newest-first durable deletes); reopen preserves logical scan correctness and duplicate-timestamp winners |
| Segment sequence | Reopen after compaction never reuses an existing local or cataloged segment sequence number |
| Cold tier | Cataloged inputs are skipped by compaction; archive catalog remains parseable; old rows cannot resurrect superseded data |
| Deterministic compaction | `Db::compact()` is `Ok(0)`, touches no filesystem, and does not alter hot-path allocation behavior |
| Multi-shard election | Multiple groups elect independently; one partitioned/crashed group does not block another group |
| Multi-shard replication | Writes proposed to group A never appear in group B; durable watermarks advance independently |
| Snapshot/membership | Snapshot install and one-step membership changes work per group; another group's membership remains unchanged |
| Transport isolation | Same `NodeId` in two groups cannot cross-deliver; per-group partition/heal behaves independently |
| Single-shard regression | All existing `rti-raft` tests pass unchanged |
| O(1) latest correctness | Matches `scan(...).last()` for unique timestamps under ordered, seal, reopen, durable-write, and deterministic-LRU cases; duplicate-ts tests pin maximum timestamp and visibility while treating cross-layer value choice as unspecified |
| O(1) latest allocation | 100k-point series: 10k `latest()` calls complete with zero heap-allocation growth |
| C ABI | `scripts/check-c-abi.sh` passes with no header diff |
| Full workspace | `cargo test --workspace`, `cargo test --workspace --all-features`, both clippy configurations, `check-no-std.sh`, and `scripts/check-neutrality.sh` all pass |

### Release validation gates (reported in `bench/results-v08/`)

| Area | Gate |
|---|---|
| O(1) latest latency | 100k-point series: 10k-call smoke reports p99 < 1 µs on the validation host |
| Benchmark parity | Compare v0.8 against archived v0.7 results; every v0.7 headline metric within 5% unless explicitly improved |
| Compaction benefit | A repeated-overwrite workload demonstrates bounded segment count and non-increasing on-disk bytes after compaction |

`check-neutrality.sh` must run `cargo tree --workspace -e normal` and reject the
forbidden model/inference/embedding dependency list used by the v0.7 audit. The
script is added in this wave and wired into CI.

## 5. Implementation streams

| Stream | Owner scope | Branch | May modify |
|---|---|---|---|
| Prep — shared core contracts | seg-seq pre-fix, `latest.rs` boundary, memtable LRU reporting | `v08-prep` | `rti-db/src/lib.rs`, `rti-db/src/latest.rs`, `rti-store/src/memtable.rs` |
| A — compaction | segment merging and directory lifecycle | `v08-compaction` | `rti-db/src/compact.rs`, compaction tests, limited `rti-db` integration points |
| B — multi-shard Raft | group endpoint, router, multi-group memory network, tests | `v08-multishard-raft` | `rti-raft` only |
| C — O(1) latest | latest index, facade API, rti-vla switch | `v08-o1-latest` | `rti-db/src/latest.rs`, `rti-vla`, latest tests |

The main agent lands or explicitly defines the Prep contracts before Streams A and C
modify `rti-db`. Cross-stream files are coordinated by the main agent. If two streams
need the same `rti-db` section, the main agent rebases the affected stream before
merge.

## 6. Non-goals (v0.8)

No shard split/merge, no cross-shard transactions, no persistent Raft log, no
multi-group TCP envelope, no background compaction scheduler, no cold-tier
compaction, no segment reverse index, no C ABI additions, and no change to the AI
neutrality boundary.

## 7. Deliverables

`docs/v08-core-spec.md`, compaction implementation and tests, multi-shard Raft
implementation and deterministic tests, public O(1) latest plus rti-vla integration,
`scripts/check-neutrality.sh`, updated README/roadmap notes, and
`bench/results-v08/` parity artifacts. A v0.8.0 release is prepared only after
explicit user approval following the validation report.
