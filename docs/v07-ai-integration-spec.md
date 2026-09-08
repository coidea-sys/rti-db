# rti-db v0.7 — AI Integration SPEC

*Status: proposed · Target: v0.7.0 · Supersedes: none · Builds on: v0.6.0 (docs/ai-strategy.md)*

This is the single source of truth for the v0.7 AI-integration wave. It exists to make
the strategic position in `docs/ai-strategy.md` executable: rti-db serves AI at inference
time (working memory), training time (flight recorder), and governance time (replay) —
without ever entering the model layer.

## 0. Invariants (binding on every v0.7 change)

1. **The reflex path never regresses.** All v0.6 benchmark gates remain CI-enforced:
   single-point `put()` p50 ≤ 0.5 µs, batch ingest ≥ 95% of the v0.6 baseline
   (9.04M pts/s), zero heap allocation on the steady-state hot path (the global
   allocation-counter test must pass unchanged).
2. **Neutrality.** No model code, no inference runtime, no embedding store. Integrations
   are data-plane adapters only.
3. **Integrations live off the hot path.** New crates depend on `rti-db`'s public API
   (`put` / `put_durable` / `scan` / iterators). No integration may touch `rti-mem`,
   `rti-buffer`, or WAL internals, and no new `unsafe` is introduced anywhere.
4. **Optional everything.** Each integration is its own crate and its own Cargo feature;
   `cargo build -p rti-db` on a bare target (including `no_std` smoke) is unaffected.

## 1. Crate map (3 new crates, 0 modified core crates)

| Crate | Role | Feature | Runs on |
|---|---|---|---|
| `rti-ros2` | ROS 2 topic bridge: subscribe sensor topics → ingest; replay history → publish | `ros2` | Linux x86_64/aarch64, std only |
| `rti-export` | Episode exporter: `[start,end)` windows → LeRobot-compatible Parquet | `export` | std |
| `rti-vla` | Working-memory adapter for VLA runtimes: latest-state reads + episodic windows + C ABI | `vla` | std (core logic embeddable) |

`rti-edge` gains one preset (`EdgeConfig::ai_working_memory()`) composing the above for
the cognition domain. No changes to `rti-core`/`rti-mem`/`rti-buffer`/`rti-wal*`.

## 2. `rti-ros2` — ROS 2 bridge

**Why.** ROS 2 is the embodied-AI ecosystem's bus. The bridge turns rti-db into the
flight recorder of any ROS 2 robot with zero application code changes.

**Design.** A standalone process (`rti-ros2d`), never a library linked into control
nodes. It subscribes via `rclrs` (DDS underneath), maps topics to `SeriesId` through a
registry file, and calls `Db::put` (default) or `Db::put_durable` (marked topics).

```rust
/// Registry entry: one ROS 2 topic → one rti-db series.
/// Loaded from `rti-ros2.toml` at startup; unknown topics are logged and skipped.
pub struct TopicBinding {
    pub topic: String,          // e.g. "/joint_states/effort[3]"
    pub series: SeriesId,       // stable u32; assigned once, persisted
    pub durable: bool,          // true → put_durable path (flight-recorder topics)
    pub field: FieldSelector,   // scalar field extraction from the message
}

/// Long-running bridge. Owns a `Db` handle opened by the caller.
pub struct Ros2Bridge { /* opaque */ }
impl Ros2Bridge {
    pub fn open(db: Arc<Db>, bindings: Vec<TopicBinding>, node_cfg: NodeConfig) -> Result<Self>;
    /// Publish a historical window back onto a topic (for replay/sim).
    pub fn replay(&self, series: SeriesId, start: Timestamp, end: Timestamp,
                  topic: &str, speed: f64) -> Result<ReplayHandle>;
    pub fn stats(&self) -> BridgeStats;   // msgs_in, points_written, dropped, lag_p99
}
```

**Contracts.** Topic→series mapping is explicit (no hashing surprises); message fields
that are non-scalar require an explicit `FieldSelector`; backpressure policy is
drop-oldest on the ROS side with a counter — the bridge must never block DDS.
Replay publishes at `speed ×` original timing using stored nanosecond timestamps.

## 3. `rti-export` — LeRobot episode exporter

**Why.** Training-time role: episodes recorded by the flight recorder must land in the
format the ecosystem trains on. LeRobot (Hugging Face) is the de-facto open standard.

**Design.** Library + CLI (`rti-export`). An *episode* is a `[start, end)` window over a
named set of series; the exporter streams `Db::scan` iterators into a Parquet writer
(arrow2), so export memory is O(chunk), not O(episode).

```rust
pub struct EpisodeSpec {
    pub name: String,
    pub series: Vec<(String, SeriesId)>, // column name → series
    pub start: Timestamp,
    pub end: Timestamp,
    pub frame_hz: Option<u32>,           // resample: align all series to a common grid
}

pub trait EpisodeSink { fn write_chunk(&mut self, cols: &Chunk) -> Result<()>; fn close(self) -> Result<PathBuf>; }

pub fn export_episode(db: &Db, spec: &EpisodeSpec, sink: impl EpisodeSink) -> Result<ExportMeta>;
```

**Contracts.** Output layout follows LeRobot v2 dataset conventions
(`data/chunk-000/episode_000000.parquet` + `meta/`); timestamps remain nanoseconds in a
`timestamp` column; `frame_hz` resampling uses last-value-carry-forward and records the
original-sample count per frame in `meta/stats.json`. CLI: `rti-export --db PATH
--spec episodes.toml --out dataset/`.

## 4. `rti-vla` — working-memory adapter

**Why.** Inference-time role: give VLA runtimes (Helix-class S1/S2, pi0-class) a
stable, documented API instead of ad-hoc drivers. Two speeds, matching dual-system
cognition.

```rust
/// S1 reflex path: latest value for a series. Lock-free read of the ring head;
/// budget: < 1 µs, no allocation, callable from a real-time thread.
pub fn latest(db: &Db, series: SeriesId) -> Result<Option<Sample>>;

/// S2 episodic context: ordered window over several series, zero-copy iterator.
/// Intended for attention-style consumption of the recent past.
pub fn window(db: &Db, series: &[SeriesId], span: TimeSpan) -> Result<WindowIter>;

/// Action-chunking aware buffering: hold `k` future action slots aligned to the
/// model's control period, flushed as one batch per chunk boundary.
pub struct ChunkBuffer { /* opaque */ }
impl ChunkBuffer {
    pub fn new(db: Arc<Db>, series: SeriesId, chunk_len: u32, period: Duration) -> Self;
    pub fn push(&mut self, s: Sample) -> Result<()>;          // never blocks
    pub fn seal_chunk(&mut self) -> Result<u64>;              // returns durable watermark
}
```

**C ABI (stable subset).** `rti_latest`, `rti_window_open/next/close`,
`rti_chunk_push/seal` — `#[repr(C)]` over opaque handles, semver-frozen from v0.7.0.
Python/PyO3 bindings are explicitly deferred (v0.8 candidate) so the C ABI is the
single integration contract for VLA runtimes in any language.

## 5. Acceptance tests (CI-enforced)

| Gate | Requirement |
|---|---|
| Perf | Full v0.6 benchmark harness rerun: all headline metrics within 5% of v0.6.0 baseline; report committed to `bench/results-v07/` |
| Ros2Bridge | Simulated 100-topic / 50 kHz aggregate feed for 60 s: 0 lost `durable` points, `lag_p99` < 5 ms |
| Export | Round-trip test: export 1M-point episode → parquet → re-read → sample-identical (ts, value) after resample rules |
| VLA | `latest` called from a dedicated thread at 1 kHz for 10 min under full ingest load: 0 allocations (counter), p99 < 1 µs |
| C ABI | `cbindgen` header diff-checked in CI; ABI break = CI failure |
| Neutrality | `cargo tree` check: no ML/inference dependency enters the workspace |

## 6. Non-goals (v0.7)

No Python bindings (deferred), no GPU/TensorRT anything, no embedding/vector index,
no ROS 1 support, no changes to Raft multi-shard (parallel track, separate SPEC),
no cloud-managed service.

## 7. Deliverables

Three crates + one preset + CLI + docs updates (README integrations table, one
end-to-end example: ROS 2 bag → rti-db → LeRobot dataset → replay). Version bump to
0.7.0 across the workspace; GitHub Release notes referencing this SPEC.
