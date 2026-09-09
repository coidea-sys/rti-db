//! rti-vla: working-memory adapter for VLA runtimes (v0.7, SPEC §4).
//!
//! Gives VLA runtimes (Helix-class S1/S2, pi0-class) a stable, documented API over
//! rti-db instead of ad-hoc drivers. Two speeds, matching dual-system cognition:
//!
//! | path | API | budget |
//! |---|---|---|
//! | S1 reflex | [`latest`] | steady-state **zero heap allocation** (verified by the `alloc-count` test), callable from a real-time thread |
//! | S2 episodic context | [`window`] / [`WindowIter`] | ordered k-way merge over several series; allocates only at open |
//! | action chunking | [`ChunkBuffer`] | `push` never blocks and never allocates; `seal_chunk` flushes one batch per chunk boundary |
//!
//! The C ABI ([`ffi`]) is the single integration contract for VLA runtimes in any
//! language, semver-frozen from v0.7.0 (`rti_latest`, `rti_window_open/next/close`,
//! `rti_chunk_push/seal` plus handle lifecycle functions).
//!
//! ## Honest complexity disclosure
//!
//! rti-db's public API has no O(1) ring-head read, so [`latest`] is implemented as a
//! full-range [`rti_query::ScanSource::collect`] over the series followed by taking the
//! last point: **O(n) in the number of samples of that series** (memtable + segment
//! merge, mutex-protected inside rti-db — the "lock-free ring head" wording of SPEC §4
//! describes the target, not what rti-db exposes today). Steady-state calls perform zero
//! heap allocations thanks to a thread-local reusable result buffer. If rti-db later
//! grows a public head-read, [`latest`] drops to O(1) without an API change.

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use rti_core::{Error, Result, Sample, SeriesId, Timestamp};
use rti_db::Db;
use rti_query::ScanSource;

pub mod ffi;

/// Half-open time span `[start, end)` (nanosecond timestamps).
///
/// Note the asymmetry with `Db::scan`, whose bounds are inclusive on both ends:
/// [`window`] filters the `ts == end` point out so the `[start, end)` contract holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeSpan {
    /// Window start (inclusive).
    pub start: Timestamp,
    /// Window end (exclusive).
    pub end: Timestamp,
}

impl TimeSpan {
    /// Construct a half-open span `[start, end)`.
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }
}

// --------------------------------------------------------------- S1: latest

thread_local! {
    /// Reusable result buffer for [`latest`] (one per thread; allocated on first use,
    /// steady-state calls only `clear()` it, so the S1 hot path performs 0 mallocs).
    static LATEST_BUF: RefCell<Vec<Sample>> = const { RefCell::new(Vec::new()) };
}

/// S1 reflex path: latest value for a series.
///
/// Budget contract (SPEC §4): steady-state calls reuse a thread-local result buffer
/// and are callable from a real-time thread — no I/O is triggered beyond rti-db's
/// in-memory merge of already-loaded data. **Zero-allocation envelope:** for series of
/// at most 256 samples (the intended S1 working set; the threshold is the standard
/// library's stable-sort small-slice limit inside rti-db's collect path, measured on
/// rustc 1.98.1) steady-state calls perform no heap allocation, even under concurrent
/// ingest; larger series cost one rti-db-internal sort-scratch allocation per call.
///
/// Complexity disclosure: rti-db exposes no O(1) head read through its public API, so
/// this is currently a full-range scan of the series (O(n) in its sample count,
/// mutex-protected inside rti-db) returning the last point. Returns `Ok(None)` when the
/// series has no samples.
pub fn latest(db: &Db, series: SeriesId) -> Result<Option<Sample>> {
    LATEST_BUF.with(|b| {
        let mut buf = b.borrow_mut();
        buf.clear();
        db.collect(series, Timestamp::MIN, Timestamp::MAX, None, &mut buf)?;
        // `collect` guarantees ascending-ts order (memtable + segment merge), so the
        // last element is the latest point.
        Ok(buf.last().copied())
    })
}

// --------------------------------------------------------------- S2: window

/// One per-series lane of the k-way merge (sorted samples + read cursor).
struct Lane {
    samples: Vec<Sample>,
    pos: usize,
}

/// S2 episodic context: ordered window over several series.
///
/// Owns one collected buffer per series and k-way merges them in ascending ts order;
/// ties are broken by the order of `series` in the original [`window`] call (stable).
/// Iteration itself performs no allocation; samples are returned by value
/// ([`Sample`] is a 16-byte `Copy` type — zero-copy semantics without lifetimes).
pub struct WindowIter {
    lanes: Vec<Lane>,
    remaining: usize,
}

impl Iterator for WindowIter {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        if self.remaining == 0 {
            return None;
        }
        // Linear scan of the lane heads: k (the number of series) is small for VLA
        // working memory, so this beats a heap and needs no extra allocation.
        let mut best: Option<usize> = None;
        for (i, lane) in self.lanes.iter().enumerate() {
            if let Some(s) = lane.samples.get(lane.pos) {
                match best {
                    Some(b) if self.lanes[b].samples[self.lanes[b].pos].ts <= s.ts => {}
                    _ => best = Some(i),
                }
            }
        }
        let b = best?;
        let s = self.lanes[b].samples[self.lanes[b].pos];
        self.lanes[b].pos += 1;
        self.remaining -= 1;
        Some(s)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for WindowIter {}

/// S2 episodic context: ordered window over several series, zero-copy iterator.
///
/// Intended for attention-style consumption of the recent past. Collects each series
/// over `span` (`[start, end)`; the underlying inclusive `Db::scan` result is filtered
/// so `ts == end` is excluded) and returns a merging iterator in ascending ts order.
///
/// An empty `series` slice yields an empty iterator; `span.start >= span.end` is
/// rejected with [`Error::Corrupt`].
pub fn window(db: &Db, series: &[SeriesId], span: TimeSpan) -> Result<WindowIter> {
    if span.start >= span.end {
        return Err(Error::Corrupt(format!(
            "window span must satisfy start < end (got [{}, {}))",
            span.start, span.end
        )));
    }
    let mut lanes = Vec::with_capacity(series.len());
    let mut remaining = 0usize;
    for &s in series {
        let mut samples = Vec::new();
        // Db::scan/collect is inclusive on both ends; collect [start, end] and drop the
        // single possible ts == end point to honour the half-open [start, end) contract.
        db.collect(s, span.start, span.end, None, &mut samples)?;
        if samples.last().map(|l| l.ts == span.end).unwrap_or(false) {
            samples.pop();
        }
        remaining += samples.len();
        lanes.push(Lane { samples, pos: 0 });
    }
    Ok(WindowIter { lanes, remaining })
}

// ------------------------------------------------------- action chunking

/// Per-record durability wait budget for [`ChunkBuffer::seal_chunk`]: generous on
/// purpose — a timeout never loses data (the record stays in the ingest pipeline) but
/// aborts the seal, so it must only fire when the ingest thread is genuinely wedged.
const SEAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Action-chunking aware buffering: hold `k` future action slots aligned to the
/// model's control period, flushed as one batch per chunk boundary.
///
/// The staging buffer is pre-allocated with capacity `chunk_len` at construction:
/// [`ChunkBuffer::push`] never blocks and never allocates; when `chunk_len` samples are
/// staged it returns [`Error::SeriesFull`] (backpressure — call
/// [`ChunkBuffer::seal_chunk`]). `period` is the model's control period: the buffer
/// performs no sleeping/timing itself, it is carried as chunk-alignment metadata for
/// the runtime (see [`ChunkBuffer::period`]).
pub struct ChunkBuffer {
    db: Arc<Db>,
    series: SeriesId,
    chunk_len: u32,
    period: Duration,
    staged: Vec<Sample>,
}

impl ChunkBuffer {
    /// Create a buffer holding up to `chunk_len` action slots for `series`.
    pub fn new(db: Arc<Db>, series: SeriesId, chunk_len: u32, period: Duration) -> Self {
        Self {
            db,
            series,
            chunk_len,
            period,
            staged: Vec::with_capacity(chunk_len as usize),
        }
    }

    /// Stage one action sample. Never blocks, never allocates (capacity was reserved at
    /// construction). Returns [`Error::SeriesFull`] when the chunk is full.
    pub fn push(&mut self, s: Sample) -> Result<()> {
        if self.staged.len() >= self.chunk_len as usize {
            return Err(Error::SeriesFull);
        }
        self.staged.push(s);
        Ok(())
    }

    /// Flush the staged chunk as one batch: every sample goes through
    /// `Db::put_durable`, then the buffer is cleared and the durable watermark is
    /// returned (monotonically increasing; `>=` the watermark before the call once any
    /// sample was staged). Blocks until the batch is persisted (per-record budget
    /// [`SEAL_TIMEOUT`]); on error the staged samples are kept so the caller can retry.
    pub fn seal_chunk(&mut self) -> Result<u64> {
        for &s in &self.staged {
            self.db.put_durable(self.series, s, SEAL_TIMEOUT)?;
        }
        self.staged.clear();
        Ok(self.db.durable_watermark())
    }

    /// Number of samples currently staged (not yet sealed).
    pub fn pending(&self) -> usize {
        self.staged.len()
    }

    /// Chunk capacity in samples.
    pub fn chunk_len(&self) -> u32 {
        self.chunk_len
    }

    /// The model's control period this buffer is aligned to (metadata; the buffer
    /// itself performs no timing).
    pub fn period(&self) -> Duration {
        self.period
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rti_core::Config;

    fn db() -> Db {
        // Deterministic profile: pure in-memory, never touches the file system.
        let mut cfg = Config::deterministic();
        cfg.memtable_max = 1 << 16;
        Db::open(cfg).unwrap()
    }

    fn put(db: &Db, series: SeriesId, ts: Timestamp, value: f64) {
        loop {
            match db.put(series, Sample::new(ts, value)) {
                Ok(()) => return,
                Err(Error::SeriesFull) => std::thread::yield_now(),
                Err(e) => panic!("put failed: {e}"),
            }
        }
    }

    #[test]
    fn latest_returns_last_point_of_series() {
        let db = db();
        assert_eq!(latest(&db, 1).unwrap(), None, "empty series has no latest");
        for i in 0..10 {
            put(&db, 1, i * 100, i as f64);
        }
        put(&db, 2, 5, 999.0); // another series must not interfere
        let s = latest(&db, 1).unwrap().expect("series 1 has points");
        assert_eq!(s, Sample::new(900, 9.0));
        let s2 = latest(&db, 2).unwrap().expect("series 2 has one point");
        assert_eq!(s2, Sample::new(5, 999.0));
    }

    #[test]
    fn window_merges_series_in_order_over_half_open_span() {
        let db = db();
        // series 1: even timestamps, series 2: odd timestamps, plus a tie at ts=5.
        for i in 0..5 {
            put(&db, 1, i * 2, 100.0 + i as f64);
            put(&db, 2, i * 2 + 1, 200.0 + i as f64);
        }
        put(&db, 1, 5, 105.0); // deliberate cross-series tie with series 2's ts=5
        db.flush().unwrap();

        // [1, 8): must exclude ts=0 and ts=8, include everything between.
        let got: Vec<Sample> = window(&db, &[1, 2], TimeSpan::new(1, 8)).unwrap().collect();
        let ts: Vec<Timestamp> = got.iter().map(|s| s.ts).collect();
        assert_eq!(ts, vec![1, 2, 3, 4, 5, 5, 6, 7], "merged, ordered, half-open");
        // tie at ts=5: series 1 (first in the slice) comes before series 2.
        let tie: Vec<f64> = got.iter().filter(|s| s.ts == 5).map(|s| s.value).collect();
        assert_eq!(tie, vec![105.0, 202.0], "ties broken by series order");
        // exact iterator size
        let it = window(&db, &[1, 2], TimeSpan::new(1, 8)).unwrap();
        assert_eq!(it.len(), 8);

        // full coverage: [start, end) includes start, excludes end
        let got: Vec<Sample> = window(&db, &[1], TimeSpan::new(0, 9)).unwrap().collect();
        assert_eq!(got.iter().map(|s| s.ts).collect::<Vec<_>>(), vec![0, 2, 4, 5, 6, 8]);

        // empty series slice and unknown series yield empty iterators
        assert_eq!(window(&db, &[], TimeSpan::new(0, 100)).unwrap().count(), 0);
        assert_eq!(window(&db, &[42], TimeSpan::new(0, 100)).unwrap().count(), 0);
        // degenerate span rejected
        assert!(matches!(window(&db, &[1], TimeSpan::new(5, 5)), Err(Error::Corrupt(_))));
        assert!(matches!(window(&db, &[1], TimeSpan::new(9, 5)), Err(Error::Corrupt(_))));
    }

    #[test]
    fn chunk_buffer_seals_batch_and_advances_watermark() {
        let db = Arc::new(db());
        let wm0 = db.durable_watermark();
        let mut cb = ChunkBuffer::new(Arc::clone(&db), 7, 4, Duration::from_millis(20));
        assert_eq!(cb.chunk_len(), 4);
        assert_eq!(cb.period(), Duration::from_millis(20));
        assert_eq!(cb.pending(), 0);

        for i in 0..4 {
            cb.push(Sample::new(i, i as f64 * 0.5)).unwrap();
        }
        assert_eq!(cb.pending(), 4);
        // a full chunk rejects further pushes without blocking (backpressure)
        assert!(matches!(cb.push(Sample::new(4, 2.0)), Err(Error::SeriesFull)));

        let wm1 = cb.seal_chunk().unwrap();
        assert!(wm1 >= wm0 + 4, "watermark must cover the sealed batch ({wm0} -> {wm1})");
        assert_eq!(cb.pending(), 0);

        // all staged points landed and are readable back
        db.flush().unwrap();
        let got: Vec<Sample> = db.scan(7, 0, 3, None, None).unwrap().collect();
        assert_eq!(got.len(), 4);
        assert_eq!(got[3], Sample::new(3, 1.5));

        // sealing an empty chunk is a no-op that still reports the watermark
        let wm2 = cb.seal_chunk().unwrap();
        assert_eq!(wm2, db.durable_watermark());
    }
}
