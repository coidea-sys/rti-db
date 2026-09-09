//! rti-export: LeRobot episode exporter for rti-db (v0.7).
//!
//! Exports a set of rti-db series over a time window as one LeRobot-style episode:
//! a single Parquet file under `data/chunk-000/episode_XXXXXX.parquet` plus dataset
//! metadata under `meta/` (see [`LeRobotParquetSink`]). Nanosecond timestamps are
//! written to the `timestamp` column; each configured series becomes one `f64` column.
//!
//! ## Frame grid and resampling
//!
//! - [`EpisodeSpec::frame_hz`] = `Some(hz)`: all series are aligned to a common frame
//!   grid `start + i * 1e9 / hz` (i = 0, 1, ... while `<= end`) using
//!   **last-value-carry-forward** (LVCF): the value of a column at frame time `t` is the
//!   value of the last sample with `ts <= t`. Frames before a series' first sample inside
//!   the window are `NaN`. The number of original samples folded into each frame is
//!   recorded in `meta/stats.json` by the Parquet sink.
//! - `None`: the grid is the sorted union of all series timestamps inside `[start, end]`
//!   (LVCF still applies across columns).
//!
//! ## Streaming
//!
//! The export pipeline runs `Db::scan` over per-chunk windows and feeds the sink in
//! bounded [`Chunk`]s of at most [`CHUNK_FRAMES`] frames, so peak memory stays O(chunk)
//! regardless of episode length.

#![forbid(unsafe_code)]

mod parquet_sink;
mod specfile;

pub use parquet_sink::{episode_parquet_path, LeRobotParquetSink};
pub use specfile::{load_spec_file, parse_spec_str};

use std::collections::VecDeque;
use std::path::PathBuf;

use rti_core::{Sample, SeriesId, Timestamp};
use rti_db::Db;

/// Maximum number of frames per [`Chunk`] handed to the sink (bounded-memory unit).
pub const CHUNK_FRAMES: usize = 8192;

/// Initial probe window (ns) used to locate the next sample of a series when the
/// frame grid is the union of timestamps (`frame_hz = None`); doubles on a miss.
const PROBE_INIT_NS: i64 = 1_000_000;

/// Export error type (std-only crate; wraps engine, I/O, and encoder failures).
#[derive(Debug)]
pub enum Error {
    /// Error returned by the rti-db engine (scan/put/open).
    Db(rti_core::Error),
    /// Filesystem I/O error.
    Io(std::io::Error),
    /// Parquet encoding/decoding error (arrow2/parquet2).
    Parquet(String),
    /// Invalid episode specification (bad range, empty name, malformed TOML, ...).
    Spec(String),
    /// JSON (de)serialization error while writing dataset metadata.
    Json(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::Db(e) => write!(f, "db error: {e}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Parquet(m) => write!(f, "parquet error: {m}"),
            Error::Spec(m) => write!(f, "spec error: {m}"),
            Error::Json(m) => write!(f, "json error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<rti_core::Error> for Error {
    fn from(e: rti_core::Error) -> Self {
        Error::Db(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<arrow2::error::Error> for Error {
    fn from(e: arrow2::error::Error) -> Self {
        Error::Parquet(e.to_string())
    }
}

impl From<parquet2::error::Error> for Error {
    fn from(e: parquet2::error::Error) -> Self {
        Error::Parquet(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e.to_string())
    }
}

/// Result alias used by this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// One episode to export: a named set of series over `[start, end]` (ns, inclusive).
pub struct EpisodeSpec {
    /// Episode name (recorded in metadata; must be non-empty).
    pub name: String,
    /// `(column name, series)` pairs; column names must be unique and non-empty.
    pub series: Vec<(String, SeriesId)>,
    /// Window start (ns, inclusive).
    pub start: Timestamp,
    /// Window end (ns, inclusive; must be `>= start`).
    pub end: Timestamp,
    /// Resample rate: when `Some(hz)`, align all series to the common grid
    /// `start + i * 1e9 / hz`; when `None`, use the union of series timestamps.
    pub frame_hz: Option<u32>,
}

/// One bounded batch of grid-aligned column data handed to an [`EpisodeSink`].
pub struct Chunk {
    /// Frame grid timestamps (ns), ascending; `len() <= CHUNK_FRAMES`.
    pub ts: Vec<Timestamp>,
    /// One `(column name, values)` entry per spec series; `values.len() == ts.len()`.
    pub cols: Vec<(String, Vec<f64>)>,
    /// Per-frame number of original samples folded into the frame (summed over series).
    pub frame_samples: Vec<u32>,
    /// Per-series number of original samples folded into this chunk.
    pub series_samples: Vec<u64>,
}

/// Export summary returned by [`export_episode`].
pub struct ExportMeta {
    /// Episode name (from the spec).
    pub name: String,
    /// Number of frames written.
    pub frames: u64,
    /// Effective frame rate (`None` for the union grid).
    pub frame_hz: Option<u32>,
    /// `(column name, original sample count)` per series.
    pub series_samples: Vec<(String, u64)>,
    /// Output path reported by the sink (episode Parquet file for [`LeRobotParquetSink`]).
    pub path: PathBuf,
}

/// Sink of aligned episode chunks (object-safe via `Self: Sized` on `close`).
pub trait EpisodeSink {
    /// Consume one aligned chunk (at most [`CHUNK_FRAMES`] frames).
    fn write_chunk(&mut self, cols: &Chunk) -> Result<()>;
    /// Flush and finish; returns the primary output path.
    fn close(self) -> Result<PathBuf>
    where
        Self: Sized;
}

/// Streaming per-series cursor: windowed `Db::scan`s keep memory bounded by the
/// samples inside the current chunk window, independent of episode length.
struct SeriesCursor<'db> {
    db: &'db Db,
    series: SeriesId,
    end: Timestamp,
    /// Lower bound (inclusive) of the next scan window.
    scan_from: Timestamp,
    /// Scanned but not yet consumed samples (ascending ts).
    buf: VecDeque<Sample>,
    /// LVCF state: value of the last consumed sample.
    carry: Option<f64>,
    /// Set once the scan range has covered `end`.
    exhausted: bool,
}

impl<'db> SeriesCursor<'db> {
    fn new(db: &'db Db, series: SeriesId, start: Timestamp, end: Timestamp) -> Self {
        Self { db, series, end, scan_from: start, buf: VecDeque::new(), carry: None, exhausted: false }
    }

    /// Scan forward so that every sample with `ts <= t` is buffered (one bounded
    /// window scan per call in the common case).
    fn fill_until(&mut self, t: Timestamp) -> Result<()> {
        let t = t.min(self.end);
        if !self.exhausted && self.scan_from <= t && self.buf.back().is_none_or(|s| s.ts > t) {
            for s in self.db.scan(self.series, self.scan_from, t, None, None)? {
                self.buf.push_back(s);
            }
            self.scan_from = t.saturating_add(1);
            if t >= self.end {
                self.exhausted = true;
            }
        }
        Ok(())
    }

    /// LVCF value at frame time `t`; also returns how many original samples were
    /// folded into this frame (samples consumed since the previous call).
    fn value_at(&mut self, t: Timestamp) -> Result<(f64, u32)> {
        self.fill_until(t)?;
        let mut n = 0u32;
        while let Some(s) = self.buf.front() {
            if s.ts > t {
                break;
            }
            self.carry = Some(s.value);
            self.buf.pop_front();
            n += 1;
        }
        Ok((self.carry.unwrap_or(f64::NAN), n))
    }

    /// Timestamp of the next unconsumed sample (used for the union grid). Probes
    /// forward with doubling windows so a sparse series never buffers the whole tail.
    fn peek_ts(&mut self) -> Result<Option<Timestamp>> {
        if let Some(s) = self.buf.front() {
            return Ok(Some(s.ts));
        }
        let mut w = PROBE_INIT_NS;
        while !self.exhausted {
            let hi = self.scan_from.saturating_add(w).min(self.end);
            if self.scan_from > hi {
                self.exhausted = true;
                break;
            }
            let mut found = false;
            for s in self.db.scan(self.series, self.scan_from, hi, None, None)? {
                self.buf.push_back(s);
                found = true;
            }
            self.scan_from = hi.saturating_add(1);
            if found {
                return Ok(self.buf.front().map(|s| s.ts));
            }
            if hi >= self.end {
                self.exhausted = true;
                break;
            }
            w = w.saturating_mul(2);
        }
        Ok(None)
    }
}

fn validate(spec: &EpisodeSpec) -> Result<()> {
    if spec.name.is_empty() {
        return Err(Error::Spec("episode name must be non-empty".into()));
    }
    if spec.series.is_empty() {
        return Err(Error::Spec("episode must contain at least one series".into()));
    }
    if spec.end < spec.start {
        return Err(Error::Spec(format!("end ({}) < start ({})", spec.end, spec.start)));
    }
    if spec.frame_hz == Some(0) {
        return Err(Error::Spec("frame_hz must be >= 1".into()));
    }
    for (i, (name, _)) in spec.series.iter().enumerate() {
        if name.is_empty() {
            return Err(Error::Spec(format!("series #{i}: column name must be non-empty")));
        }
        if spec.series[..i].iter().any(|(n, _)| n == name) {
            return Err(Error::Spec(format!("duplicate column name {name:?}")));
        }
    }
    Ok(())
}

/// Export one episode from `db` according to `spec`, streaming aligned chunks to `sink`.
///
/// See the crate-level docs for grid/LVCF semantics and streaming guarantees.
pub fn export_episode(db: &Db, spec: &EpisodeSpec, mut sink: impl EpisodeSink) -> Result<ExportMeta> {
    validate(spec)?;

    let mut cursors: Vec<SeriesCursor<'_>> = spec
        .series
        .iter()
        .map(|(_, id)| SeriesCursor::new(db, *id, spec.start, spec.end))
        .collect();
    let n_cols = cursors.len();
    let mut frames = 0u64;
    let mut series_samples = vec![0u64; n_cols];

    loop {
        let mut ts: Vec<Timestamp> = Vec::new();
        let mut cols: Vec<(String, Vec<f64>)> =
            spec.series.iter().map(|(n, _)| (n.clone(), Vec::new())).collect();
        let mut frame_samples: Vec<u32> = Vec::new();
        let mut chunk_series = vec![0u64; n_cols];

        match spec.frame_hz {
            Some(hz) => {
                // fixed grid: start + i * 1e9 / hz (u128 math avoids rounding drift).
                let span = (spec.end - spec.start) as u128;
                let total = span * hz as u128 / 1_000_000_000 + 1;
                let base = frames as u128;
                let n = (total - base).min(CHUNK_FRAMES as u128);
                ts.reserve(n as usize);
                for i in 0..n {
                    ts.push(spec.start + ((base + i) * 1_000_000_000 / hz as u128) as i64);
                }
                if ts.is_empty() {
                    break;
                }
                // one bounded window scan per series covers the whole chunk.
                let last = *ts.last().unwrap();
                for cur in cursors.iter_mut() {
                    cur.fill_until(last)?;
                }
                for &t in &ts {
                    let mut cnt = 0u32;
                    for (c, cur) in cursors.iter_mut().enumerate() {
                        let (v, n) = cur.value_at(t)?;
                        cols[c].1.push(v);
                        cnt += n;
                        chunk_series[c] += n as u64;
                        series_samples[c] += n as u64;
                    }
                    frame_samples.push(cnt);
                }
            }
            None => {
                // union grid: frames are the sorted union of series timestamps; each
                // frame consumes its samples immediately (keeps LVCF counts exact).
                while ts.len() < CHUNK_FRAMES {
                    let mut next: Option<Timestamp> = None;
                    for cur in cursors.iter_mut() {
                        if let Some(t) = cur.peek_ts()? {
                            next = Some(next.map_or(t, |m: Timestamp| m.min(t)));
                        }
                    }
                    let Some(t) = next else { break };
                    ts.push(t);
                    let mut cnt = 0u32;
                    for (c, cur) in cursors.iter_mut().enumerate() {
                        let (v, n) = cur.value_at(t)?;
                        cols[c].1.push(v);
                        cnt += n;
                        chunk_series[c] += n as u64;
                        series_samples[c] += n as u64;
                    }
                    frame_samples.push(cnt);
                }
                if ts.is_empty() {
                    break;
                }
            }
        }

        frames += ts.len() as u64;
        sink.write_chunk(&Chunk { ts, cols, frame_samples, series_samples: chunk_series })?;
    }

    let path = sink.close()?;
    Ok(ExportMeta {
        name: spec.name.clone(),
        frames,
        frame_hz: spec.frame_hz,
        series_samples: spec
            .series
            .iter()
            .map(|(n, _)| n.clone())
            .zip(series_samples)
            .collect(),
        path,
    })
}
