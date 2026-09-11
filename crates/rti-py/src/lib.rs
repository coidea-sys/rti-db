//! Python bindings for rti-db (v0.9 roadmap item).
//!
//! The module is importable as `rti_db` after `maturin build` / `maturin develop`.
//! All blocking I/O (open, flush, seal, compact, durable waits, scan collection)
//! releases the GIL; `put`/`latest` are nanosecond-scale and hold it.

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use rti_core::{Config, Profile, Sample, SyncPolicy};
use rti_db_rs::Db;
use rti_query::Agg;
use std::time::Duration;

fn to_pyerr(e: rti_core::Error) -> PyErr {
    PyIOError::new_err(e.to_string())
}

/// rti-db database handle.
///
/// ```python
/// db = rti_db.Db("/var/lib/rti", sync="group", group_interval_us=1000)
/// db.put(1, 1_700_000_000_000_000_000, 42.0)
/// db.latest(1)  # -> (1700000000000000000, 42.0)
/// ```
#[pyclass(name = "Db")]
struct PyDb {
    inner: Db,
}

#[pymethods]
impl PyDb {
    /// Open (or create) a database.
    ///
    /// Args:
    ///     data_dir: directory for WAL + segments (omit for pure in-memory deterministic mode)
    ///     sync: "group" (default), "always", or "none"
    ///     group_interval_us: group-commit interval for sync="group" (default 1000)
    ///     profile: "balanced" (default) or "deterministic" (pure in-memory + LRU)
    ///     memtable_max: samples per MemTable before seal (0 = engine default)
    ///     pool_bytes: memory-pool pre-allocation (0 = engine default)
    #[new]
    #[pyo3(signature = (data_dir=None, sync="group", group_interval_us=1000, profile="balanced", memtable_max=0, pool_bytes=0))]
    fn new(
        py: Python<'_>,
        data_dir: Option<&str>,
        sync: &str,
        group_interval_us: u32,
        profile: &str,
        memtable_max: usize,
        pool_bytes: usize,
    ) -> PyResult<Self> {
        let mut cfg = Config {
            data_dir: data_dir.map(std::path::PathBuf::from),
            wal_sync: match sync {
                "always" => SyncPolicy::Always,
                "none" => SyncPolicy::None,
                "group" => SyncPolicy::Group {
                    interval_us: group_interval_us,
                },
                other => {
                    return Err(PyValueError::new_err(format!(
                        "sync must be 'group' | 'always' | 'none', got {other:?}"
                    )))
                }
            },
            profile: match profile {
                "balanced" => Profile::Balanced,
                "deterministic" => Profile::Deterministic,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "profile must be 'balanced' | 'deterministic', got {other:?}"
                    )))
                }
            },
            ..Config::default()
        };
        if memtable_max > 0 {
            cfg.memtable_max = memtable_max;
        }
        if pool_bytes > 0 {
            cfg.pool_bytes = pool_bytes;
        }
        let inner = py.detach(|| Db::open(cfg)).map_err(to_pyerr)?;
        Ok(Self { inner })
    }

    /// Enqueue one sample (lock-free, ~0.5 µs). Applied asynchronously by the ingest thread.
    fn put(&self, series: u32, ts: i64, value: f64) -> PyResult<()> {
        self.inner.put(series, Sample::new(ts, value)).map_err(to_pyerr)
    }

    /// Durable write: survives kill -9 once it returns. Returns the durable watermark.
    #[pyo3(signature = (series, ts, value, timeout_ms=50))]
    fn put_durable(&self, py: Python<'_>, series: u32, ts: i64, value: f64, timeout_ms: u64) -> PyResult<u64> {
        let db = &self.inner;
        py.detach(|| db.put_durable(series, Sample::new(ts, value), Duration::from_millis(timeout_ms)))
            .map_err(to_pyerr)
    }

    /// O(1) latest sample for a series: (ts, value) or None. Zero allocation, no I/O.
    fn latest(&self, series: u32) -> PyResult<Option<(i64, f64)>> {
        self.inner
            .latest(series)
            .map(|o| o.map(|s| (s.ts, s.value)))
            .map_err(to_pyerr)
    }

    /// Scan a series over [t0, t1] (nanosecond timestamps). Returns a list of (ts, value).
    /// agg folds the range into a single value: "min" | "max" | "sum" | "avg".
    #[pyo3(signature = (series, t0, t1, agg=None))]
    fn scan(&self, py: Python<'_>, series: u32, t0: i64, t1: i64, agg: Option<&str>) -> PyResult<Vec<(i64, f64)>> {
        let agg = match agg {
            None => None,
            Some("min") => Some(Agg::Min),
            Some("max") => Some(Agg::Max),
            Some("sum") => Some(Agg::Sum),
            Some("avg") => Some(Agg::Avg),
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "agg must be None | 'min' | 'max' | 'sum' | 'avg', got {other:?}"
                )))
            }
        };
        let db = &self.inner;
        py.detach(|| -> rti_core::Result<Vec<(i64, f64)>> {
            Ok(db.scan(series, t0, t1, None, agg)?
                .map(|s| (s.ts, s.value))
                .collect())
        })
        .map_err(to_pyerr)
    }

    /// Flush WAL to the OS and wait for the ingest thread to drain.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let db = &self.inner;
        py.detach(|| db.flush()).map_err(to_pyerr)
    }

    /// Seal the current MemTable into on-disk segments.
    fn seal(&self, py: Python<'_>) -> PyResult<()> {
        let db = &self.inner;
        py.detach(|| db.seal()).map_err(to_pyerr)
    }

    /// Compact eligible segments across all series. Returns merged-segment count.
    fn compact(&self, py: Python<'_>) -> PyResult<usize> {
        let db = &self.inner;
        py.detach(|| db.compact()).map_err(to_pyerr)
    }

    /// Compact eligible segments of one series. Returns merged-segment count.
    fn compact_series(&self, py: Python<'_>, series: u32) -> PyResult<usize> {
        let db = &self.inner;
        py.detach(|| db.compact_series(series)).map_err(to_pyerr)
    }

    /// Cumulative compaction counters as a dict.
    fn compaction_stats(&self) -> std::collections::BTreeMap<String, u64> {
        let s = self.inner.compaction_stats();
        [
            ("runs".to_string(), s.runs),
            ("input_segments".to_string(), s.input_segments),
            ("output_segments".to_string(), s.output_segments),
            ("input_bytes".to_string(), s.input_bytes),
            ("output_bytes".to_string(), s.output_bytes),
        ]
        .into_iter()
        .collect()
    }

    /// Durable watermark (monotonic; everything ≤ it survives process kill).
    #[getter]
    fn durable_watermark(&self) -> u64 {
        self.inner.durable_watermark()
    }

    /// Number of on-disk segment readers currently registered.
    #[getter]
    fn segment_count(&self) -> usize {
        self.inner.segment_count()
    }

    /// Samples currently in the MemTable.
    #[getter]
    fn memtable_len(&self) -> usize {
        self.inner.memtable_len()
    }
}

/// rti-db Python bindings.
#[pymodule(name = "rti_db")]
pub fn rti_db_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDb>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
