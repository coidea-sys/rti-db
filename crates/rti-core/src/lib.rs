//! rti-core: fundamental types, errors, time, and configuration for rti-db.
//!
//! This crate contains no unsafe code and performs no hidden heap magic:
//! every type is `Copy` or a small explicitly-owned struct that upper-layer
//! crates can pass around freely on hard-real-time hot paths.

#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

//! ## no_std (v0.3)
//!
//! With the default `std` feature disabled this crate is `no_std` (depends only on `core` + `alloc`):
//! - the `Io` variant of [`Error`] and the `std::error::Error` impl exist only under `std`;
//! - [`Config`] (which contains a `PathBuf`) exists only under `std`;
//! - all other types ([`Sample`] / [`SyncPolicy`] / [`Profile`] / [`Mirror`] /
//!   [`TsAligner`], etc.) are identical in both modes.

#[cfg(not(feature = "std"))]
extern crate alloc;

mod align;

pub use align::{PtpProfile, TsAligner};

#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::path::PathBuf;

use core::net::SocketAddr;

/// Series (time-series / event stream) identifier.
pub type SeriesId = u32;

/// Timestamp in nanoseconds; the monotonic clock domain is guaranteed by the caller.
pub type Timestamp = i64;

/// A single sample point: timestamp + f64 value, 16 bytes, `Copy`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// Nanosecond timestamp.
    pub ts: Timestamp,
    /// Sample value.
    pub value: f64,
}

impl Sample {
    /// Construct a sample point.
    pub fn new(ts: Timestamp, value: f64) -> Self {
        Self { ts, value }
    }
}

/// Sync (durability) policy for the WAL / storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `sync_data` on every append (tightest latency-jitter bound, lowest throughput).
    Always,
    /// Group commit: flush only when more than `interval_us` microseconds have elapsed since the last sync.
    Group {
        /// Group-commit interval in microseconds.
        interval_us: u32,
    },
    /// No explicit flushing; rely on the OS page cache (highest throughput, largest crash window).
    None,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Group { interval_us: 1_000 }
    }
}

/// Runtime configuration profile (v0.3).
///
/// - [`Profile::Balanced`]: v0.1/v0.2 semantics — WAL + segment persistence;
///   the MemTable is sealed when full.
/// - [`Profile::Deterministic`]: deterministic profile — forces [`SyncPolicy::None`],
///   runs purely in memory (WAL/segment persistence disabled; `data_dir` may be `None`,
///   and even when it is `Some` the file system is never touched); when the MemTable is full the oldest
///   series is evicted by LRU and counted (see `Db::lru_evictions`); an optional [`Mirror`] UDP mirror can be attached.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Profile {
    /// Default profile: persistence + balanced latency.
    #[default]
    Balanced,
    /// Deterministic profile: pure in-memory, LRU eviction, optional mirroring.
    Deterministic,
}

/// UDP mirror configuration (v0.3, best-effort).
///
/// On every successful `put` enqueue, send one 20-byte little-endian datagram
/// (`series u32 | ts i64 | value bits u64`) to `addr`. Non-blocking socket:
/// send failures are only counted (`Db::mirror_stats`); never retried, never blocking the hot path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mirror {
    /// Mirror receiver address.
    pub addr: SocketAddr,
}

impl Mirror {
    /// Construct a mirror configuration.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

/// Engine configuration. All buffers are pre-allocated according to it.
///
/// Exists only under the `std` feature (`PathBuf` requires std).
#[cfg(feature = "std")]
#[derive(Clone, Debug)]
pub struct Config {
    /// Data directory (where WAL and segment files live).
    ///
    /// `Option` since v0.3: may be `None` under `Profile::Deterministic`
    /// (pure in-memory operation); must be `Some` under `Profile::Balanced`.
    pub data_dir: Option<PathBuf>,
    /// Maximum number of samples in the MemTable; on reaching it, seal to disk as a segment
    /// (the Deterministic profile evicts by LRU instead).
    pub memtable_max: usize,
    /// WAL sync policy (forced to [`SyncPolicy::None`] under the Deterministic profile).
    pub wal_sync: SyncPolicy,
    /// Number of bytes to pre-allocate for the rti-mem memory pool.
    pub pool_bytes: usize,
    /// Runtime configuration profile.
    pub profile: Profile,
    /// Optional UDP mirror.
    pub mirror: Option<Mirror>,
}

#[cfg(feature = "std")]
impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: Some(PathBuf::from("rti-data")),
            memtable_max: 1 << 16,
            wal_sync: SyncPolicy::default(),
            pool_bytes: 1 << 20,
            profile: Profile::Balanced,
            mirror: None,
        }
    }
}

#[cfg(feature = "std")]
impl Config {
    /// Convenience constructor for the deterministic profile: pure in-memory (`data_dir = None`),
    /// LRU eviction, no mirror (set the `mirror` field afterwards if needed).
    pub fn deterministic() -> Self {
        Self {
            data_dir: None,
            wal_sync: SyncPolicy::None,
            profile: Profile::Deterministic,
            ..Self::default()
        }
    }
}

/// Unified engine error type.
#[derive(Debug)]
pub enum Error {
    /// Series/buffer is full and the write was rejected (backpressure signal; the caller may retry).
    SeriesFull,
    /// WAL CRC check failed or frame incomplete at `offset`; recovery should truncate here.
    WalCorrupt {
        /// Byte offset where the corruption occurred.
        offset: u64,
    },
    /// Underlying I/O error (`std` feature only).
    #[cfg(feature = "std")]
    Io(std::io::Error),
    /// The requested series or segment does not exist.
    NotFound,
    /// Protocol parse error (rti-net line protocol).
    Protocol(String),
    /// Data format error (segment header, corrupted compressed stream, etc.).
    Corrupt(String),
    /// In-flight pipeline depth is full and the write was rejected (backpressure signal; the caller
    /// may retry later; produced under `BackpressurePolicy::Error` of the v0.5 io_uring in-flight batch pipeline).
    Backpressure,
    /// Timed out waiting for durability acknowledgment (v0.6 `put_durable`). **No data was lost** —
    /// the record is still in the ingest pipeline and will be persisted shortly; the caller may re-check the watermark or retry the wait.
    Timeout,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::SeriesFull => write!(f, "series/buffer full (backpressure)"),
            Error::WalCorrupt { offset } => write!(f, "wal corrupt at offset {offset}"),
            #[cfg(feature = "std")]
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::NotFound => write!(f, "not found"),
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
            Error::Corrupt(m) => write!(f, "corrupt data: {m}"),
            Error::Backpressure => write!(f, "in-flight pipeline full (backpressure)"),
            Error::Timeout => write!(f, "timed out waiting for durable watermark (data still in pipeline)"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Engine result alias.
pub type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_is_copy_and_eq() {
        let a = Sample::new(42, 1.5);
        let b = a; // Copy
        assert_eq!(a, b);
        assert_eq!(a.ts, 42);
        assert_eq!(a.value, 1.5);
        assert_eq!(std::mem::size_of::<Sample>(), 16);
    }

    #[test]
    fn error_display_and_io_conversion() {
        let e = Error::WalCorrupt { offset: 128 };
        assert!(e.to_string().contains("128"));
        let io = std::io::Error::new(std::io::ErrorKind::Other, "boom");
        let e2: Error = io.into();
        assert!(matches!(e2, Error::Io(_)));
        assert!(std::error::Error::source(&e2).is_some());
    }

    #[test]
    fn config_default_is_sane() {
        let c = Config::default();
        assert!(c.memtable_max > 0);
        assert!(c.pool_bytes > 0);
        assert!(matches!(c.wal_sync, SyncPolicy::Group { .. }));
        assert_eq!(c.profile, Profile::Balanced);
        assert!(c.data_dir.is_some());
        assert!(c.mirror.is_none());
    }

    #[test]
    fn deterministic_config_is_in_memory() {
        let c = Config::deterministic();
        assert_eq!(c.profile, Profile::Deterministic);
        assert_eq!(c.wal_sync, SyncPolicy::None, "Deterministic profile forces SyncPolicy::None");
        assert!(c.data_dir.is_none(), "Deterministic profile defaults to pure in-memory");
        assert!(c.mirror.is_none());
    }

    #[test]
    fn profile_and_mirror_basics() {
        assert_eq!(Profile::default(), Profile::Balanced);
        assert_ne!(Profile::Balanced, Profile::Deterministic);
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let m = Mirror::new(addr);
        assert_eq!(m.addr.port(), 9999);
        assert_eq!(m.addr.ip().to_string(), "127.0.0.1");
    }

    #[test]
    fn sync_policy_variants() {
        let a = SyncPolicy::Always;
        let g = SyncPolicy::Group { interval_us: 500 };
        let n = SyncPolicy::None;
        assert_ne!(a, g);
        assert_ne!(g, n);
        if let SyncPolicy::Group { interval_us } = g {
            assert_eq!(interval_us, 500);
        } else {
            panic!("expected group policy");
        }
    }
}
