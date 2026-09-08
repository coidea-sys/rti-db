//! rti-core：rti-db 的基础类型、错误、时间与配置。
//!
//! 本 crate 不包含任何 unsafe 代码，也不做任何堆上的隐式魔法：
//! 所有类型都是 `Copy` 或显式拥有所有权的小结构，供上层 crate 在
//! 硬实时热路径上自由传递。

#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

//! ## no_std（v0.3）
//!
//! 默认 feature `std` 关闭时本 crate 为 `no_std`（仅依赖 `core` + `alloc`）：
//! - [`Error`] 的 `Io` 变体与 `std::error::Error` 实现仅在 `std` 下存在；
//! - [`Config`]（含 `PathBuf`）仅在 `std` 下存在；
//! - 其余类型（[`Sample`] / [`SyncPolicy`] / [`Profile`] / [`Mirror`] /
//!   [`TsAligner`] 等）在两种模式下完全一致。

#[cfg(not(feature = "std"))]
extern crate alloc;

mod align;

pub use align::{PtpProfile, TsAligner};

#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::path::PathBuf;

use core::net::SocketAddr;

/// 序列（时间序列/事件流）标识。
pub type SeriesId = u32;

/// 时间戳，纳秒，单调时钟域由调用方保证。
pub type Timestamp = i64;

/// 单个采样点：时间戳 + f64 值，16 字节，`Copy`。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// 纳秒时间戳。
    pub ts: Timestamp,
    /// 采样值。
    pub value: f64,
}

impl Sample {
    /// 构造一个采样点。
    pub fn new(ts: Timestamp, value: f64) -> Self {
        Self { ts, value }
    }
}

/// WAL / 存储的同步（持久化）策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// 每次 append 都 `sync_data`（最低时延抖动上界明确，吞吐最低）。
    Always,
    /// 组提交：距上次 sync 超过 `interval_us` 微秒时才刷盘。
    Group {
        /// 组提交间隔（微秒）。
        interval_us: u32,
    },
    /// 不主动刷盘，交给 OS 页缓存（吞吐最高，崩溃窗口最大）。
    None,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Group { interval_us: 1_000 }
    }
}

/// 运行配置档（v0.3）。
///
/// - [`Profile::Balanced`]：v0.1/v0.2 语义——WAL + segment 落盘，
///   MemTable 满则 seal；
/// - [`Profile::Deterministic`]：确定性档——强制 [`SyncPolicy::None`]、
///   纯内存运行（WAL/segment 落盘禁用，`data_dir` 可为 `None` 且即使
///   为 `Some` 也绝不触碰文件系统）、MemTable 满按 LRU 丢弃最老序列
///   并计数（见 `Db::lru_evictions`），可挂接 [`Mirror`] UDP 镜像。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Profile {
    /// 默认档：持久化 + 平衡时延。
    #[default]
    Balanced,
    /// 确定性档：纯内存、LRU 丢弃、可选镜像。
    Deterministic,
}

/// UDP 镜像配置（v0.3，best-effort）。
///
/// `put` 入队成功的同时向 `addr` 发送一条 20 字节小端数据报
/// （`series u32 | ts i64 | value bits u64`）。非阻塞 socket：
/// 发送失败只计数（`Db::mirror_stats`），不重试、不阻塞热路径。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mirror {
    /// 镜像接收端地址。
    pub addr: SocketAddr,
}

impl Mirror {
    /// 构造镜像配置。
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

/// 引擎配置。所有缓冲均按此预分配。
///
/// 仅 `std` feature 下存在（`PathBuf` 依赖 std）。
#[cfg(feature = "std")]
#[derive(Clone, Debug)]
pub struct Config {
    /// 数据目录（WAL 与 segment 文件所在）。
    ///
    /// v0.3 起为 `Option`：`Profile::Deterministic` 下可为 `None`
    /// （纯内存运行）；`Profile::Balanced` 下必须为 `Some`。
    pub data_dir: Option<PathBuf>,
    /// MemTable 最大采样点数，达到后 seal 落盘为 segment
    /// （Deterministic 档改为 LRU 丢弃）。
    pub memtable_max: usize,
    /// WAL 同步策略（Deterministic 档强制为 [`SyncPolicy::None`]）。
    pub wal_sync: SyncPolicy,
    /// rti-mem 内存池预分配字节数。
    pub pool_bytes: usize,
    /// 运行配置档。
    pub profile: Profile,
    /// 可选 UDP 镜像。
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
    /// 确定性档便捷构造：纯内存（`data_dir = None`）、LRU 丢弃、
    /// 无镜像（可再设 `mirror` 字段）。
    pub fn deterministic() -> Self {
        Self {
            data_dir: None,
            wal_sync: SyncPolicy::None,
            profile: Profile::Deterministic,
            ..Self::default()
        }
    }
}

/// 引擎统一错误类型。
#[derive(Debug)]
pub enum Error {
    /// 序列/缓冲已满，写入被拒绝（背压信号，调用方可重试）。
    SeriesFull,
    /// WAL 在 `offset` 处 CRC 校验失败或帧不完整；恢复时应在此截断。
    WalCorrupt {
        /// 损坏发生的字节偏移。
        offset: u64,
    },
    /// 底层 I/O 错误（仅 `std` feature）。
    #[cfg(feature = "std")]
    Io(std::io::Error),
    /// 请求的序列或 segment 不存在。
    NotFound,
    /// 协议解析错误（rti-net 行协议）。
    Protocol(String),
    /// 数据格式错误（segment 头部、压缩流损坏等）。
    Corrupt(String),
    /// 在途流水深度已满，写入被拒绝（背压信号，调用方可稍后重试；
    /// v0.5 io_uring 在途批流水 `BackpressurePolicy::Error` 下产生）。
    Backpressure,
    /// 等待持久化确认超时（v0.6 `put_durable`）。**数据未丢失**——
    /// 记录仍在 ingest 管线中，稍后将持久化；调用方可重查水位或重试等待。
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

/// 引擎结果别名。
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
        assert_eq!(c.wal_sync, SyncPolicy::None, "确定性档强制 SyncPolicy::None");
        assert!(c.data_dir.is_none(), "确定性档默认纯内存");
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
