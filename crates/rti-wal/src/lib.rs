//! rti-wal：预写日志。
//!
//! 帧格式（小端，定长 28 字节 + CRC，共 32 字节）：
//!
//! ```text
//! ┌──────────┬────────┬───────────┬───────────┬─────────┐
//! │ len u32  │ series │ ts   i64  │ value f64 │ crc u32 │
//! │ (=24)    │  u32   │           │  (bits)   │ (帧体)  │
//! └──────────┴────────┴───────────┴───────────┴─────────┘
//! ```
//!
//! - append-only：只追加，单写者；
//! - CRC32 覆盖 len 与帧体；恢复时遇到 CRC 不匹配或残帧即**截断**，
//!   之前的记录全部有效；
//! - 组提交：按 [`SyncPolicy`] 决定何时 `sync_data`。
//!
//! v0.2：写入路径抽象为 [`WalWriter`] trait——[`StdWalWriter`]
//! （v0.1 默认，[`Wal`] 内部委托给它，API 不变）与 feature
//! `io-uring`（Linux only）的 `IoUringWalWriter`（批量 SQE 提交 +
//! 组提交）；`WalWriter::auto` 探测失败优雅回退 Std。
//!
//! v0.5：feature `io-uring` 下新增 [`IoUringPipelinedWalWriter`]——
//! io_uring **在途批流水**：提交 SQE 后不等待，CQE reap 循环增量回收，
//! `sync_now` 只等本组完成事件 + fsync；在途深度（默认 64）满时按
//! 配置返回 [`Error::Backpressure`] 或阻塞。`WalWriter::auto` 行为不变。
//! unsafe 的 io_uring 胶水隔离在 `rti-wal-uring` crate，本 crate
//! 保持 `#![forbid(unsafe_code)]`。

#![forbid(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rti_core::{Error, Result, Sample, SeriesId, SyncPolicy, Timestamp};

mod batch;

pub use batch::{BatchSubmitter, BatchingWalWriter, DEFAULT_MAX_BATCH_BYTES};
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub use batch::{IoUringPipelineSubmitter, IoUringPipelinedWalWriter, IoUringSubmitter, IoUringWalWriter};

/// 一条 WAL 记录：序列 id + 采样点。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Record {
    /// 序列 id。
    pub series: SeriesId,
    /// 采样点。
    pub sample: Sample,
}

impl Record {
    /// 构造一条记录。
    pub fn new(series: SeriesId, ts: Timestamp, value: f64) -> Self {
        Self { series, sample: Sample { ts, value } }
    }
}

/// 帧长字段之后的帧体字节数：series(4) + ts(8) + value(8)。
const BODY_LEN: u32 = 20;
/// 整帧字节数：len(4) + body(20) + crc(4)。
const FRAME_LEN: usize = 28;

fn encode_frame(rec: &Record, out: &mut [u8; FRAME_LEN]) {
    out[0..4].copy_from_slice(&BODY_LEN.to_le_bytes());
    out[4..8].copy_from_slice(&rec.series.to_le_bytes());
    out[8..16].copy_from_slice(&rec.sample.ts.to_le_bytes());
    out[16..24].copy_from_slice(&rec.sample.value.to_bits().to_le_bytes());
    let crc = crc32fast::hash(&out[0..24]);
    out[24..28].copy_from_slice(&crc.to_le_bytes());
}

fn decode_frame(buf: &[u8]) -> std::result::Result<Record, ()> {
    if buf.len() < FRAME_LEN {
        return Err(());
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().map_err(|_| ())?);
    if len != BODY_LEN {
        return Err(());
    }
    let crc = u32::from_le_bytes(buf[24..28].try_into().map_err(|_| ())?);
    if crc32fast::hash(&buf[0..24]) != crc {
        return Err(());
    }
    let series = u32::from_le_bytes(buf[4..8].try_into().map_err(|_| ())?);
    let ts = i64::from_le_bytes(buf[8..16].try_into().map_err(|_| ())?);
    let bits = u64::from_le_bytes(buf[16..24].try_into().map_err(|_| ())?);
    Ok(Record { series, sample: Sample { ts, value: f64::from_bits(bits) } })
}

// ---------------------------------------------------------------- WalWriter

/// WAL 写入后端抽象（v0.2）。
///
/// v0.1 的 std `File` 实现保留为 [`StdWalWriter`]（默认后端）；
/// feature `io-uring`（Linux only）下另有 [`IoUringWalWriter`]
/// （批量 SQE 提交 + 组提交 fsync）。
///
/// 后端选择用 [`WalWriter::auto`]：探测失败（老内核 / seccomp 沙箱 /
/// 未启用 feature）自动回退 Std，永不 panic。
pub trait WalWriter {
    /// 追加一条记录，返回其字节偏移。O(1) 摊销。
    fn append(&mut self, rec: &Record) -> Result<u64>;

    /// 批量追加，返回**首条**记录的字节偏移。
    ///
    /// 默认实现逐条 [`WalWriter::append`]；io_uring 后端覆写为
    /// 「整批编码进同一 pending 缓冲、一次 SQE 组提交」。
    fn append_batch(&mut self, recs: &[Record]) -> Result<u64> {
        let mut first = self.offset();
        for (i, r) in recs.iter().enumerate() {
            let off = self.append(r)?;
            if i == 0 {
                first = off;
            }
        }
        Ok(first)
    }

    /// 立即持久化边界：flush 缓冲 + 刷盘（组提交的组边界 / 关闭前调用）。
    fn sync_now(&mut self) -> Result<()>;

    /// 当前写入偏移（即逻辑文件长度）。
    fn offset(&self) -> u64;

    /// 本写者生效的同步策略（兼容 v0.1 [`Wal`] 语义）。
    fn flush_policy(&self) -> SyncPolicy;

    /// 后端名（诊断/测试用）：`"std"` 或 `"io_uring"`。
    fn backend_name(&self) -> &'static str;
}

impl dyn WalWriter {
    /// 自动选择后端：优先 io_uring（feature 启用 + Linux + 内核允许），
    /// 探测失败优雅回退 [`StdWalWriter`]。
    ///
    /// 设环境变量 `RTI_WAL_FORCE_STD=1` 可强制回退（测试/部署逃生门）。
    pub fn auto(path: impl AsRef<Path>, sync: SyncPolicy) -> Result<Box<dyn WalWriter>> {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        {
            if std::env::var_os("RTI_WAL_FORCE_STD").is_none() {
                if let Ok(w) = IoUringWalWriter::open(path.as_ref(), sync) {
                    return Ok(Box::new(w));
                }
            }
        }
        Ok(Box::new(StdWalWriter::open(path, sync)?))
    }
}

/// std `File` + `BufWriter` 的 WAL 写者（v0.1 默认后端，语义不变）。
pub struct StdWalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    sync: SyncPolicy,
    /// 下一条记录的偏移。
    offset: u64,
    last_sync: Instant,
}

impl StdWalWriter {
    /// 打开（不存在则创建）`path` 并定位到末尾；已有内容被视为历史有效记录。
    pub fn open(path: impl AsRef<Path>, sync: SyncPolicy) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).append(true).open(&path).or_else(|_| {
            OpenOptions::new().create(true).read(true).append(true).open(&path)
        })?;
        let offset = file.metadata()?.len();
        Ok(Self {
            writer: BufWriter::new(file),
            path,
            sync,
            offset,
            last_sync: Instant::now(),
        })
    }

    /// 文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 按同步策略决定是否刷盘。
    fn maybe_sync(&mut self) -> Result<()> {
        match self.sync {
            SyncPolicy::Always => self.sync_now(),
            SyncPolicy::Group { interval_us } => {
                if self.last_sync.elapsed().as_micros() >= interval_us as u128 {
                    self.sync_now()?;
                }
                Ok(())
            }
            SyncPolicy::None => Ok(()),
        }
    }
}

impl WalWriter for StdWalWriter {
    fn append(&mut self, rec: &Record) -> Result<u64> {
        let mut frame = [0u8; FRAME_LEN];
        encode_frame(rec, &mut frame);
        self.writer.write_all(&frame)?;
        let off = self.offset;
        self.offset += FRAME_LEN as u64;
        self.maybe_sync()?;
        Ok(off)
    }

    fn sync_now(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.last_sync = Instant::now();
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn flush_policy(&self) -> SyncPolicy {
        self.sync
    }

    fn backend_name(&self) -> &'static str {
        "std"
    }
}

impl Drop for StdWalWriter {
    fn drop(&mut self) {
        // 尽力 flush；Drop 中忽略错误。
        let _ = self.writer.flush();
    }
}

/// 预写日志（append-only，单写者）。
///
/// v0.1 公共 API 保持不变；内部委托给 [`StdWalWriter`]。
pub struct Wal {
    inner: StdWalWriter,
}

impl Wal {
    /// 打开（不存在则创建）`path` 并定位到末尾；已有内容被视为历史有效记录。
    pub fn open(path: impl AsRef<Path>, sync: SyncPolicy) -> Result<Self> {
        Ok(Self { inner: StdWalWriter::open(path, sync)? })
    }

    /// 追加一条记录，返回其字节偏移。
    ///
    /// O(1)：只写顺序缓冲；是否落盘由 [`SyncPolicy`] 决定。
    pub fn append(&mut self, rec: &Record) -> Result<u64> {
        self.inner.append(rec)
    }

    /// 立即 flush 缓冲并 `sync_data`（组提交的组边界 / 关闭前调用）。
    pub fn sync_now(&mut self) -> Result<()> {
        self.inner.sync_now()
    }

    /// 当前写入偏移（即逻辑文件长度）。
    pub fn offset(&self) -> u64 {
        self.inner.offset()
    }

    /// 文件路径。
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// 崩溃恢复：顺序扫描，返回迭代有效记录的迭代器。
    ///
    /// 遇到 CRC 不匹配、长度字段非法或残帧时停止——即"损坏截断"语义：
    /// 损坏点之后的字节（崩溃半写）被忽略。整个文件预读入内存，
    /// 迭代本身零拷贝、零分配。
    pub fn recover(path: impl AsRef<Path>) -> Result<RecoverIter> {
        let mut buf = Vec::new();
        let mut file = match File::open(path.as_ref()) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(RecoverIter { buf, pos: 0 }),
            Err(e) => return Err(Error::Io(e)),
        };
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut buf)?;
        Ok(RecoverIter { buf, pos: 0 })
    }
}

/// WAL 恢复迭代器：逐帧产出有效记录，损坏处停止。
pub struct RecoverIter {
    buf: Vec<u8>,
    pos: usize,
}

impl RecoverIter {
    /// 遇到损坏帧时的字节偏移（若因损坏而终止）。
    pub fn corrupt_offset(&self) -> Option<u64> {
        if self.pos < self.buf.len() {
            Some(self.pos as u64)
        } else {
            None
        }
    }
}

impl Iterator for RecoverIter {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if self.pos + FRAME_LEN > self.buf.len() {
            // 残帧：视为此处截断
            return None;
        }
        match decode_frame(&self.buf[self.pos..self.pos + FRAME_LEN]) {
            Ok(rec) => {
                self.pos += FRAME_LEN;
                Some(rec)
            }
            Err(()) => None, // CRC 损坏：截断
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-wal-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn append_recover_roundtrip() {
        let d = tmpdir("roundtrip");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            let o0 = w.append(&Record::new(1, 100, 1.5)).unwrap();
            let o1 = w.append(&Record::new(2, 200, -2.5)).unwrap();
            assert_eq!(o0, 0);
            assert_eq!(o1, FRAME_LEN as u64);
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs, vec![Record::new(1, 100, 1.5), Record::new(2, 200, -2.5)]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §5 点名：CRC 损坏截断恢复测试。
    #[test]
    fn recover_truncates_on_crc_corruption() {
        let d = tmpdir("corrupt");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            for i in 0..5u32 {
                w.append(&Record::new(i, i as i64 * 10, i as f64)).unwrap();
            }
            w.sync_now().unwrap();
        }
        // 破坏第 3 条记录（offset = 2*FRAME_LEN）的一个 payload 字节
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start((2 * FRAME_LEN + 10) as u64)).unwrap();
            f.write_all(&[0xAB]).unwrap();
        }
        let mut it = Wal::recover(&p).unwrap();
        let recs: Vec<Record> = it.by_ref().collect();
        assert_eq!(recs.len(), 2, "损坏点后的记录必须被截断");
        assert_eq!(recs[0], Record::new(0, 0, 0.0));
        assert_eq!(recs[1], Record::new(1, 10, 1.0));
        assert_eq!(it.corrupt_offset(), Some((2 * FRAME_LEN) as u64));
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn recover_truncates_on_partial_tail_frame() {
        let d = tmpdir("partial");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            w.append(&Record::new(7, 70, 7.0)).unwrap();
            w.sync_now().unwrap();
        }
        // 模拟崩溃半写：追加半帧垃圾
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(&[0u8; FRAME_LEN / 2]).unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs, vec![Record::new(7, 70, 7.0)]);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn recover_missing_file_yields_empty() {
        let d = tmpdir("missing");
        let recs: Vec<Record> = Wal::recover(d.join("nope.log")).unwrap().collect();
        assert!(recs.is_empty());
        std::fs::remove_dir_all(&d).ok();
    }

    /// 环境变量是进程全局状态，触环境变量的测试必须串行。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// v0.1 API 不变：Wal 委托 StdWalWriter，语义逐字节一致。
    #[test]
    fn wal_delegates_to_std_writer_unchanged() {
        let d = tmpdir("delegate");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            w.append(&Record::new(1, 10, 1.0)).unwrap();
            assert_eq!(w.offset(), FRAME_LEN as u64);
            assert_eq!(w.path(), p.as_path());
            w.sync_now().unwrap();
        }
        assert_eq!(Wal::recover(&p).unwrap().count(), 1);
        std::fs::remove_dir_all(&d).ok();
    }

    /// 通过 trait object 使用 StdWalWriter（v0.2 多态写路径）。
    #[test]
    fn std_wal_writer_via_trait_object() {
        let d = tmpdir("traitobj");
        let p = d.join("wal.log");
        {
            let mut w: Box<dyn WalWriter> = Box::new(
                StdWalWriter::open(&p, SyncPolicy::Group { interval_us: 60_000_000 }).unwrap(),
            );
            assert_eq!(w.backend_name(), "std");
            assert_eq!(w.flush_policy(), SyncPolicy::Group { interval_us: 60_000_000 });
            let batch: Vec<Record> = (0..7u32).map(|i| Record::new(i, i as i64, i as f64)).collect();
            let first = w.append_batch(&batch).unwrap();
            assert_eq!(first, 0);
            assert_eq!(w.offset(), 7 * FRAME_LEN as u64);
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 7);
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC 点名：降级路径测试。`RTI_WAL_FORCE_STD=1` 强制回退 Std，
    /// 模拟 io_uring 不可用（seccomp 沙箱/老内核/未启用 feature）的场景；
    /// 回退后写读必须完整可用。
    #[test]
    fn auto_falls_back_to_std_when_forced() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("RTI_WAL_FORCE_STD", "1");
        let d = tmpdir("fallback");
        let p = d.join("wal.log");
        let res = <dyn WalWriter>::auto(&p, SyncPolicy::Always);
        std::env::remove_var("RTI_WAL_FORCE_STD");
        let mut w = res.unwrap();
        assert_eq!(w.backend_name(), "std", "强制回退后必须是 Std 后端");
        w.append(&Record::new(1, 1, 1.0)).unwrap();
        w.sync_now().unwrap();
        drop(w);
        assert_eq!(Wal::recover(&p).unwrap().count(), 1);
        std::fs::remove_dir_all(&d).ok();
    }

    /// 未强制时：feature on + 内核允许 → io_uring；否则优雅回退 std。
    /// 两种结果都必须可写可读（永不 panic / 永不 Err）。
    #[test]
    fn auto_picks_available_backend_and_works() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("RTI_WAL_FORCE_STD");
        let d = tmpdir("auto");
        let p = d.join("wal.log");
        let mut w = <dyn WalWriter>::auto(&p, SyncPolicy::Always).unwrap();
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        {
            let expect = if rti_wal_uring::probe() { "io_uring" } else { "std" };
            assert_eq!(w.backend_name(), expect);
        }
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        assert_eq!(w.backend_name(), "std", "无 feature/非 Linux 必须回退 Std");
        for i in 0..4u32 {
            w.append(&Record::new(i, i as i64, i as f64)).unwrap();
        }
        w.sync_now().unwrap();
        drop(w);
        assert_eq!(Wal::recover(&p).unwrap().count(), 4);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn group_sync_policy_batches() {
        let d = tmpdir("group");
        let p = d.join("wal.log");
        let mut w = Wal::open(&p, SyncPolicy::Group { interval_us: 1_000_000 }).unwrap();
        for i in 0..100u32 {
            w.append(&Record::new(i, i as i64, 0.0)).unwrap();
        }
        assert_eq!(w.offset(), 100 * FRAME_LEN as u64);
        w.sync_now().unwrap();
        drop(w);
        assert_eq!(Wal::recover(&p).unwrap().count(), 100);
        std::fs::remove_dir_all(&d).ok();
    }
}
