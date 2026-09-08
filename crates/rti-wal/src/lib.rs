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

/// checkpoint 临时文件路径（`wal.log` → `wal.tmp`）。
fn checkpoint_tmp_path(path: &Path) -> PathBuf {
    path.with_extension("tmp")
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
    /// 批量编码暂存缓冲（复用，避免每批分配）。
    scratch: Vec<u8>,
}

impl StdWalWriter {
    /// 打开（不存在则创建）`path` 并定位到末尾；已有内容被视为历史有效记录。
    ///
    /// v0.6：同时清理 checkpoint 中途崩溃可能遗留的临时文件（`wal.tmp`）——
    /// rename 之前崩溃时旧 WAL 完好，临时文件可直接删除。
    pub fn open(path: impl AsRef<Path>, sync: SyncPolicy) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let tmp = checkpoint_tmp_path(&path);
        if tmp.exists() {
            let _ = std::fs::remove_file(&tmp);
        }
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
            scratch: Vec::new(),
        })
    }

    /// 文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// v0.6：批量追加——整批编码进暂存缓冲、一次 `write_all`，
    /// 随后只做一次 [`StdWalWriter::maybe_sync`] 检查。
    ///
    /// 与逐条 [`WalWriter::append`] 语义等价（同一帧格式、同一策略），
    /// 但把每条的 `write_all` + 时钟读取摊薄到每批一次；
    /// [`SyncPolicy::Always`] 下整批一次 sync（调用方按批即组边界）。
    pub fn append_batch_fast(&mut self, recs: &[Record]) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        self.scratch.clear();
        self.scratch.reserve(recs.len() * FRAME_LEN);
        for rec in recs {
            let mut frame = [0u8; FRAME_LEN];
            encode_frame(rec, &mut frame);
            self.scratch.extend_from_slice(&frame);
        }
        self.writer.write_all(&self.scratch)?;
        self.offset += self.scratch.len() as u64;
        self.maybe_sync()
    }

    /// v0.6：仅在 sync 到期时刷盘（Group：距上次 sync ≥ interval）。
    ///
    /// 组提交的调用点（ingest 批边界）以此替代无条件 `sync_now`：
    /// fsync 频率由策略 interval 决定，而不是由批大小决定。
    /// `Always` 策略下等同 `sync_now`；`None` 策略下为 no-op。
    pub fn sync_if_due(&mut self) -> Result<()> {
        self.maybe_sync()
    }

    /// v0.6 WAL checkpoint：截断 WAL，仅保留 `keep` 中的记录。
    ///
    /// **调用方必须保证**除 `keep` 外的全部记录已被 segment 覆盖并落盘
    /// （rti-db 在 MemTable seal 成功、segment fsync 之后调用；`keep`
    /// 为 seal 点后进入新 MemTable、仍需 WAL 保护的记录）。
    ///
    /// 崩溃安全：`keep` 先完整编码写入 `wal.tmp` 并 fsync，再原子
    /// rename 覆盖 `wal.log`，最后 fsync 目录。rename 前崩溃 → 旧 WAL
    /// 完好（已覆盖记录与 segment 重复，恢复时按 ts 去重）；rename 后
    /// 崩溃 → 新 WAL（仅 `keep`）+ 已落盘 segment，两方向都无丢失。
    /// 偏移重置为 `keep` 的字节长度，后续追加接在其后。
    pub fn checkpoint_keep(&mut self, keep: &[Record]) -> Result<()> {
        self.writer.flush()?;
        let tmp = checkpoint_tmp_path(&self.path);
        {
            let mut f = File::create(&tmp)?;
            self.scratch.clear();
            self.scratch.reserve(keep.len() * FRAME_LEN);
            for rec in keep {
                let mut frame = [0u8; FRAME_LEN];
                encode_frame(rec, &mut frame);
                self.scratch.extend_from_slice(&frame);
            }
            f.write_all(&self.scratch)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            // 保证 rename 的目录项持久（Linux 下目录可以只读打开）。
            if let Ok(d) = File::open(dir) {
                let _ = d.sync_data();
            }
        }
        let file = OpenOptions::new().read(true).append(true).open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.offset = (keep.len() * FRAME_LEN) as u64;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// v0.6：把已缓冲但尚未写给 OS 的字节 flush 到内核（不 fsync）。
    ///
    /// ingest 批边界调用：保证「已应用」的记录至少离开进程地址空间——
    /// 进程崩溃（kill -9）时 OS 页缓存仍在，这是 `put_durable`
    /// 在 Group/None 档下的崩溃不丢语义基础；机器掉电语义仍由
    /// [`SyncPolicy`] 的 fsync 频率决定。
    pub fn flush_os(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
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

    /// v0.6：批量追加（整批一次编码一次写，按策略检查 sync）。语义同逐条 [`Wal::append`]。
    pub fn append_batch(&mut self, recs: &[Record]) -> Result<()> {
        self.inner.append_batch_fast(recs)
    }

    /// v0.6：仅在 sync 到期时刷盘（组提交的批边界调用；fsync 频率由 interval 决定）。
    pub fn sync_if_due(&mut self) -> Result<()> {
        self.inner.sync_if_due()
    }

    /// v0.6 WAL checkpoint：截断 WAL，仅保留 `keep` 中的记录
    /// （调用方保证其余记录已全部被 segment 覆盖并落盘）。
    pub fn checkpoint_keep(&mut self, keep: &[Record]) -> Result<()> {
        self.inner.checkpoint_keep(keep)
    }

    /// v0.6：flush 用户态缓冲到 OS（不 fsync）；ingest 批边界调用。
    pub fn flush_os(&mut self) -> Result<()> {
        self.inner.flush_os()
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

    /// v0.6：checkpoint 截断——截断后 offset 归零、旧记录不可恢复、
    /// 新写入正常追加且恢复只见新记录。
    #[test]
    fn checkpoint_truncates_and_recovers_only_new_records() {
        let d = tmpdir("checkpoint");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            for i in 0..10u32 {
                w.append(&Record::new(i, i as i64, i as f64)).unwrap();
            }
            w.sync_now().unwrap();
            assert_eq!(w.offset(), 10 * FRAME_LEN as u64);
            w.checkpoint_keep(&[]).unwrap();
            assert_eq!(w.offset(), 0, "checkpoint 后偏移归零");
            assert_eq!(std::fs::metadata(&p).unwrap().len(), 0, "checkpoint 后文件为空");
            // 截断后继续写入
            for i in 100..105u32 {
                w.append(&Record::new(i, i as i64, i as f64)).unwrap();
            }
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 5, "恢复只能看到 checkpoint 之后的记录");
        assert_eq!(recs[0], Record::new(100, 100, 100.0));
        assert_eq!(recs[4], Record::new(104, 104, 104.0));
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6：checkpoint 保留尾巴——seal 点后进入新 MemTable 的记录
    /// 必须留在 WAL 中，截断只丢弃已被 segment 覆盖的前缀。
    #[test]
    fn checkpoint_keep_preserves_uncovered_tail() {
        let d = tmpdir("checkpoint-keep");
        let p = d.join("wal.log");
        let tail: Vec<Record> = (50..53u32).map(|i| Record::new(i, i as i64, i as f64)).collect();
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            for i in 0..53u32 {
                w.append(&Record::new(i, i as i64, i as f64)).unwrap();
            }
            w.sync_now().unwrap();
            // 前 50 条已被 segment 覆盖，保留 50..53
            w.checkpoint_keep(&tail).unwrap();
            assert_eq!(w.offset(), 3 * FRAME_LEN as u64);
            // 截断后追加的新记录接在保留尾巴之后
            w.append(&Record::new(60, 60, 60.0)).unwrap();
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 4, "保留尾巴 3 条 + 新追加 1 条");
        assert_eq!(recs[0], Record::new(50, 50, 50.0));
        assert_eq!(recs[3], Record::new(60, 60, 60.0));
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6：checkpoint 中途崩溃模拟——rename 前留下 wal.tmp，
    /// 旧 WAL 完好；再次打开时临时文件被清理，数据无损。
    #[test]
    fn stale_checkpoint_tmp_is_cleaned_on_open() {
        let d = tmpdir("checkpoint-stale");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            w.append(&Record::new(1, 10, 1.5)).unwrap();
            w.sync_now().unwrap();
        }
        // 模拟 rename 前崩溃：留下非空 wal.tmp
        std::fs::write(d.join("wal.tmp"), b"garbage").unwrap();
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            assert!(!d.join("wal.tmp").exists(), "open 必须清理遗留临时文件");
            w.append(&Record::new(2, 20, 2.5)).unwrap();
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 2, "旧 WAL 不受遗留临时文件影响");
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6：批量追加与逐条追加帧格式逐字节一致。
    #[test]
    fn append_batch_matches_per_record_encoding() {
        let d = tmpdir("batch");
        let p1 = d.join("a.log");
        let p2 = d.join("b.log");
        let recs: Vec<Record> = (0..50u32).map(|i| Record::new(i, i as i64 * 7, i as f64 * 0.5)).collect();
        {
            let mut w = Wal::open(&p1, SyncPolicy::None).unwrap();
            for r in &recs {
                w.append(r).unwrap();
            }
            w.sync_now().unwrap();
        }
        {
            let mut w = Wal::open(&p2, SyncPolicy::None).unwrap();
            w.append_batch(&recs).unwrap();
            assert_eq!(w.offset(), 50 * FRAME_LEN as u64);
            w.sync_now().unwrap();
        }
        assert_eq!(std::fs::read(&p1).unwrap(), std::fs::read(&p2).unwrap(), "批量与逐条编码必须一致");
        assert_eq!(Wal::recover(&p2).unwrap().count(), 50);
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
