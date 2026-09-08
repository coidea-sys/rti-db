//! 批量提交的 WAL 写者（v0.2）：帧先入 pending 缓冲，按 [`SyncPolicy`]
//! 整批提交到后端，组提交边界一次 fsync。
//!
//! 提交后端抽象为 [`BatchSubmitter`]：真实后端是 feature `io-uring`
//! （Linux only）下的 `IoUringSubmitter`（一次系统调用提交整批 SQE）；
//! 测试用 mock 后端验证批处理/组提交决策逻辑，与内核是否放行
//! io_uring 无关。

use std::time::Instant;

use rti_core::{Result, SyncPolicy};

use super::{encode_frame, Record, WalWriter, FRAME_LEN};

/// pending 缓冲默认上限（字节）：达到即强制提交，保证内存有界。
pub const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024;

/// 批量提交后端抽象（同步阻塞语义：返回即完成）。
///
/// 实现者必须是安全的（rti-wal 为 `#![forbid(unsafe_code)]`）；
/// io_uring 的 unsafe 胶水隔离在 `rti-wal-uring` crate。
pub trait BatchSubmitter {
    /// 把 `buf` 整体写入文件的 `offset` 处，完成（至少进入内核）后返回。
    fn submit_write(&mut self, buf: &[u8], offset: u64) -> Result<()>;
    /// 组提交刷盘边界。
    fn submit_fsync(&mut self) -> Result<()>;
    /// 后端名（诊断/测试用）。
    fn name(&self) -> &'static str;
}

/// 批量提交 WAL 写者：泛型于提交后端。
///
/// - `append`/`append_batch` 只编码进 pending（O(1)，无系统调用）；
/// - `SyncPolicy::Always`：每次 append 提交 + fsync；
/// - `SyncPolicy::Group`：距上次 fsync 超过 `interval_us` 才提交 + fsync；
/// - `SyncPolicy::None`：pending 达到 [`DEFAULT_MAX_BATCH_BYTES`] 才提交
///   （不 fsync），崩溃窗口与 v0.1 Std 后端同语义。
pub struct BatchingWalWriter<S: BatchSubmitter> {
    submitter: S,
    /// 已编码未提交的帧。
    pending: Vec<u8>,
    /// 下一条记录的逻辑偏移（= committed + pending 已占）。
    offset: u64,
    /// 已提交到后端的字节数（= 文件写位置）。
    committed: u64,
    sync: SyncPolicy,
    last_sync: Instant,
    max_batch: usize,
}

impl<S: BatchSubmitter> BatchingWalWriter<S> {
    /// 用给定后端构造；`start_offset` 为已有日志长度（续写场景）。
    pub fn with_submitter(submitter: S, sync: SyncPolicy, start_offset: u64) -> Self {
        Self {
            submitter,
            pending: Vec::new(),
            offset: start_offset,
            committed: start_offset,
            sync,
            last_sync: Instant::now(),
            max_batch: DEFAULT_MAX_BATCH_BYTES,
        }
    }

    /// 设置 pending 上限（至少一帧）。
    pub fn with_max_batch_bytes(mut self, n: usize) -> Self {
        self.max_batch = n.max(FRAME_LEN);
        self
    }

    /// 当前 pending 未提交字节数（诊断/测试用）。
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    /// 把 pending 整批提交（一次后端调用）。
    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.submitter.submit_write(&self.pending, self.committed)?;
        self.committed += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// 按同步策略决定是否提交/刷盘。
    fn maybe_flush(&mut self) -> Result<()> {
        match self.sync {
            SyncPolicy::Always => self.sync_now(),
            SyncPolicy::Group { interval_us } => {
                if self.last_sync.elapsed().as_micros() >= interval_us as u128 {
                    self.sync_now()?;
                }
                Ok(())
            }
            SyncPolicy::None => {
                if self.pending.len() >= self.max_batch {
                    self.flush_pending()?;
                }
                Ok(())
            }
        }
    }
}

impl<S: BatchSubmitter> WalWriter for BatchingWalWriter<S> {
    fn append(&mut self, rec: &Record) -> Result<u64> {
        let off = self.offset;
        let mut frame = [0u8; FRAME_LEN];
        encode_frame(rec, &mut frame);
        self.pending.extend_from_slice(&frame);
        self.offset += FRAME_LEN as u64;
        self.maybe_flush()?;
        Ok(off)
    }

    fn append_batch(&mut self, recs: &[Record]) -> Result<u64> {
        let first = self.offset;
        self.pending.reserve(recs.len() * FRAME_LEN);
        for r in recs {
            let mut frame = [0u8; FRAME_LEN];
            encode_frame(r, &mut frame);
            self.pending.extend_from_slice(&frame);
        }
        self.offset += (recs.len() * FRAME_LEN) as u64;
        self.maybe_flush()?;
        Ok(first)
    }

    fn sync_now(&mut self) -> Result<()> {
        if let Err(e) = self.flush_pending() {
            // v0.5 在途流水：Error 背压策略下 pending 提交可能因在途深度满
            // 被拒。持久化边界必须能推进：先 submit_fsync 回收在途批
            // （流水后端 drain 在途 + fsync；同步后端本就要 fsync），
            // 再重试本批提交一次。
            if matches!(e, rti_core::Error::Backpressure) {
                self.submitter.submit_fsync()?;
                self.flush_pending()?;
            } else {
                return Err(e);
            }
        }
        self.submitter.submit_fsync()?;
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
        self.submitter.name()
    }
}

impl<S: BatchSubmitter> Drop for BatchingWalWriter<S> {
    fn drop(&mut self) {
        // 尽力提交残余 pending；Drop 中忽略错误。
        let _ = self.flush_pending();
    }
}

// ------------------------------------------------- io_uring 后端（feature）

/// io_uring 提交后端（feature `io-uring`，Linux only）。
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub struct IoUringSubmitter {
    file: rti_wal_uring::UringFile,
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl BatchSubmitter for IoUringSubmitter {
    fn submit_write(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        self.file.write_at(&[buf], offset)?;
        Ok(())
    }

    fn submit_fsync(&mut self) -> Result<()> {
        self.file.fsync()?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "io_uring"
    }
}

/// io_uring 后端 WAL 写者（feature `io-uring`，Linux only）。
///
/// 打开失败（`io_uring_setup` 被 seccomp 拒绝、内核 < 5.1 等）返回
/// `Err`；自动后端选择请用 [`WalWriter::auto`]（探测失败回退 Std）。
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub type IoUringWalWriter = BatchingWalWriter<IoUringSubmitter>;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl BatchingWalWriter<IoUringSubmitter> {
    /// 打开（不存在则创建）`path`；已有内容长度作为续写起点。
    ///
    /// `queue_depth` 固定 64：单写者按批提交，64 个 SQE 槽位远超
    /// 「每批一次提交」的需求。
    pub fn open(path: impl AsRef<std::path::Path>, sync: SyncPolicy) -> Result<Self> {
        let path = path.as_ref();
        let start = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let file = rti_wal_uring::UringFile::open(path, 64)?;
        Ok(Self::with_submitter(IoUringSubmitter { file }, sync, start))
    }
}

// ------------------------------------- io_uring 在途批流水（v0.5 Stream B）

/// io_uring **在途批流水**提交后端（feature `io-uring`，Linux only，v0.5）。
///
/// 与 [`IoUringSubmitter`]（同步阻塞）的差异：`submit_write` 提交 SQE 后
/// **不等待**完成即返回（多批在途），`submit_fsync`（= 组提交边界）只等待
/// 当前已提交批的完成事件再 fsync。背压由
/// [`rti_wal_uring::PipelineConfig`] 配置：在途深度（默认 64）满时返回
/// [`rti_core::Error::Backpressure`] 或阻塞 reap。
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub struct IoUringPipelineSubmitter {
    pipe: rti_wal_uring::UringPipeline,
}

/// 把流水错误映射为引擎错误：背压 → [`rti_core::Error::Backpressure`]。
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn map_pipe_err(e: rti_wal_uring::PipeError) -> rti_core::Error {
    match e {
        rti_wal_uring::PipeError::Backpressure => rti_core::Error::Backpressure,
        rti_wal_uring::PipeError::Io(e) => rti_core::Error::Io(e),
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl BatchSubmitter for IoUringPipelineSubmitter {
    fn submit_write(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        debug_assert_eq!(offset, self.pipe.offset(), "WAL 提交偏移必须连续");
        // 提交即返回（批进入内核在途）；持久化由 submit_fsync 保证，
        // 满足 BatchSubmitter「完成（至少进入内核）后返回」的合同。
        self.pipe.push(buf).map_err(map_pipe_err)?;
        Ok(())
    }

    fn submit_fsync(&mut self) -> Result<()> {
        // 只等当前已提交批（本组）完成 + fsync；不清空环。
        self.pipe.sync().map_err(map_pipe_err)
    }

    fn name(&self) -> &'static str {
        "io_uring_pipeline"
    }
}

/// io_uring 在途批流水 WAL 写者（feature `io-uring`，Linux only，v0.5）。
///
/// 打开失败（`io_uring_setup` 被 seccomp 拒绝、内核 < 5.1 等）返回
/// `Err`；自动后端选择仍用 [`WalWriter::auto`]（保持 v0.2 行为），
/// 流水后端需显式打开。
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub type IoUringPipelinedWalWriter = BatchingWalWriter<IoUringPipelineSubmitter>;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl BatchingWalWriter<IoUringPipelineSubmitter> {
    /// 打开（不存在则创建）`path`；已有内容长度作为续写起点。
    ///
    /// `cfg.max_in_flight` 为在途深度上限（默认 64），`cfg.backpressure`
    /// 为深度满时策略；pending 攒批上限自动对齐槽位大小。
    pub fn open(
        path: impl AsRef<std::path::Path>,
        sync: SyncPolicy,
        cfg: rti_wal_uring::PipelineConfig,
    ) -> Result<Self> {
        let pipe = rti_wal_uring::UringPipeline::open(path, cfg)?;
        let slot = pipe.slot_bytes();
        let start = pipe.offset();
        Ok(Self::with_submitter(IoUringPipelineSubmitter { pipe }, sync, start)
            .with_max_batch_bytes(slot))
    }

    /// 当前在途批数（诊断/测试用）。
    pub fn in_flight(&self) -> usize {
        self.submitter.pipe.in_flight()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// mock 提交后端：内存"文件"，记录 fsync 次数与提交调用。
    struct MockSubmitter {
        file: Vec<u8>,
        fsyncs: usize,
        write_calls: usize,
        fail_on_write: bool,
    }

    impl MockSubmitter {
        fn new() -> Self {
            Self { file: Vec::new(), fsyncs: 0, write_calls: 0, fail_on_write: false }
        }

        fn records(&self) -> Vec<Record> {
            let mut out = Vec::new();
            let mut pos = 0;
            while pos + FRAME_LEN <= self.file.len() {
                out.push(super::super::decode_frame(&self.file[pos..pos + FRAME_LEN]).unwrap());
                pos += FRAME_LEN;
            }
            out
        }
    }

    impl BatchSubmitter for MockSubmitter {
        fn submit_write(&mut self, buf: &[u8], offset: u64) -> Result<()> {
            self.write_calls += 1;
            if self.fail_on_write {
                return Err(rti_core::Error::Corrupt("mock write failure".into()));
            }
            let off = offset as usize;
            if self.file.len() < off + buf.len() {
                self.file.resize(off + buf.len(), 0);
            }
            self.file[off..off + buf.len()].copy_from_slice(buf);
            Ok(())
        }

        fn submit_fsync(&mut self) -> Result<()> {
            self.fsyncs += 1;
            Ok(())
        }

        fn name(&self) -> &'static str {
            "mock"
        }
    }

    fn rec(i: u32) -> Record {
        Record::new(i, i as i64 * 10, i as f64 + 0.5)
    }

    /// 组提交：interval 内不提交不 fsync；sync_now 一次提交整批。
    #[test]
    fn group_commit_holds_batch_until_sync() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::Group { interval_us: 60_000_000 }, 0);
        let recs: Vec<Record> = (0..100).map(rec).collect();
        let first = w.append_batch(&recs).unwrap();
        assert_eq!(first, 0);
        assert_eq!(w.offset(), 100 * FRAME_LEN as u64);
        assert_eq!(w.pending_bytes(), 100 * FRAME_LEN, "组提交窗口内必须攒批");
        assert_eq!(w.submitter.write_calls, 0);
        assert_eq!(w.submitter.fsyncs, 0);

        w.sync_now().unwrap();
        assert_eq!(w.submitter.write_calls, 1, "整批必须一次提交");
        assert_eq!(w.submitter.fsyncs, 1);
        assert_eq!(w.pending_bytes(), 0);
        assert_eq!(w.submitter.records(), recs, "mock 文件逐帧可解码且一致");
        assert_eq!(w.flush_policy(), SyncPolicy::Group { interval_us: 60_000_000 });
    }

    /// Always：每条记录一次提交 + 一次 fsync。
    #[test]
    fn always_syncs_every_append() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::Always, 0);
        for i in 0..3 {
            let off = w.append(&rec(i)).unwrap();
            assert_eq!(off, i as u64 * FRAME_LEN as u64);
        }
        assert_eq!(w.submitter.fsyncs, 3);
        assert_eq!(w.submitter.records(), (0..3).map(rec).collect::<Vec<_>>());
    }

    /// None：不 fsync；pending 达到阈值强制提交（内存有界）。
    #[test]
    fn none_flushes_on_threshold_without_fsync() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::None, 0)
            .with_max_batch_bytes(FRAME_LEN * 4);
        for i in 0..10u32 {
            w.append(&rec(i)).unwrap();
        }
        assert_eq!(w.submitter.fsyncs, 0);
        assert_eq!(w.submitter.write_calls, 2, "每攒够 4 帧提交一次");
        assert_eq!(w.pending_bytes(), 2 * FRAME_LEN);
        // Drop 尽力 flush 残余
        drop(w);
    }

    /// 批量提交偏移连续；后端错误沿 append 传播（fail-fast）。
    #[test]
    fn batch_offsets_contiguous_and_errors_propagate() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::Group { interval_us: 60_000_000 }, 0);
        let a = w.append_batch(&(0..4).map(rec).collect::<Vec<_>>()).unwrap();
        let b = w.append_batch(&(4..9).map(rec).collect::<Vec<_>>()).unwrap();
        assert_eq!(a, 0);
        assert_eq!(b, 4 * FRAME_LEN as u64);
        w.sync_now().unwrap();
        assert_eq!(w.submitter.records().len(), 9);

        let mut failing = MockSubmitter::new();
        failing.fail_on_write = true;
        let mut w2 = BatchingWalWriter::with_submitter(failing, SyncPolicy::Always, 0);
        assert!(w2.append(&rec(0)).is_err(), "后端写失败必须传播");
    }

    #[test]
    fn backend_name_and_start_offset() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::None, 7 * FRAME_LEN as u64);
        assert_eq!(w.backend_name(), "mock");
        let off = w.append(&rec(0)).unwrap();
        assert_eq!(off, 7 * FRAME_LEN as u64, "续写起点必须生效");
        w.sync_now().unwrap();
        // 帧必须落在起始偏移处且可解码
        let s = 7 * FRAME_LEN;
        let frame = super::super::decode_frame(&w.submitter.file[s..s + FRAME_LEN]).unwrap();
        assert_eq!(frame, rec(0));
    }
}

/// io_uring 真实后端测试（feature on + Linux；ring 不可用时优雅跳过）。
#[cfg(all(test, feature = "io-uring", target_os = "linux"))]
mod uring_tests {
    use super::*;
    use crate::Wal;
    use std::path::PathBuf;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-wal-batch-uring-{}-{}-{}",
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

    /// 真实 ring：append + append_batch + sync，Std 恢复路径读回一致。
    #[test]
    fn uring_writer_roundtrip_via_std_recover() {
        let d = tmpdir("roundtrip");
        let p = d.join("wal.log");
        {
            let mut w = match IoUringWalWriter::open(&p, SyncPolicy::Group { interval_us: 1_000 }) {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("io_uring unavailable ({e}), skipping on-ring assertion");
                    return;
                }
            };
            assert_eq!(w.backend_name(), "io_uring");
            for i in 0..3u32 {
                w.append(&Record::new(i, i as i64, 1.0)).unwrap();
            }
            let batch: Vec<Record> = (3..9u32).map(|i| Record::new(i, i as i64, 2.0)).collect();
            let first = w.append_batch(&batch).unwrap();
            assert_eq!(first, 3 * FRAME_LEN as u64);
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 9);
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.series, i as u32);
            assert_eq!(r.sample.ts, i as i64);
            assert_eq!(r.sample.value, if i < 3 { 1.0 } else { 2.0 });
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.5 点名：流水下顺序性。多批在途（批数 > 深度）经
    /// IoUringPipelinedWalWriter 写入，Std 恢复路径读回逐条一致。
    #[test]
    fn pipelined_writer_roundtrip_via_std_recover() {
        let d = tmpdir("pipe-roundtrip");
        let p = d.join("wal.log");
        let cfg = rti_wal_uring::PipelineConfig {
            max_in_flight: 8,
            slot_bytes: 256,
            backpressure: rti_wal_uring::BackpressurePolicy::Block,
        };
        {
            let mut w = match IoUringPipelinedWalWriter::open(&p, SyncPolicy::None, cfg) {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("io_uring unavailable ({e}), skipping on-ring assertion");
                    return;
                }
            };
            assert_eq!(w.backend_name(), "io_uring_pipeline");
            // 256 B 槽位 / 28 B 帧：每槽 9 帧；2000 条 >> 深度 8。
            for i in 0..2000u32 {
                w.append(&Record::new(i, i as i64 * 7, i as f64 + 0.25)).unwrap();
            }
            assert!(w.in_flight() > 0, "SyncPolicy::None 下提交后必须有多批在途");
            w.sync_now().unwrap();
            assert_eq!(w.in_flight(), 0, "sync_now 必须等本组全部完成");
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 2000);
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.series, i as u32);
            assert_eq!(r.sample.ts, i as i64 * 7);
            assert_eq!(r.sample.value, i as f64 + 0.25);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.5 点名：背压触发。Error 策略 + 深度 2：在途满时 append 返回
    /// Error::Backpressure；sync_now 回收后可继续写且数据完整。
    #[test]
    fn pipelined_backpressure_surfaces_as_error() {
        let d = tmpdir("pipe-bp");
        let p = d.join("wal.log");
        let cfg = rti_wal_uring::PipelineConfig {
            max_in_flight: 2,
            slot_bytes: 2 * FRAME_LEN, // 每批 2 帧即触发提交
            backpressure: rti_wal_uring::BackpressurePolicy::Error,
        };
        let mut w = match IoUringPipelinedWalWriter::open(&p, SyncPolicy::None, cfg) {
            Ok(w) => w,
            Err(_) => return, // 环境不支持时优雅跳过
        };
        // 槽位 = 2 帧：append 1/3 各触发一次提交 → 深度 2 占满在途。
        for i in 0..5u32 {
            w.append(&Record::new(i, i as i64, 1.0)).unwrap();
        }
        assert_eq!(w.in_flight(), 2);
        // append 5 使 pending 满 2 帧触发第三次提交：Error 策略不做隐式
        // reap，确定性触发背压。
        let err = w.append(&Record::new(5, 5, 1.0));
        assert!(
            matches!(err, Err(rti_core::Error::Backpressure)),
            "在途深度满必须返回 Backpressure，实际 {err:?}"
        );
        // sync_now 回收在途批；被拒批仍在 pending，重试后完整落盘。
        w.sync_now().unwrap();
        w.append(&Record::new(6, 6, 1.0)).unwrap();
        w.sync_now().unwrap();
        drop(w);
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 7, "背压拒绝不丢数据：pending 保留可重试");
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.series, i as u32);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.5 点名：关闭时全部 drain。不显式 sync_now，Drop 后读回完整。
    #[test]
    fn pipelined_drop_drains_all_in_flight() {
        let d = tmpdir("pipe-drain");
        let p = d.join("wal.log");
        let cfg = rti_wal_uring::PipelineConfig {
            max_in_flight: 4,
            slot_bytes: 4 * FRAME_LEN,
            backpressure: rti_wal_uring::BackpressurePolicy::Block,
        };
        {
            let mut w = match IoUringPipelinedWalWriter::open(&p, SyncPolicy::None, cfg) {
                Ok(w) => w,
                Err(_) => return,
            };
            for i in 0..100u32 {
                w.append(&Record::new(i, i as i64, 2.5)).unwrap();
            }
            assert!(w.in_flight() > 0, "Drop 前必须仍有在途批");
        } // Drop：flush pending + drain 全部在途
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 100, "Drop 必须 drain 全部在途批");
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.series, i as u32);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.5：流水后端组提交语义与 v0.2 一致（Group 窗口内攒批，
    /// sync_now 一次组提交）；续写不覆盖历史帧。
    #[test]
    fn pipelined_group_commit_and_resume() {
        let d = tmpdir("pipe-group");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            for i in 0..3u32 {
                w.append(&Record::new(i, i as i64, 0.0)).unwrap();
            }
            w.sync_now().unwrap();
        }
        let cfg = rti_wal_uring::PipelineConfig::default();
        {
            let mut w = match IoUringPipelinedWalWriter::open(
                &p,
                SyncPolicy::Group { interval_us: 60_000_000 },
                cfg,
            ) {
                Ok(w) => w,
                Err(_) => return,
            };
            let off = w.append(&Record::new(9, 90, 9.0)).unwrap();
            assert_eq!(off, 3 * FRAME_LEN as u64, "续写起点必须生效");
            assert_eq!(w.pending_bytes(), FRAME_LEN, "Group 窗口内必须攒批");
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[3], Record::new(9, 90, 9.0));
        std::fs::remove_dir_all(&d).ok();
    }

    /// 续写：已有日志长度作为起始偏移，不覆盖历史帧。
    #[test]
    fn uring_open_resumes_at_existing_length() {
        let d = tmpdir("resume");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            for i in 0..5u32 {
                w.append(&Record::new(i, i as i64, 0.0)).unwrap();
            }
            w.sync_now().unwrap();
        }
        {
            let mut w = match IoUringWalWriter::open(&p, SyncPolicy::Always) {
                Ok(w) => w,
                Err(_) => return,
            };
            let off = w.append(&Record::new(9, 90, 9.0)).unwrap();
            assert_eq!(off, 5 * FRAME_LEN as u64);
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 6);
        assert_eq!(recs[5], Record::new(9, 90, 9.0));
        std::fs::remove_dir_all(&d).ok();
    }
}
