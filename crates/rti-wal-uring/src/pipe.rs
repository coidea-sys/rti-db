//! io_uring **在途批流水**（v0.5 Stream B，仅本文件豁免 unsafe）。
//!
//! 与 [`crate::UringFile`]（v0.2，同步阻塞）的差异：
//!
//! - [`UringPipeline::push`] 提交写 SQE 后**不等待**完成即返回批令牌
//!   （`BatchToken`），多批可同时在内核在途；
//! - 完成事件由 CQE reap 循环增量回收（[`UringPipeline::reap_available`] /
//!   阻塞版），回收即释放对应槽位；
//! - [`UringPipeline::flush`] 只等待**本批（令牌）及之前**的完成事件，
//!   不 drain 整个 ring；[`UringPipeline::sync`] = flush(最新批) + fsync；
//! - 在途深度上限可配（[`PipelineConfig::max_in_flight`]，默认 64）；
//!   深度满时按 [`BackpressurePolicy`] 返回 [`PipeError::Backpressure`]
//!   或阻塞等待槽位。
//!
//! 安全模型：每批数据先**拷贝**进 pipeline 自有的槽位缓冲，SQE 按裸指针
//! 引用槽位；槽位只有在对应 CQE 被 reap 后才复用，且 `Drop` 会 drain
//! 全部在途 SQE——因此内核持有裸指针的窗口严格在缓冲有效期内。

#![allow(unsafe_code)]

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use io_uring::{opcode, types, IoUring};

/// FSYNC SQE 的 user_data 标记（写 SQE 的 user_data 是槽位下标，远小于此值）。
const FSYNC_TAG: u64 = u64::MAX;

/// 批令牌：`push` 返回的单调递增批号，`flush(token)` 只等 ≤ token 的批完成。
pub type BatchToken = u64;

/// 在途深度满时的背压策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressurePolicy {
    /// 立即返回 [`PipeError::Backpressure`]，由调用方决定重试时机
    /// （fail-fast，时延有界，硬实时场景推荐）。
    Error,
    /// 阻塞 reap CQE 直到有空闲槽位（吞吐优先）。
    Block,
}

/// 流水配置。
#[derive(Clone, Copy, Debug)]
pub struct PipelineConfig {
    /// 同时在途的最大批数（默认 64；SPEC-wave45 Stream B 点名默认值）。
    pub max_in_flight: usize,
    /// 每批槽位缓冲字节数（默认 64 KiB，与 rti-wal 默认攒批上限一致）。
    pub slot_bytes: usize,
    /// 背压策略（默认 [`BackpressurePolicy::Block`]）。
    pub backpressure: BackpressurePolicy,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            slot_bytes: 64 * 1024,
            backpressure: BackpressurePolicy::Block,
        }
    }
}

/// 流水错误：I/O 失败或背压。
#[derive(Debug)]
pub enum PipeError {
    /// 底层 I/O 错误（含 CQE 返回的负 errno、短写）。
    Io(io::Error),
    /// 在途深度已满且策略为 [`BackpressurePolicy::Error`]。
    Backpressure,
}

impl fmt::Display for PipeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipeError::Io(e) => write!(f, "io_uring pipeline io error: {e}"),
            PipeError::Backpressure => write!(f, "io_uring pipeline in-flight limit reached"),
        }
    }
}

impl std::error::Error for PipeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipeError::Io(e) => Some(e),
            PipeError::Backpressure => None,
        }
    }
}

impl From<io::Error> for PipeError {
    fn from(e: io::Error) -> Self {
        PipeError::Io(e)
    }
}

/// 槽位：pipeline 自有的批缓冲；CQE reap 前绝不复用。
struct Slot {
    buf: Box<[u8]>,
    /// 当前占用该槽位的批号（仅在槽位被占用时有效）。
    batch: BatchToken,
    /// 本批有效字节数（CQE 短写核验基准）。
    len: u32,
}

/// io_uring 在途批流水提交器（Linux only）。
///
/// 单写者、append-only：写入偏移由内部单调推进，与 rti-wal 的
/// `BatchingWalWriter` 的连续提交一一对应。
pub struct UringPipeline {
    ring: IoUring,
    /// 保持 fd 存活；ring 中的 SQE 按裸 fd 引用它。
    file: File,
    slots: Vec<Slot>,
    /// 空闲槽位下标栈。
    free: Vec<usize>,
    /// 已提交未 reap 的写 SQE 数。
    in_flight: usize,
    /// 下一批写入偏移（= 已 push 字节数 + 起始偏移）。
    offset: u64,
    /// 已派发的最大批号（最新批令牌）。
    next_batch: BatchToken,
    /// 连续完成水位：≤ done_batch 的批全部 reap 完毕。
    done_batch: BatchToken,
    /// 批完成标志，按下标 `batch % max_in_flight` 复用
    /// （在途批数 < max_in_flight，无混叠）。
    done: Vec<bool>,
    cfg: PipelineConfig,
}

impl UringPipeline {
    /// 打开（不存在则创建）`path` 并按 `cfg` 创建流水。
    ///
    /// 已有内容长度作为续写起始偏移。`io_uring_setup` 被拒绝时返回
    /// `Err`（调用方回退 std 后端）。
    pub fn open(path: impl AsRef<Path>, cfg: PipelineConfig) -> io::Result<Self> {
        let path = path.as_ref();
        let start = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        Self::from_file(file, cfg, start)
    }

    /// 用已打开的 `file` 构造；`start_offset` 为已有日志长度（续写场景）。
    pub fn from_file(file: File, cfg: PipelineConfig, start_offset: u64) -> io::Result<Self> {
        let max_in_flight = cfg.max_in_flight.max(1);
        // ring 条目需容纳全部在途写 SQE + 1 个 FSYNC，向上取 2 的幂。
        let entries = (max_in_flight as u32 + 2).next_power_of_two();
        let ring = IoUring::new(entries)?;
        let slot_bytes = cfg.slot_bytes.max(1);
        let slots = (0..max_in_flight)
            .map(|_| Slot { buf: vec![0u8; slot_bytes].into_boxed_slice(), batch: 0, len: 0 })
            .collect();
        Ok(Self {
            ring,
            file,
            slots,
            free: (0..max_in_flight).rev().collect(),
            in_flight: 0,
            offset: start_offset,
            next_batch: 0,
            done_batch: 0,
            done: vec![true; max_in_flight],
            cfg,
        })
    }

    /// 当前写入偏移（下一批起点）。
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// 当前在途批数（诊断/测试用）。
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// 空闲槽位数（诊断/背压预检用）。
    pub fn available_slots(&self) -> usize {
        self.free.len()
    }

    /// 槽位字节上限（单批数据超过此值时 `push` 内部按槽拆分）。
    pub fn slot_bytes(&self) -> usize {
        self.slots.first().map(|s| s.buf.len()).unwrap_or(0)
    }

    /// 生效的背压策略。
    pub fn backpressure_policy(&self) -> BackpressurePolicy {
        self.cfg.backpressure
    }

    /// 把 `data` 提交为一批写 SQE（按槽位拆分可超过槽位大小），
    /// **不等待完成**即返回最新批令牌。
    ///
    /// 背压：无空闲槽位时按策略 [`BackpressurePolicy::Error`] 立即返回
    /// [`PipeError::Backpressure`]（本批一字节未提交，可整体重试），
    /// 或 [`BackpressurePolicy::Block`] 阻塞 reap 至有槽位。
    pub fn push(&mut self, data: &[u8]) -> Result<BatchToken, PipeError> {
        if data.is_empty() {
            return Ok(self.next_batch);
        }
        let slot_bytes = self.slot_bytes();
        // Error 策略：预检，保证整批要么全部提交要么一字节未提交。
        if self.cfg.backpressure == BackpressurePolicy::Error
            && (data.len() + slot_bytes - 1) / slot_bytes > self.free.len()
        {
            return Err(PipeError::Backpressure);
        }
        let mut token = self.next_batch;
        for chunk in data.chunks(slot_bytes) {
            token = self.push_one(chunk)?;
        }
        Ok(token)
    }

    /// 提交单个不超过槽位大小的 chunk 为一批。
    fn push_one(&mut self, data: &[u8]) -> Result<BatchToken, PipeError> {
        debug_assert!(data.len() <= self.slot_bytes());
        // 取槽位：Error 策略此处必然有（push 已预检）；Block 策略阻塞 reap。
        while self.free.is_empty() {
            match self.cfg.backpressure {
                BackpressurePolicy::Error => return Err(PipeError::Backpressure),
                BackpressurePolicy::Block => self.reap_blocking(1)?,
            }
        }
        let idx = self.free.pop().expect("free list non-empty after wait");
        let batch = self.next_batch + 1;
        {
            let slot = &mut self.slots[idx];
            slot.buf[..data.len()].copy_from_slice(data);
            slot.batch = batch;
            slot.len = data.len() as u32;
        }
        let di = self.done_idx(batch);
        self.done[di] = false;
        let fd = types::Fd(self.file.as_raw_fd());
        let slot = &self.slots[idx];
        let op = opcode::Write::new(fd, slot.buf.as_ptr(), slot.len)
            .offset(self.offset)
            .build()
            .user_data(idx as u64);
        {
            let mut sq = self.ring.submission();
            // SAFETY: SQE 按裸指针引用 `slot.buf`、按裸 fd 引用 `self.file`。
            // 两者均为 self 所有；槽位在该 SQE 的 CQE 被 reap 之前绝不复用
            // （free 栈只在 reap 时回收该下标），`Drop` 会 drain 全部在途
            // SQE，故内核持有引用的窗口严格在缓冲/fd 有效期内。
            unsafe { sq.push(&op) }
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ full"))?;
        }
        self.ring.submit()?;
        self.offset += data.len() as u64;
        self.next_batch = batch;
        self.in_flight += 1;
        Ok(batch)
    }

    /// 批号到完成标志下标（在途批数 < max_in_flight，取模无混叠）。
    fn done_idx(&self, batch: BatchToken) -> usize {
        (batch % self.cfg.max_in_flight as u64) as usize
    }

    /// 处理一个 CQE（内联以便在 `completion()` 借用期间更新槽位状态）。
    /// 返回 Err 时槽位已照常回收，避免泄漏死锁。
    fn handle_cqe(&mut self, user_data: u64, res: i32) -> Result<(), PipeError> {
        if user_data == FSYNC_TAG {
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res).into());
            }
            return Ok(());
        }
        let idx = user_data as usize;
        if idx >= self.slots.len() {
            return Err(io::Error::new(io::ErrorKind::Other, "io_uring bogus user_data").into());
        }
        let slot = &mut self.slots[idx];
        let expect = slot.len;
        let batch = slot.batch;
        let di = self.done_idx(batch);
        self.done[di] = true;
        self.free.push(idx);
        self.in_flight -= 1;
        while self.done_batch < self.next_batch
            && self.done[self.done_idx(self.done_batch + 1)]
        {
            self.done_batch += 1;
        }
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res).into());
        }
        if res as u32 != expect {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "io_uring short write").into());
        }
        Ok(())
    }

    /// 非阻塞 reap： drain 当前已就绪的全部 CQE，释放槽位。
    pub fn reap_available(&mut self) -> Result<(), PipeError> {
        // 先收集 user_data/result，再在 borrow 结束后统一更新状态。
        let mut pending: Vec<(u64, i32)> = Vec::new();
        for cqe in self.ring.completion() {
            pending.push((cqe.user_data(), cqe.result()));
        }
        let mut first_err = None;
        for (ud, res) in pending {
            if let Err(e) = self.handle_cqe(ud, res) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// 阻塞 reap：至少等 `want` 个 CQE 后 drain 全部就绪事件。
    fn reap_blocking(&mut self, want: usize) -> Result<(), PipeError> {
        self.ring.submit_and_wait(want)?;
        self.reap_available()
    }

    /// 等待 **token 及之前** 的批完成（只等本批，不 drain 全环）。
    ///
    /// token 之后的批（更晚 push 的）允许继续在途。
    pub fn flush(&mut self, token: BatchToken) -> Result<(), PipeError> {
        while self.done_batch < token {
            self.reap_blocking(1)?;
        }
        Ok(())
    }

    /// 持久化边界：等待当前全部在途批完成（= flush(最新令牌)），
    /// 再提交 `IORING_OP_FSYNC` 并等其完成（组提交的组边界）。
    pub fn sync(&mut self) -> Result<(), PipeError> {
        self.flush(self.next_batch)?;
        let fd = types::Fd(self.file.as_raw_fd());
        let op = opcode::Fsync::new(fd).build().user_data(FSYNC_TAG);
        {
            let mut sq = self.ring.submission();
            // SAFETY: FSYNC 只按裸 fd 引用 `self.file`；返回前已阻塞等待
            // 该 SQE 的 CQE，fd 活到函数返回之后。
            unsafe { sq.push(&op) }
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ full"))?;
        }
        self.ring.submit_and_wait(1)?;
        let mut completed = false;
        let mut err = None;
        for cqe in self.ring.completion() {
            if cqe.user_data() != FSYNC_TAG {
                continue;
            }
            let res = cqe.result();
            if res < 0 {
                err = Some(io::Error::from_raw_os_error(-res));
            }
            completed = true;
        }
        match (completed, err) {
            (true, None) => Ok(()),
            (true, Some(e)) => Err(e.into()),
            (false, _) => {
                Err(io::Error::new(io::ErrorKind::Other, "io_uring lost fsync completion").into())
            }
        }
    }

    /// 等待全部在途批完成（不 fsync；关闭前调用）。
    pub fn drain(&mut self) -> Result<(), PipeError> {
        self.flush(self.next_batch)
    }
}

impl Drop for UringPipeline {
    fn drop(&mut self) {
        // 尽力 drain：保证内核不再引用槽位缓冲后再释放。忽略错误。
        while self.in_flight > 0 {
            if self.ring.submit_and_wait(1).is_err() {
                break;
            }
            if self.reap_available().is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpfile(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-wal-uring-pipe-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d.join("pipe.log")
    }

    fn open_or_skip(p: &std::path::Path, cfg: PipelineConfig) -> Option<UringPipeline> {
        match UringPipeline::open(p, cfg) {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("io_uring unavailable ({e}), skipping on-ring assertion");
                None
            }
        }
    }

    /// 流水下顺序性：批数远超在途深度，中途穿插 flush/sync，读回逐字节一致。
    #[test]
    fn pipeline_roundtrip_ordering_beyond_depth() {
        let p = tmpfile("roundtrip");
        let cfg = PipelineConfig {
            max_in_flight: 8,
            slot_bytes: 64,
            backpressure: BackpressurePolicy::Block,
        };
        let mut pipe = match open_or_skip(&p, cfg) {
            Some(f) => f,
            None => return,
        };
        // 每批 64 B = 槽位大小；共 500 批 >> 深度 8。
        let batches: Vec<Vec<u8>> = (0..500u32)
            .map(|i| (0..64).map(|j| ((i * 64 + j) % 251) as u8).collect())
            .collect();
        let mut tokens = Vec::new();
        for (i, b) in batches.iter().enumerate() {
            tokens.push(pipe.push(b).unwrap());
            if i % 97 == 0 {
                // 只等本批：不 drain 全环。
                pipe.flush(tokens[i]).unwrap();
            }
        }
        assert_eq!(tokens.last().copied(), Some(500));
        pipe.sync().unwrap();
        assert_eq!(pipe.in_flight(), 0);
        let got = std::fs::read(&p).unwrap();
        let want: Vec<u8> = batches.concat();
        assert_eq!(got.len(), want.len());
        assert_eq!(got, want, "流水多批在途下写回读必须逐字节一致");
        drop(pipe);
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 单批数据超过槽位大小时内部拆分提交，读回一致。
    #[test]
    fn pipeline_oversize_batch_is_split() {
        let p = tmpfile("split");
        let cfg = PipelineConfig {
            max_in_flight: 4,
            slot_bytes: 16,
            backpressure: BackpressurePolicy::Block,
        };
        let mut pipe = match open_or_skip(&p, cfg) {
            Some(f) => f,
            None => return,
        };
        let data: Vec<u8> = (0..100u32).map(|i| (i % 13) as u8).collect(); // 100 B > 16 B
        pipe.push(&data).unwrap();
        pipe.sync().unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), data);
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 背压（Error 策略）：深度满后 push 立即返回 Backpressure 且
    /// 一字节未提交；flush 后槽位回收可继续提交。
    #[test]
    fn pipeline_backpressure_error_policy_is_fail_fast() {
        let p = tmpfile("backpressure");
        let cfg = PipelineConfig {
            max_in_flight: 2,
            slot_bytes: 32,
            backpressure: BackpressurePolicy::Error,
        };
        let mut pipe = match open_or_skip(&p, cfg) {
            Some(f) => f,
            None => return,
        };
        let b = |tag: u8| vec![tag; 32];
        let t1 = pipe.push(&b(1)).unwrap();
        let t2 = pipe.push(&b(2)).unwrap();
        assert_eq!(pipe.in_flight(), 2);
        let off_before = pipe.offset();
        // 深度满：Error 策略下 push 不做隐式 reap，确定性触发背压。
        assert!(matches!(pipe.push(&b(3)), Err(PipeError::Backpressure)));
        assert_eq!(pipe.offset(), off_before, "背压拒绝的批必须一字节未提交");
        // flush 本批后槽位回收，可继续。
        pipe.flush(t2).unwrap();
        assert_eq!(pipe.in_flight(), 0);
        let t3 = pipe.push(&b(3)).unwrap();
        assert!(t3 > t2 && t2 > t1, "批令牌必须单调递增");
        pipe.sync().unwrap();
        let got = std::fs::read(&p).unwrap();
        let mut want = b(1);
        want.extend_from_slice(&b(2));
        want.extend_from_slice(&b(3));
        assert_eq!(got, want);
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 背压（Block 策略）：深度 1 也能连续提交多批（内部阻塞回收）。
    #[test]
    fn pipeline_backpressure_block_policy_reaps_inline() {
        let p = tmpfile("block");
        let cfg = PipelineConfig {
            max_in_flight: 1,
            slot_bytes: 16,
            backpressure: BackpressurePolicy::Block,
        };
        let mut pipe = match open_or_skip(&p, cfg) {
            Some(f) => f,
            None => return,
        };
        let mut want = Vec::new();
        for i in 0..10u8 {
            let b = vec![i; 16];
            pipe.push(&b).unwrap();
            want.extend_from_slice(&b);
        }
        pipe.sync().unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), want);
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 关闭时全部 drain：不显式 flush/sync，Drop 后文件内容完整。
    #[test]
    fn pipeline_drop_drains_all_in_flight() {
        let p = tmpfile("drain");
        let cfg = PipelineConfig {
            max_in_flight: 4,
            slot_bytes: 32,
            backpressure: BackpressurePolicy::Block,
        };
        let mut want = Vec::new();
        {
            let mut pipe = match open_or_skip(&p, cfg) {
                Some(f) => f,
                None => return,
            };
            for i in 0..37u8 {
                let b = vec![i; 32];
                pipe.push(&b).unwrap();
                want.extend_from_slice(&b);
            }
            assert!(pipe.in_flight() > 0, "Drop 前必须仍有在途批");
        } // Drop：drain 全部在途 SQE
        assert_eq!(std::fs::read(&p).unwrap(), want, "Drop 必须 drain 全部在途写");
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// flush 只等本批：旧令牌在已完成时立即返回；续写偏移正确。
    #[test]
    fn flush_with_stale_token_and_resume_offset() {
        let p = tmpfile("stale");
        let cfg = PipelineConfig {
            max_in_flight: 4,
            slot_bytes: 16,
            backpressure: BackpressurePolicy::Block,
        };
        {
            let mut pipe = match open_or_skip(&p, cfg) {
                Some(f) => f,
                None => return,
            };
            let t = pipe.push(&[7u8; 16]).unwrap();
            pipe.sync().unwrap();
            // 水位已过：旧令牌 flush 必须立即返回（不等待任何事件）。
            pipe.flush(t).unwrap();
        }
        // 续写：已有 16 B，新 push 必须落在 offset 16。
        let mut pipe = match open_or_skip(&p, cfg) {
            Some(f) => f,
            None => return,
        };
        assert_eq!(pipe.offset(), 16);
        pipe.push(&[9u8; 16]).unwrap();
        pipe.sync().unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got, [vec![7u8; 16], vec![9u8; 16]].concat());
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 默认配置：深度 64（SPEC 点名默认值）。
    #[test]
    fn pipeline_config_defaults() {
        let cfg = PipelineConfig::default();
        assert_eq!(cfg.max_in_flight, 64);
        assert_eq!(cfg.backpressure, BackpressurePolicy::Block);
    }
}
