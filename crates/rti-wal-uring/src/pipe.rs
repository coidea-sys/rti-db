//! io_uring **in-flight batch pipeline** (v0.5 Stream B; only this file is exempt from the unsafe ban).
//!
//! Differences from [`crate::UringFile`] (v0.2, synchronous blocking):
//!
//! - [`UringPipeline::push`] returns a batch token (`BatchToken`) **without waiting** for completion
//!   after submitting write SQEs; multiple batches can be in flight in the kernel at once;
//! - completion events are reclaimed incrementally by a CQE reap loop ([`UringPipeline::reap_available`] /
//!   the blocking variant); reclaiming frees the corresponding slot;
//! - [`UringPipeline::flush`] waits only for the completion events of **this batch (token) and earlier**,
//!   without draining the whole ring; [`UringPipeline::sync`] = flush(latest batch) + fsync;
//! - the in-flight depth cap is configurable ([`PipelineConfig::max_in_flight`], default 64);
//!   when the depth is full, returns [`PipeError::Backpressure`] or blocks waiting for a slot,
//!   per [`BackpressurePolicy`].
//!
//! Safety model: each batch's data is first **copied** into a pipeline-owned slot buffer, and SQEs
//! reference the slot by raw pointer; a slot is reused only after its CQE has been reaped, and `Drop`
//! drains all in-flight SQEs — so the window where the kernel holds raw pointers is strictly within the buffer's lifetime.

#![allow(unsafe_code)]

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use io_uring::{opcode, types, IoUring};

/// user_data marker for FSYNC SQEs (write SQEs use the slot index as user_data, far smaller than this).
const FSYNC_TAG: u64 = u64::MAX;

/// Batch token: the monotonically increasing batch number returned by `push`; `flush(token)` waits only for batches <= token.
pub type BatchToken = u64;

/// Backpressure policy when the in-flight depth is full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressurePolicy {
    /// Return [`PipeError::Backpressure`] immediately, letting the caller decide when to retry
    /// (fail-fast, bounded latency, recommended for hard real-time scenarios).
    Error,
    /// Block in reap CQE until a slot is free (throughput-first).
    Block,
}

/// Pipeline configuration.
#[derive(Clone, Copy, Debug)]
pub struct PipelineConfig {
    /// Maximum number of batches in flight at once (default 64; the default named by SPEC-wave45 Stream B).
    pub max_in_flight: usize,
    /// Bytes per batch slot buffer (default 64 KiB, matching rti-wal's default batching cap).
    pub slot_bytes: usize,
    /// Backpressure policy (default [`BackpressurePolicy::Block`]).
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

/// Pipeline error: I/O failure or backpressure.
#[derive(Debug)]
pub enum PipeError {
    /// Underlying I/O error (including negative errnos returned by CQEs, short writes).
    Io(io::Error),
    /// In-flight depth full with policy [`BackpressurePolicy::Error`].
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

/// Slot: a pipeline-owned batch buffer; never reused before its CQE is reaped.
struct Slot {
    buf: Box<[u8]>,
    /// Batch number currently occupying the slot (valid only while the slot is occupied).
    batch: BatchToken,
    /// Valid bytes of this batch (baseline for CQE short-write verification).
    len: u32,
}

/// io_uring in-flight batch pipeline submitter (Linux only).
///
/// Single writer, append-only: the write offset advances monotonically internally, corresponding
/// one-to-one with the continuous commits of rti-wal's `BatchingWalWriter`.
pub struct UringPipeline {
    ring: IoUring,
    /// Keeps the fd alive; SQEs in the ring reference it by raw fd.
    file: File,
    slots: Vec<Slot>,
    /// Stack of free slot indices.
    free: Vec<usize>,
    /// Write SQEs submitted but not yet reaped.
    in_flight: usize,
    /// Write offset of the next batch (= pushed bytes + start offset).
    offset: u64,
    /// Highest batch number issued (latest batch token).
    next_batch: BatchToken,
    /// Continuous completion watermark: all batches <= done_batch have been reaped.
    done_batch: BatchToken,
    /// Batch completion flags, reused by index `batch % max_in_flight`
    /// (fewer than max_in_flight batches in flight, so no aliasing).
    done: Vec<bool>,
    cfg: PipelineConfig,
}

impl UringPipeline {
    /// Open (creating if missing) `path` and create the pipeline per `cfg`.
    ///
    /// The existing content length is the resume offset. Returns `Err` when `io_uring_setup` is rejected
    /// (the caller falls back to the std backend).
    pub fn open(path: impl AsRef<Path>, cfg: PipelineConfig) -> io::Result<Self> {
        let path = path.as_ref();
        let start = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let file = OpenOptions::new().create(true).write(true).truncate(false).open(path)?;
        Self::from_file(file, cfg, start)
    }

    /// Construct from an already-open `file`; `start_offset` is the existing log length (resume scenario).
    pub fn from_file(file: File, cfg: PipelineConfig, start_offset: u64) -> io::Result<Self> {
        let max_in_flight = cfg.max_in_flight.max(1);
        // ring entries must hold all in-flight write SQEs + 1 FSYNC; rounded up to a power of two.
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

    /// Current write offset (start of the next batch).
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Current number of in-flight batches (diagnostics/tests).
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Number of free slots (diagnostics / backpressure pre-check).
    pub fn available_slots(&self) -> usize {
        self.free.len()
    }

    /// Slot byte cap (`push` internally splits a batch larger than this into slots).
    pub fn slot_bytes(&self) -> usize {
        self.slots.first().map(|s| s.buf.len()).unwrap_or(0)
    }

    /// Backpressure policy in effect.
    pub fn backpressure_policy(&self) -> BackpressurePolicy {
        self.cfg.backpressure
    }

    /// Submit `data` as a batch of write SQEs (split into slots if larger than a slot),
    /// returning the latest batch token **without waiting for completion**.
    ///
    /// Backpressure: with no free slot, returns [`PipeError::Backpressure`] immediately under
    /// [`BackpressurePolicy::Error`] (not a single byte of this batch is submitted; the whole batch can
    /// be retried), or blocks in reap until a slot is free under [`BackpressurePolicy::Block`].
    pub fn push(&mut self, data: &[u8]) -> Result<BatchToken, PipeError> {
        if data.is_empty() {
            return Ok(self.next_batch);
        }
        let slot_bytes = self.slot_bytes();
        // Error policy: pre-check, guaranteeing the whole batch is either fully submitted or not submitted at all.
        if self.cfg.backpressure == BackpressurePolicy::Error
            && data.len().div_ceil(slot_bytes) > self.free.len()
        {
            return Err(PipeError::Backpressure);
        }
        let mut token = self.next_batch;
        for chunk in data.chunks(slot_bytes) {
            token = self.push_one(chunk)?;
        }
        Ok(token)
    }

    /// Submit a single chunk no larger than a slot as one batch.
    fn push_one(&mut self, data: &[u8]) -> Result<BatchToken, PipeError> {
        debug_assert!(data.len() <= self.slot_bytes());
        // take a slot: the Error policy always has one here (push pre-checked); the Block policy blocks in reap.
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
            // SAFETY: the SQE references `slot.buf` by raw pointer and `self.file` by raw fd.
            // Both are owned by self; a slot is never reused before that SQE's CQE is reaped
            // (the free stack recycles an index only at reap time), and `Drop` drains all in-flight
            // SQEs — so the window where the kernel holds these references is strictly within the buffer/fd lifetimes.
            unsafe { sq.push(&op) }
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ full"))?;
        }
        self.ring.submit()?;
        self.offset += data.len() as u64;
        self.next_batch = batch;
        self.in_flight += 1;
        Ok(batch)
    }

    /// Batch number to completion-flag index (fewer than max_in_flight batches in flight, so modulo never aliases).
    fn done_idx(&self, batch: BatchToken) -> usize {
        (batch % self.cfg.max_in_flight as u64) as usize
    }

    /// Process one CQE (inlined so slot state can be updated during the `completion()` borrow).
    /// On Err the slot is recycled as usual, avoiding leak-deadlocks.
    fn handle_cqe(&mut self, user_data: u64, res: i32) -> Result<(), PipeError> {
        if user_data == FSYNC_TAG {
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res).into());
            }
            return Ok(());
        }
        let idx = user_data as usize;
        if idx >= self.slots.len() {
            return Err(io::Error::other("io_uring bogus user_data").into());
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

    /// Non-blocking reap: drain all currently ready CQEs, freeing slots.
    pub fn reap_available(&mut self) -> Result<(), PipeError> {
        // collect user_data/result first, then update state after the borrow ends.
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

    /// Blocking reap: wait for at least `want` CQEs, then drain all ready events.
    fn reap_blocking(&mut self, want: usize) -> Result<(), PipeError> {
        self.ring.submit_and_wait(want)?;
        self.reap_available()
    }

    /// Wait for the completion of **token and earlier** batches (this batch only; the ring is not drained).
    ///
    /// Batches after token (pushed later) may stay in flight.
    pub fn flush(&mut self, token: BatchToken) -> Result<(), PipeError> {
        while self.done_batch < token {
            self.reap_blocking(1)?;
        }
        Ok(())
    }

    /// Durability boundary: wait for all currently in-flight batches to complete (= flush(latest token)),
    /// then submit `IORING_OP_FSYNC` and wait for its completion (group boundary of group commit).
    pub fn sync(&mut self) -> Result<(), PipeError> {
        self.flush(self.next_batch)?;
        let fd = types::Fd(self.file.as_raw_fd());
        let op = opcode::Fsync::new(fd).build().user_data(FSYNC_TAG);
        {
            let mut sq = self.ring.submission();
            // SAFETY: FSYNC references only `self.file` by raw fd; the CQE of this SQE has been
            // blockingly awaited before returning, and the fd outlives the function return.
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
                Err(io::Error::other("io_uring lost fsync completion").into())
            }
        }
    }

    /// Wait for all in-flight batches to complete (no fsync; call before close).
    pub fn drain(&mut self) -> Result<(), PipeError> {
        self.flush(self.next_batch)
    }
}

impl Drop for UringPipeline {
    fn drop(&mut self) {
        // best-effort drain: ensure the kernel no longer references slot buffers before freeing them. Errors ignored.
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

    /// Ordering under the pipeline: far more batches than the in-flight depth, flush/sync interleaved, read back byte-identical.
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
        // 64 B per batch = slot size; 500 batches >> depth 8.
        let batches: Vec<Vec<u8>> = (0..500u32)
            .map(|i| (0..64).map(|j| ((i * 64 + j) % 251) as u8).collect())
            .collect();
        let mut tokens = Vec::new();
        for (i, b) in batches.iter().enumerate() {
            tokens.push(pipe.push(b).unwrap());
            if i % 97 == 0 {
                // wait only for this batch: the ring is not drained.
                pipe.flush(tokens[i]).unwrap();
            }
        }
        assert_eq!(tokens.last().copied(), Some(500));
        pipe.sync().unwrap();
        assert_eq!(pipe.in_flight(), 0);
        let got = std::fs::read(&p).unwrap();
        let want: Vec<u8> = batches.concat();
        assert_eq!(got.len(), want.len());
        assert_eq!(got, want, "write-then-read under multiple in-flight pipeline batches must be byte-identical");
        drop(pipe);
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// A single batch larger than a slot is split and submitted internally; read back identical.
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

    /// Backpressure (Error policy): after the depth fills, push returns Backpressure immediately with
    /// not a single byte committed; after flush, slots are reclaimed and submission can continue.
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
        // depth full: under the Error policy push does no implicit reap, so backpressure fires deterministically.
        assert!(matches!(pipe.push(&b(3)), Err(PipeError::Backpressure)));
        assert_eq!(pipe.offset(), off_before, "a batch rejected by backpressure must not have committed a single byte");
        // slots are reclaimed after flushing this batch; submission can continue.
        pipe.flush(t2).unwrap();
        assert_eq!(pipe.in_flight(), 0);
        let t3 = pipe.push(&b(3)).unwrap();
        assert!(t3 > t2 && t2 > t1, "batch tokens must increase monotonically");
        pipe.sync().unwrap();
        let got = std::fs::read(&p).unwrap();
        let mut want = b(1);
        want.extend_from_slice(&b(2));
        want.extend_from_slice(&b(3));
        assert_eq!(got, want);
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// Backpressure (Block policy): even depth 1 can submit many batches in a row (internally blocks to reclaim).
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

    /// Everything drains on close: no explicit flush/sync; the file contents are complete after Drop.
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
            assert!(pipe.in_flight() > 0, "there must still be in-flight batches before Drop");
        } // Drop: drain all in-flight SQEs
        assert_eq!(std::fs::read(&p).unwrap(), want, "Drop must drain all in-flight writes");
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// flush waits only for its own batch: old tokens return immediately once completed; resume offsets are correct.
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
            // watermark already passed: flushing an old token must return immediately (not waiting for any event).
            pipe.flush(t).unwrap();
        }
        // resume: 16 B already exist; a new push must land at offset 16.
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

    /// Default configuration: depth 64 (the SPEC-named default).
    #[test]
    fn pipeline_config_defaults() {
        let cfg = PipelineConfig::default();
        assert_eq!(cfg.max_in_flight, 64);
        assert_eq!(cfg.backpressure, BackpressurePolicy::Block);
    }
}
