//! Batch-committing WAL writer (v0.2): frames first go into a pending buffer and are
//! committed to the backend in whole batches per [`SyncPolicy`], with one fsync per group-commit boundary.
//!
//! The commit backend is abstracted as [`BatchSubmitter`]: the real backend is `IoUringSubmitter`
//! under feature `io-uring` (Linux only) (a whole batch of SQEs in one syscall);
//! tests use a mock backend to validate the batching/group-commit decision logic, independent
//! of whether the kernel allows io_uring.

use std::time::Instant;

use rti_core::{Result, SyncPolicy};

use super::{encode_frame, Record, WalWriter, FRAME_LEN};

/// Default pending-buffer cap (bytes): forces a commit when reached, keeping memory bounded.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024;

/// Batch-commit backend abstraction (synchronous blocking semantics: return means done).
///
/// Implementations must be safe (rti-wal is `#![forbid(unsafe_code)]`);
/// the unsafe io_uring glue is isolated in the `rti-wal-uring` crate.
pub trait BatchSubmitter {
    /// Write all of `buf` at `offset` of the file; returns once completed (at least inside the kernel).
    fn submit_write(&mut self, buf: &[u8], offset: u64) -> Result<()>;
    /// Group-commit flush boundary.
    fn submit_fsync(&mut self) -> Result<()>;
    /// Backend name (diagnostics/tests).
    fn name(&self) -> &'static str;
}

/// Batch-committing WAL writer: generic over the commit backend.
///
/// - `append`/`append_batch` only encode into pending (O(1), no syscalls);
/// - `SyncPolicy::Always`: commit + fsync on every append;
/// - `SyncPolicy::Group`: commit + fsync only when more than `interval_us` has elapsed since the last fsync;
/// - `SyncPolicy::None`: commit only when pending reaches [`DEFAULT_MAX_BATCH_BYTES`]
///   (no fsync); the crash window has the same semantics as the v0.1 Std backend.
pub struct BatchingWalWriter<S: BatchSubmitter> {
    submitter: S,
    /// Encoded but not-yet-committed frames.
    pending: Vec<u8>,
    /// Logical offset of the next record (= committed + pending occupied).
    offset: u64,
    /// Bytes committed to the backend (= file write position).
    committed: u64,
    sync: SyncPolicy,
    last_sync: Instant,
    max_batch: usize,
}

impl<S: BatchSubmitter> BatchingWalWriter<S> {
    /// Construct with a given backend; `start_offset` is the existing log length (append-resume scenario).
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

    /// Set the pending cap (at least one frame).
    pub fn with_max_batch_bytes(mut self, n: usize) -> Self {
        self.max_batch = n.max(FRAME_LEN);
        self
    }

    /// Current pending uncommitted bytes (diagnostics/tests).
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    /// Commit the whole pending batch (one backend call).
    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.submitter.submit_write(&self.pending, self.committed)?;
        self.committed += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Decide whether to commit/flush per the sync policy.
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
            // v0.5 in-flight pipeline: under the Error backpressure policy a pending commit may be
            // rejected because the in-flight depth is full. The durability boundary must still advance:
            // first submit_fsync to reap in-flight batches (a pipeline backend drains in-flight + fsyncs;
            // a synchronous backend fsyncs anyway), then retry this batch's commit once.
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
        // best-effort commit of leftover pending; errors are ignored in Drop.
        let _ = self.flush_pending();
    }
}

// ------------------------------------------------- io_uring backend (feature)

/// io_uring commit backend (feature `io-uring`, Linux only).
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

/// io_uring-backend WAL writer (feature `io-uring`, Linux only).
///
/// Returns `Err` when opening fails (`io_uring_setup` rejected by seccomp, kernel < 5.1, etc.);
/// for automatic backend selection use [`WalWriter::auto`] (falls back to Std when probing fails).
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub type IoUringWalWriter = BatchingWalWriter<IoUringSubmitter>;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl BatchingWalWriter<IoUringSubmitter> {
    /// Open (creating if missing) `path`; the existing content length is the resume point.
    ///
    /// `queue_depth` is fixed at 64: a single writer commits batch by batch, and 64 SQE slots far
    /// exceed the 'one submission per batch' need.
    pub fn open(path: impl AsRef<std::path::Path>, sync: SyncPolicy) -> Result<Self> {
        let path = path.as_ref();
        let start = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let file = rti_wal_uring::UringFile::open(path, 64)?;
        Ok(Self::with_submitter(IoUringSubmitter { file }, sync, start))
    }
}

// ------------------------------------- io_uring in-flight batch pipeline (v0.5 Stream B)

/// io_uring **in-flight batch pipeline** commit backend (feature `io-uring`, Linux only, v0.5).
///
/// Difference from [`IoUringSubmitter`] (synchronous blocking): `submit_write` returns **without
/// waiting** for completion after submitting SQEs (multiple batches in flight); `submit_fsync` (= the
/// group-commit boundary) waits only for the completion events of the currently submitted batches,
/// then fsyncs. Backpressure is configured via [`rti_wal_uring::PipelineConfig`]: when the in-flight
/// depth (default 64) is full, it returns [`rti_core::Error::Backpressure`] or blocks in reap.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub struct IoUringPipelineSubmitter {
    pipe: rti_wal_uring::UringPipeline,
}

/// Map pipeline errors to engine errors: backpressure → [`rti_core::Error::Backpressure`].
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
        debug_assert_eq!(offset, self.pipe.offset(), "WAL commit offsets must be continuous");
        // submit and return immediately (the batch enters the kernel in flight); durability is guaranteed by
        // submit_fsync, satisfying BatchSubmitter's 'return once completed (at least inside the kernel)' contract.
        self.pipe.push(buf).map_err(map_pipe_err)?;
        Ok(())
    }

    fn submit_fsync(&mut self) -> Result<()> {
        // wait only for the currently submitted batches (this group) to complete + fsync; the ring is not drained.
        self.pipe.sync().map_err(map_pipe_err)
    }

    fn name(&self) -> &'static str {
        "io_uring_pipeline"
    }
}

/// io_uring in-flight batch pipeline WAL writer (feature `io-uring`, Linux only, v0.5).
///
/// Returns `Err` when opening fails (`io_uring_setup` rejected by seccomp, kernel < 5.1, etc.);
/// automatic backend selection still goes through [`WalWriter::auto`] (preserving v0.2 behavior);
/// the pipeline backend must be opened explicitly.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub type IoUringPipelinedWalWriter = BatchingWalWriter<IoUringPipelineSubmitter>;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl BatchingWalWriter<IoUringPipelineSubmitter> {
    /// Open (creating if missing) `path`; the existing content length is the resume point.
    ///
    /// `cfg.max_in_flight` is the in-flight depth cap (default 64); `cfg.backpressure` is the policy
    /// when the depth is full; the pending batching cap is automatically aligned to the slot size.
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

    /// Current number of in-flight batches (diagnostics/tests).
    pub fn in_flight(&self) -> usize {
        self.submitter.pipe.in_flight()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock commit backend: an in-memory 'file' that records fsync counts and commit calls.
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

    /// Group commit: no commit and no fsync within the interval; sync_now commits the whole batch at once.
    #[test]
    fn group_commit_holds_batch_until_sync() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::Group { interval_us: 60_000_000 }, 0);
        let recs: Vec<Record> = (0..100).map(rec).collect();
        let first = w.append_batch(&recs).unwrap();
        assert_eq!(first, 0);
        assert_eq!(w.offset(), 100 * FRAME_LEN as u64);
        assert_eq!(w.pending_bytes(), 100 * FRAME_LEN, "must batch within the group-commit window");
        assert_eq!(w.submitter.write_calls, 0);
        assert_eq!(w.submitter.fsyncs, 0);

        w.sync_now().unwrap();
        assert_eq!(w.submitter.write_calls, 1, "the whole batch must be committed in one call");
        assert_eq!(w.submitter.fsyncs, 1);
        assert_eq!(w.pending_bytes(), 0);
        assert_eq!(w.submitter.records(), recs, "the mock file must be frame-by-frame decodable and consistent");
        assert_eq!(w.flush_policy(), SyncPolicy::Group { interval_us: 60_000_000 });
    }

    /// Always: one commit + one fsync per record.
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

    /// None: no fsync; pending is force-committed at the threshold (bounded memory).
    #[test]
    fn none_flushes_on_threshold_without_fsync() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::None, 0)
            .with_max_batch_bytes(FRAME_LEN * 4);
        for i in 0..10u32 {
            w.append(&rec(i)).unwrap();
        }
        assert_eq!(w.submitter.fsyncs, 0);
        assert_eq!(w.submitter.write_calls, 2, "commit once per 4 accumulated frames");
        assert_eq!(w.pending_bytes(), 2 * FRAME_LEN);
        // Drop best-effort flushes the remainder
        drop(w);
    }

    /// Batch commits have continuous offsets; backend errors propagate through append (fail-fast).
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
        assert!(w2.append(&rec(0)).is_err(), "backend write failures must propagate");
    }

    #[test]
    fn backend_name_and_start_offset() {
        let m = MockSubmitter::new();
        let mut w = BatchingWalWriter::with_submitter(m, SyncPolicy::None, 7 * FRAME_LEN as u64);
        assert_eq!(w.backend_name(), "mock");
        let off = w.append(&rec(0)).unwrap();
        assert_eq!(off, 7 * FRAME_LEN as u64, "the resume point must take effect");
        w.sync_now().unwrap();
        // frames must land at the start offset and be decodable
        let s = 7 * FRAME_LEN;
        let frame = super::super::decode_frame(&w.submitter.file[s..s + FRAME_LEN]).unwrap();
        assert_eq!(frame, rec(0));
    }
}

/// Real io_uring backend tests (feature on + Linux; skipped gracefully when rings are unavailable).
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

    /// Real ring: append + append_batch + sync, read back identically through the Std recovery path.
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

    /// Named by v0.5: ordering under the pipeline. Multiple batches in flight (more batches than the
    /// depth) written via IoUringPipelinedWalWriter, read back record-by-record identical through the Std recovery path.
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
            // 256 B slots / 28 B frames: 9 frames per slot; 2000 records >> depth 8.
            for i in 0..2000u32 {
                w.append(&Record::new(i, i as i64 * 7, i as f64 + 0.25)).unwrap();
            }
            assert!(w.in_flight() > 0, "under SyncPolicy::None there must be multiple batches in flight after commit");
            w.sync_now().unwrap();
            assert_eq!(w.in_flight(), 0, "sync_now must wait for this whole group to complete");
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

    /// Named by v0.5: backpressure triggering. Error policy + depth 2: append returns
    /// Error::Backpressure when in-flight is full; after sync_now reaps, writing continues with data intact.
    #[test]
    fn pipelined_backpressure_surfaces_as_error() {
        let d = tmpdir("pipe-bp");
        let p = d.join("wal.log");
        let cfg = rti_wal_uring::PipelineConfig {
            max_in_flight: 2,
            slot_bytes: 2 * FRAME_LEN, // 2 frames per batch trigger a commit
            backpressure: rti_wal_uring::BackpressurePolicy::Error,
        };
        let mut w = match IoUringPipelinedWalWriter::open(&p, SyncPolicy::None, cfg) {
            Ok(w) => w,
            Err(_) => return, // skip gracefully when the environment does not support it
        };
        // slot = 2 frames: appends 1/3 each trigger a commit -> depth 2 fills the in-flight slots.
        for i in 0..5u32 {
            w.append(&Record::new(i, i as i64, 1.0)).unwrap();
        }
        assert_eq!(w.in_flight(), 2);
        // append 5 fills pending with 2 frames and triggers a third commit: the Error policy does no
        // implicit reap, so backpressure fires deterministically.
        let err = w.append(&Record::new(5, 5, 1.0));
        assert!(
            matches!(err, Err(rti_core::Error::Backpressure)),
            "a full in-flight depth must return Backpressure, got {err:?}"
        );
        // sync_now reaps in-flight batches; the rejected batch stays in pending and is fully persisted on retry.
        w.sync_now().unwrap();
        w.append(&Record::new(6, 6, 1.0)).unwrap();
        w.sync_now().unwrap();
        drop(w);
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 7, "backpressure rejection loses no data: pending is kept for retry");
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.series, i as u32);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.5: everything drains on close. No explicit sync_now; read-back is complete after Drop.
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
            assert!(w.in_flight() > 0, "there must still be in-flight batches before Drop");
        } // Drop: flush pending + drain all in-flight
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 100, "Drop must drain all in-flight batches");
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.series, i as u32);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.5: pipeline backend group-commit semantics match v0.2 (batching within the Group window,
    /// one group commit at sync_now); resuming does not overwrite historical frames.
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
            assert_eq!(off, 3 * FRAME_LEN as u64, "the resume point must take effect");
            assert_eq!(w.pending_bytes(), FRAME_LEN, "must batch within the Group window");
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[3], Record::new(9, 90, 9.0));
        std::fs::remove_dir_all(&d).ok();
    }

    /// Resume: the existing log length is the start offset; historical frames are not overwritten.
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
