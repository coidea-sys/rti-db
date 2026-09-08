//! rti-wal: write-ahead log.
//!
//! Frame format (little-endian, fixed 28 bytes + CRC, 32 bytes total):
//!
//! ```text
//! ┌──────────┬────────┬───────────┬───────────┬─────────┐
//! │ len u32  │ series │ ts   i64  │ value f64 │ crc u32 │
//! │ (=24)    │  u32   │           │  (bits)   │ (frame body)  │
//! └──────────┴────────┴───────────┴───────────┴─────────┘
//! ```
//!
//! - append-only: append only, single writer;
//! - CRC32 covers len and the frame body; on recovery, a CRC mismatch or partial frame means
//!   **truncate** — all records before it are valid;
//! - group commit: [`SyncPolicy`] decides when to `sync_data`.
//!
//! v0.2: the write path is abstracted as the [`WalWriter`] trait — [`StdWalWriter`]
//! (the v0.1 default, which [`Wal`] delegates to internally with an unchanged API) and, under feature
//! `io-uring` (Linux only), `IoUringWalWriter` (batched SQE submission + group commit);
//! `WalWriter::auto` falls back to Std gracefully when probing fails.
//!
//! v0.5: under feature `io-uring`, adds [`IoUringPipelinedWalWriter`] — an io_uring **in-flight
//! batch pipeline**: no waiting after submitting SQEs, an incremental CQE reap loop, `sync_now`
//! waits only for this group's completion events + fsync; when the in-flight depth (default 64)
//! is full, returns [`Error::Backpressure`] or blocks per configuration. `WalWriter::auto` behavior
//! is unchanged. The unsafe io_uring glue is isolated in the `rti-wal-uring` crate; this crate
//! stays `#![forbid(unsafe_code)]`.

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

/// One WAL record: series id + sample point.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Record {
    /// Series id.
    pub series: SeriesId,
    /// Sample point.
    pub sample: Sample,
}

impl Record {
    /// Construct a record.
    pub fn new(series: SeriesId, ts: Timestamp, value: f64) -> Self {
        Self { series, sample: Sample { ts, value } }
    }
}

/// Frame-body bytes after the length field: series(4) + ts(8) + value(8).
const BODY_LEN: u32 = 20;
/// Whole-frame bytes: len(4) + body(20) + crc(4).
const FRAME_LEN: usize = 28;

fn encode_frame(rec: &Record, out: &mut [u8; FRAME_LEN]) {
    out[0..4].copy_from_slice(&BODY_LEN.to_le_bytes());
    out[4..8].copy_from_slice(&rec.series.to_le_bytes());
    out[8..16].copy_from_slice(&rec.sample.ts.to_le_bytes());
    out[16..24].copy_from_slice(&rec.sample.value.to_bits().to_le_bytes());
    let crc = crc32fast::hash(&out[0..24]);
    out[24..28].copy_from_slice(&crc.to_le_bytes());
}

/// Checkpoint temporary-file path (`wal.log` → `wal.tmp`).
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

/// WAL write-backend abstraction (v0.2).
///
/// The v0.1 std `File` implementation is kept as [`StdWalWriter`] (default backend);
/// under feature `io-uring` (Linux only) there is also [`IoUringWalWriter`]
/// (batched SQE submission + group-commit fsync).
///
/// Backend selection uses [`WalWriter::auto`]: on probe failure (old kernel / seccomp sandbox /
/// feature disabled) it automatically falls back to Std — never panics.
pub trait WalWriter {
    /// Append one record, returning its byte offset. Amortized O(1).
    fn append(&mut self, rec: &Record) -> Result<u64>;

    /// Batch append, returning the byte offset of the **first** record.
    ///
    /// The default implementation appends one by one via [`WalWriter::append`]; io_uring backends
    /// override it to 'encode the whole batch into the same pending buffer, one SQE group commit'.
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

    /// Immediate durability boundary: flush buffers + fsync (called at group boundaries of group commit / before close).
    fn sync_now(&mut self) -> Result<()>;

    /// Current write offset (i.e. logical file length).
    fn offset(&self) -> u64;

    /// Sync policy in effect for this writer (compatible with v0.1 [`Wal`] semantics).
    fn flush_policy(&self) -> SyncPolicy;

    /// Backend name (diagnostics/tests): `"std"` or `"io_uring"`.
    fn backend_name(&self) -> &'static str;
}

impl dyn WalWriter {
    /// Automatic backend selection: prefer io_uring (feature enabled + Linux + kernel allows),
    /// falling back gracefully to [`StdWalWriter`] when probing fails.
    ///
    /// Set environment variable `RTI_WAL_FORCE_STD=1` to force the fallback (test/deployment escape hatch).
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

/// WAL writer over std `File` + `BufWriter` (v0.1 default backend, semantics unchanged).
pub struct StdWalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    sync: SyncPolicy,
    /// Offset of the next record.
    offset: u64,
    last_sync: Instant,
    /// Staging buffer for batch encoding (reused, avoiding per-batch allocation).
    scratch: Vec<u8>,
}

impl StdWalWriter {
    /// Open (creating if missing) `path` and seek to the end; existing contents are treated as valid historical records.
    ///
    /// v0.6: also cleans up temporary files (`wal.tmp`) that a crash mid-checkpoint may have left —
    /// the old WAL is intact when the crash happens before rename, so the temporary file can simply be deleted.
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

    /// File path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// v0.6: batch append — encode the whole batch into the staging buffer, one `write_all`,
    /// then a single [`StdWalWriter::maybe_sync`] check.
    ///
    /// Semantically equivalent to per-record [`WalWriter::append`] (same frame format, same policy),
    /// but amortizes the per-record `write_all` + clock read into one per batch;
    /// under [`SyncPolicy::Always`] the whole batch syncs once (callers treat a batch as a group boundary).
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

    /// v0.6: flush only when sync is due (Group: >= interval since the last sync).
    ///
    /// Group-commit call sites (ingest batch boundaries) use this instead of unconditional `sync_now`:
    /// fsync frequency is governed by the policy interval, not the batch size.
    /// Equivalent to `sync_now` under `Always`; a no-op under `None`.
    pub fn sync_if_due(&mut self) -> Result<()> {
        self.maybe_sync()
    }

    /// v0.6 WAL checkpoint: truncate the WAL, keeping only the records in `keep`.
    ///
    /// **The caller must guarantee** that all records outside `keep` are already covered by segments
    /// and persisted (rti-db calls this after the MemTable seal succeeds and the segment is fsynced;
    /// `keep` holds the records that entered the new MemTable after the seal point and still need WAL protection).
    ///
    /// Crash safety: `keep` is first fully encoded into `wal.tmp` and fsynced, then atomically
    /// renamed over `wal.log`, and finally the directory is fsynced. Crash before rename → the old WAL
    /// is intact (covered records duplicate the segments; recovery deduplicates by ts); crash after rename →
    /// the new WAL (only `keep`) + persisted segments — no loss in either direction.
    /// The offset resets to `keep`'s byte length; subsequent appends follow it.
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
            // fsync is skipped under SyncPolicy::None ('no flushing' semantics):
            // process-crash safety comes from the page cache; no paying for durability that was never requested.
            if self.sync != SyncPolicy::None {
                f.sync_data()?;
            }
        }
        std::fs::rename(&tmp, &self.path)?;
        if self.sync != SyncPolicy::None {
            if let Some(dir) = self.path.parent() {
                // make the rename's directory entry durable (on Linux a directory can be opened read-only).
                if let Ok(d) = File::open(dir) {
                    let _ = d.sync_data();
                }
            }
        }
        let file = OpenOptions::new().read(true).append(true).open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.offset = (keep.len() * FRAME_LEN) as u64;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// v0.6: flush bytes that are buffered but not yet given to the OS into the kernel (no fsync).
    ///
    /// Called at ingest batch boundaries: guarantees 'applied' records have at least left the process
    /// address space — on a process crash (kill -9) the OS page cache still holds them; this is the
    /// crash-safety basis of `put_durable` under the Group/None tiers; machine power-loss semantics
    /// are still governed by the [`SyncPolicy`] fsync frequency.
    pub fn flush_os(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Decide whether to fsync per the sync policy.
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
        // best-effort flush; errors ignored in Drop.
        let _ = self.writer.flush();
    }
}

/// Write-ahead log (append-only, single writer).
///
/// The v0.1 public API is unchanged; internally delegates to [`StdWalWriter`].
pub struct Wal {
    inner: StdWalWriter,
}

impl Wal {
    /// Open (creating if missing) `path` and seek to the end; existing contents are treated as valid historical records.
    pub fn open(path: impl AsRef<Path>, sync: SyncPolicy) -> Result<Self> {
        Ok(Self { inner: StdWalWriter::open(path, sync)? })
    }

    /// Append one record, returning its byte offset.
    ///
    /// O(1): writes only to a sequential buffer; durability is governed by [`SyncPolicy`].
    pub fn append(&mut self, rec: &Record) -> Result<u64> {
        self.inner.append(rec)
    }

    /// Flush the buffer and `sync_data` immediately (group boundary of group commit / before close).
    pub fn sync_now(&mut self) -> Result<()> {
        self.inner.sync_now()
    }

    /// v0.6: batch append (whole batch encoded once and written once, sync checked per policy). Same semantics as per-record [`Wal::append`].
    pub fn append_batch(&mut self, recs: &[Record]) -> Result<()> {
        self.inner.append_batch_fast(recs)
    }

    /// v0.6: fsync only when due (called at group-commit batch boundaries; fsync frequency governed by the interval).
    pub fn sync_if_due(&mut self) -> Result<()> {
        self.inner.sync_if_due()
    }

    /// v0.6 WAL checkpoint: truncate the WAL, keeping only the records in `keep`
    /// (the caller guarantees all other records are covered by segments and persisted).
    pub fn checkpoint_keep(&mut self, keep: &[Record]) -> Result<()> {
        self.inner.checkpoint_keep(keep)
    }

    /// v0.6: flush user-space buffers to the OS (no fsync); called at ingest batch boundaries.
    pub fn flush_os(&mut self) -> Result<()> {
        self.inner.flush_os()
    }

    /// Current write offset (i.e. logical file length).
    pub fn offset(&self) -> u64 {
        self.inner.offset()
    }

    /// File path.
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Crash recovery: sequential scan returning an iterator over valid records.
    ///
    /// Stops at a CRC mismatch, illegal length field, or partial frame — the 'truncate at corruption' semantics:
    /// bytes after the corruption point (crash-torn writes) are ignored. The whole file is pre-read into
    /// memory; iteration itself is zero-copy and zero-allocation.
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

/// WAL recovery iterator: yields valid records frame by frame, stopping at corruption.
pub struct RecoverIter {
    buf: Vec<u8>,
    pos: usize,
}

impl RecoverIter {
    /// Byte offset of the corrupt frame (if terminated due to corruption).
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
            // partial frame: treat as truncated here
            return None;
        }
        match decode_frame(&self.buf[self.pos..self.pos + FRAME_LEN]) {
            Ok(rec) => {
                self.pos += FRAME_LEN;
                Some(rec)
            }
            Err(()) => None, // CRC corruption: truncate
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

    /// Named by SPEC §5: CRC-corruption truncation recovery test.
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
        // corrupt one payload byte of the 3rd record (offset = 2*FRAME_LEN)
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start((2 * FRAME_LEN + 10) as u64)).unwrap();
            f.write_all(&[0xAB]).unwrap();
        }
        let mut it = Wal::recover(&p).unwrap();
        let recs: Vec<Record> = it.by_ref().collect();
        assert_eq!(recs.len(), 2, "records after the corruption point must be truncated");
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
        // simulate a crash-torn write: append half a frame of garbage
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

    /// Environment variables are process-global state; tests touching them must run serially.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// v0.1 API unchanged: Wal delegates to StdWalWriter, byte-for-byte identical semantics.
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

    /// Use StdWalWriter through a trait object (v0.2 polymorphic write path).
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

    /// Named by SPEC: degradation-path test. `RTI_WAL_FORCE_STD=1` forces the Std fallback,
    /// simulating io_uring being unavailable (seccomp sandbox / old kernel / feature disabled);
    /// after the fallback, writing and reading must work fully.
    #[test]
    fn auto_falls_back_to_std_when_forced() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("RTI_WAL_FORCE_STD", "1");
        let d = tmpdir("fallback");
        let p = d.join("wal.log");
        let res = <dyn WalWriter>::auto(&p, SyncPolicy::Always);
        std::env::remove_var("RTI_WAL_FORCE_STD");
        let mut w = res.unwrap();
        assert_eq!(w.backend_name(), "std", "must be the Std backend after the forced fallback");
        w.append(&Record::new(1, 1, 1.0)).unwrap();
        w.sync_now().unwrap();
        drop(w);
        assert_eq!(Wal::recover(&p).unwrap().count(), 1);
        std::fs::remove_dir_all(&d).ok();
    }

    /// When not forced: feature on + kernel allows → io_uring; otherwise a graceful std fallback.
    /// Both outcomes must be writable and readable (never panic / never Err).
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
        assert_eq!(w.backend_name(), "std", "no feature / non-Linux must fall back to Std");
        for i in 0..4u32 {
            w.append(&Record::new(i, i as i64, i as f64)).unwrap();
        }
        w.sync_now().unwrap();
        drop(w);
        assert_eq!(Wal::recover(&p).unwrap().count(), 4);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6: checkpoint truncation — after truncation the offset is zero, old records are unrecoverable,
    /// new writes append normally and recovery sees only new records.
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
            assert_eq!(w.offset(), 0, "offset resets to zero after checkpoint");
            assert_eq!(std::fs::metadata(&p).unwrap().len(), 0, "file is empty after checkpoint");
            // keep writing after truncation
            for i in 100..105u32 {
                w.append(&Record::new(i, i as i64, i as f64)).unwrap();
            }
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 5, "recovery must only see records after the checkpoint");
        assert_eq!(recs[0], Record::new(100, 100, 100.0));
        assert_eq!(recs[4], Record::new(104, 104, 104.0));
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6: checkpoint keeps the tail — records that entered the new MemTable after the seal point
    /// must stay in the WAL; truncation only discards the prefix already covered by segments.
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
            // the first 50 records are covered by segments; keep 50..53
            w.checkpoint_keep(&tail).unwrap();
            assert_eq!(w.offset(), 3 * FRAME_LEN as u64);
            // new records appended after truncation follow the retained tail
            w.append(&Record::new(60, 60, 60.0)).unwrap();
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 4, "retained tail of 3 + 1 newly appended");
        assert_eq!(recs[0], Record::new(50, 50, 50.0));
        assert_eq!(recs[3], Record::new(60, 60, 60.0));
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6: simulated crash mid-checkpoint — a wal.tmp is left before rename,
    /// the old WAL intact; reopening cleans up the temporary file with no data loss.
    #[test]
    fn stale_checkpoint_tmp_is_cleaned_on_open() {
        let d = tmpdir("checkpoint-stale");
        let p = d.join("wal.log");
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            w.append(&Record::new(1, 10, 1.5)).unwrap();
            w.sync_now().unwrap();
        }
        // simulate a crash before rename: leave a non-empty wal.tmp
        std::fs::write(d.join("wal.tmp"), b"garbage").unwrap();
        {
            let mut w = Wal::open(&p, SyncPolicy::Always).unwrap();
            assert!(!d.join("wal.tmp").exists(), "open must clean up leftover temporary files");
            w.append(&Record::new(2, 20, 2.5)).unwrap();
            w.sync_now().unwrap();
        }
        let recs: Vec<Record> = Wal::recover(&p).unwrap().collect();
        assert_eq!(recs.len(), 2, "the old WAL is unaffected by leftover temporary files");
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6: batch append and per-record append produce byte-identical frame formats.
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
        assert_eq!(std::fs::read(&p1).unwrap(), std::fs::read(&p2).unwrap(), "batch and per-record encodings must be identical");
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
