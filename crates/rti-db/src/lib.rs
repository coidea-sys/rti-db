//! rti-db facade: `Db::open` / `put` / `scan`.
//!
//! Write path (SPEC §4.1): `put` performs a single lock-free SPSC enqueue (~20ns);
//! a background ingest thread dequeues in batches → batch WAL append → group commit
//! per [`SyncPolicy`] → write into the MemTable; when full, the MemTable is sealed into a columnar segment.
//!
//! Read path (SPEC §4.2): `scan` first `flush`es to guarantee read-your-writes, then
//! zone maps skip irrelevant segments and predicates are pushed down to the decode layer;
//! result buffers come from an object pool, so steady-state scans perform 0 mallocs.

#![forbid(unsafe_code)]

//! ## v0.3: deterministic profile and mirroring
//!
//! Under [`Profile::Deterministic`]: forces [`SyncPolicy::None`], runs purely in memory
//! (no directories created, no WAL opened, no segments written — the file system is
//! never touched even when `data_dir = Some`); when the MemTable is full the oldest series
//! is evicted by LRU and counted ([`Db::lru_evictions`]); [`Mirror`] sends a 20-byte mirror
//! datagram over non-blocking UDP on every successful `put` enqueue (best-effort: failures
//! are only counted, see [`Db::mirror_stats`]).

use std::collections::BTreeMap;
use std::fs;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rti_buffer::SpscRing;
use rti_core::{Config, Error, Profile, Result, Sample, SeriesId, SyncPolicy, Timestamp};
use rti_query::{Agg, Pred, ScanSource};
use rti_store::{ColdTier, MemTable, SegmentReader, SegmentWriter, ZoneMap};
use rti_wal::{Record, Wal};

mod compact;
mod latest;

pub use compact::CompactionStats;
pub use latest::{latest, LatestIndex};

#[cfg(feature = "alloc-count")]
pub use rti_mem::alloc_count::{alloc_count, CountingAllocator};

/// Default capacity of the ingest ring (power of two).
const RING_CAP: usize = 1 << 16;
/// Maximum records the ingest thread processes per batch (v0.6 dynamic batching: each wakeup
/// drains as much of the ring as possible, chunked by this cap; fsync frequency is governed by the SyncPolicy interval, not the batch size).
const BATCH_MAX: usize = 8192;
/// Maximum number of series slots in the MemTable.
const MAX_SERIES: usize = 4096;

/// Internal shared state (shared by the ingest thread and query threads).
struct Shared {
    state: Mutex<DbState>,
    /// Enqueue counter (+1 after a successful put; the base of the next watermark ticket).
    enqueued: AtomicU64,
    /// Counter of records applied by the ingest thread (= durable watermark, v0.6).
    acked: AtomicU64,
    /// Fatal ingest-thread error (put/scan fail once set).
    err: Mutex<Option<String>>,
    /// Scan result buffer pool (reused in steady state, 0 malloc).
    buf_pool: Arc<Mutex<Vec<Vec<Sample>>>>,
    /// Segment file sequence number.
    seg_seq: Mutex<u64>,
    /// Cold-tier storage (v0.4; injected via `set_cold_tier`).
    cold: Mutex<Option<Arc<dyn ColdTier>>>,
    /// Per-series O(1) latest index (v0.8). Kept in its own mutex so `latest()` never
    /// blocks on the WAL/segment I/O performed under `state`.
    latest: Mutex<LatestIndex>,
    /// Cumulative compaction counters (v0.8, SPEC §1).
    compact_stats: Mutex<CompactionStats>,
    /// Serializes concurrent `compact*()` calls (ingest and scans never take this lock).
    compact_lock: Mutex<()>,
    config: Config,
}

/// Segment location: local (parsed in memory) or cold tier (fetched on demand and cached).
enum SegLoc {
    /// Local segment (v0.1 semantics: fully read into memory at open).
    Local(SegmentReader),
    /// Archived to the cold tier; the `Option` is the transparent read-back cache (kept in memory after the first hit).
    Archived(Option<SegmentReader>),
}

/// One segment record in the catalog (v0.4 cold tiering).
struct SegEntry {
    /// Segment file name (cold-tier object name).
    name: String,
    series: SeriesId,
    /// zone map: kept in the catalog after archiving, so scans can still skip whole segments.
    zone: ZoneMap,
    loc: SegLoc,
}

impl SegEntry {
    fn local(name: String, reader: SegmentReader) -> Self {
        Self { name, series: reader.series(), zone: reader.zone_map(), loc: SegLoc::Local(reader) }
    }

    fn is_archived(&self) -> bool {
        matches!(self.loc, SegLoc::Archived(_))
    }
}

struct DbState {
    /// `Some` under Balanced; `None` under Deterministic (pure in-memory operation).
    wal: Option<Wal>,
    mem: MemTable,
    segments: Vec<SegEntry>,
}

/// Mirror statistics (v0.3): UDP mirror datagram send counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MirrorStats {
    /// Datagrams successfully handed to the socket.
    pub sent: u64,
    /// Datagrams that failed to send (non-blocking refusal/error).
    pub failed: u64,
}

/// Database handle. `Send + Sync`, shareable across threads (e.g. the rti-net server).
///
/// `put` enqueues via `Mutex<SpscProducer>`: with a single writer (the recommended deployment)
/// the lock is uncontended and costs about the same as a lock-free enqueue; with concurrent writers it degrades to a short (bounded) critical section.
pub struct Db {
    shared: Arc<Shared>,
    producer: Mutex<rti_buffer::SpscProducer<Record>>,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    /// Non-blocking UDP mirror socket (when `Some`, a mirror datagram is sent after each successful put enqueue).
    mirror: Option<UdpSocket>,
    mirror_sent: AtomicU64,
    mirror_failed: AtomicU64,
}

impl Db {
    /// Open (or create) the database: create directories → load existing segments → WAL crash recovery.
    ///
    /// Under [`Profile::Deterministic`] all file-system operations are skipped (pure in-memory),
    /// and [`SyncPolicy::None`] is forced; under [`Profile::Balanced`]
    /// `config.data_dir` must be `Some`.
    pub fn open(config: Config) -> Result<Db> {
        let deterministic = config.profile == Profile::Deterministic;
        // The deterministic profile forces SyncPolicy::None (no WAL; flushing is a no-op).
        let mut config = config;
        if deterministic {
            config.wal_sync = SyncPolicy::None;
        }

        let (wal, segments, seg_seq, latest) = if deterministic {
            // Pure in-memory: no directories, no WAL, no segment loading.
            (None, Vec::new(), 0, LatestIndex::new())
        } else {
            let dir = config
                .data_dir
                .as_ref()
                .ok_or_else(|| Error::Corrupt("Balanced profile requires data_dir = Some(..)".into()))?;
            fs::create_dir_all(dir)?;

            // Load existing segments (file-name order == time order)
            let mut segments = Vec::new();
            // v0.8 SPEC §1 pre-fix: the next sequence number is max(parsed NNNNNN) + 1 over both
            // local segment names and archive.catalog names — never the file count, which would
            // reuse numbers once compaction or archiving deletes local files.
            let mut seg_seq = 0u64;
            // v0.8: rebuild the per-series latest index by decoding each pre-read segment
            // forward once (SPEC §3; archived catalog entries are not pre-read, see Db::latest).
            let mut latest = LatestIndex::new();
            let mut names: Vec<PathBuf> = fs::read_dir(dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
                .collect();
            names.sort();
            for p in names {
                let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                if let Some(n) = parse_seg_seq(&name) {
                    seg_seq = seg_seq.max(n + 1);
                }
                let reader = SegmentReader::open(&p)?;
                latest.apply_segment(&reader)?;
                segments.push(SegEntry::local(name, reader));
            }
            // Read back the archive catalog: cold-tier segments are registered as Archived (lazy read-back).
            // If a same-named file still exists locally (crash between archive and delete), the local one wins and the entry is skipped.
            for entry in load_catalog(dir)? {
                // Cataloged names also advance the sequence (collision with cold-tier object keys).
                if let Some(n) = parse_seg_seq(&entry.name) {
                    seg_seq = seg_seq.max(n + 1);
                }
                if !segments.iter().any(|e: &SegEntry| e.name == entry.name) {
                    segments.push(entry);
                }
            }

            let wal = Wal::open(dir.join("wal.log"), config.wal_sync)?;
            (Some(wal), segments, seg_seq, latest)
        };

        let shared = Arc::new(Shared {
            state: Mutex::new(DbState { wal, mem: MemTable::new(config.memtable_max, MAX_SERIES), segments }),
            enqueued: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            err: Mutex::new(None),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            seg_seq: Mutex::new(seg_seq),
            cold: Mutex::new(None),
            latest: Mutex::new(latest),
            compact_stats: Mutex::new(CompactionStats::default()),
            compact_lock: Mutex::new(()),
            config: config.clone(),
        });

        // WAL crash recovery: replay directly into the memtable (sealing midway if necessary)
        if let Some(dir) = config.data_dir.as_ref().filter(|_| !deterministic) {
            let mut state = shared.state.lock().unwrap();
            for rec in Wal::recover(dir.join("wal.log"))? {
                if state.mem.is_full() {
                    // WAL checkpointing is forbidden during recovery replay (the replay has not finished reading).
                    seal_memtable(&shared, &mut state)?;
                }
                state.mem.insert(rec.series, rec.sample)?;
                // v0.8: recovered samples become visible to latest() as they are applied.
                shared.latest.lock().unwrap().apply(rec.series, rec.sample);
            }
        }

        // Mirror socket: non-blocking; configuration errors (bind failure, etc.) are reported at open.
        let mirror = match &config.mirror {
            Some(m) => {
                let sock = UdpSocket::bind("0.0.0.0:0")?;
                sock.connect(m.addr)?;
                sock.set_nonblocking(true)?;
                Some(sock)
            }
            None => None,
        };

        let ring = SpscRing::with_capacity(RING_CAP);
        let (producer, consumer) = ring.split();
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker = {
            let shared = Arc::clone(&shared);
            let shutdown = Arc::clone(&shutdown);
            std::thread::Builder::new()
                .name("rti-ingest".into())
                .spawn(move || ingest_loop(shared, consumer, shutdown))
                .map_err(Error::Io)?
        };
        Ok(Db {
            shared,
            producer: Mutex::new(producer),
            shutdown,
            worker: Some(worker),
            mirror,
            mirror_sent: AtomicU64::new(0),
            mirror_failed: AtomicU64::new(0),
        })
    }

    /// Write one sample: lock-free SPSC enqueue, O(1), no heap allocation.
    ///
    /// Returns [`Error::SeriesFull`] as a backpressure signal when the ring is full (consumption cannot keep up).
    /// When [`rti_core::Mirror`] is configured, a 20-byte mirror datagram is sent over non-blocking
    /// UDP on every successful enqueue (best-effort: failures are only counted; never blocks,
    /// never retries, never affects the return value).
    pub fn put(&self, series: SeriesId, sample: Sample) -> Result<()> {
        self.check_err()?;
        // The placeholder count and the enqueue happen in the same critical section: the sequence number
        // (watermark ticket) matches ring order — the correctness basis of put_durable's wait semantics; the lock is uncontended with a single writer.
        let prod = self.producer.lock().unwrap();
        self.shared.enqueued.fetch_add(1, Ordering::SeqCst);
        match prod.push(Record { series, sample }) {
            Ok(()) => {
                drop(prod);
                self.mirror_send(series, &sample);
                Ok(())
            }
            Err(_) => {
                self.shared.enqueued.fetch_sub(1, Ordering::SeqCst);
                Err(Error::SeriesFull)
            }
        }
    }

    /// Write one sample and **block until it is persisted** (v0.6, no loss on crash). Enqueues first
    ///
    /// like [`Db::put`] (low-latency path unchanged), then waits for the ingest thread to complete
    /// "apply MemTable + WAL append" (whether fsync is included depends on the current
    /// [`SyncPolicy`]: `Always` fsyncs every record; `Group` is bounded by the group-commit window —
    /// on process crash the data already in the OS page cache survives, while machine power-loss
    /// semantics are bounded by the interval; `None` never fsyncs). Returns the durable watermark
    /// sequence number (>= this record's number, monotonically increasing). Returns [`Error::Timeout`]
    ///
    /// on timeout, but **no data is lost**: the record is still in the ingest pipeline and will be
    /// persisted shortly; the caller may re-check later with [`Db::durable_watermark`].
    /// A full ring returns [`Error::SeriesFull`] (backpressure; not enqueued, retryable).
    pub fn put_durable(&self, series: SeriesId, sample: Sample, timeout: Duration) -> Result<u64> {
        self.check_err()?;
        let ticket = {
            let prod = self.producer.lock().unwrap();
            let t = self.shared.enqueued.fetch_add(1, Ordering::SeqCst) + 1;
            match prod.push(Record { series, sample }) {
                Ok(()) => t,
                Err(_) => {
                    self.shared.enqueued.fetch_sub(1, Ordering::SeqCst);
                    return Err(Error::SeriesFull);
                }
            }
        };
        self.mirror_send(series, &sample);
        let deadline = Instant::now() + timeout;
        loop {
            let wm = self.shared.acked.load(Ordering::SeqCst);
            if wm >= ticket {
                return Ok(wm);
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            self.check_err()?;
            std::thread::yield_now();
        }
    }

    /// Current durable watermark (v0.6): total number of records the ingest thread has
    /// "applied to MemTable + WAL-appended" (by enqueue sequence number), monotonically increasing.
    ///
    /// When the watermark returned by [`Db::put_durable`] is >= a record's sequence number, that
    /// record is persisted (fsync or not depends on [`SyncPolicy`], see above).
    pub fn durable_watermark(&self) -> u64 {
        self.shared.acked.load(Ordering::SeqCst)
    }

    /// Mirror send: 20-byte little-endian datagram (series | ts | value bits).
    ///
    /// Non-blocking socket; any failure is only counted — mirroring stays out of the hot-path latency budget.
    #[inline]
    fn mirror_send(&self, series: SeriesId, sample: &Sample) {
        if let Some(sock) = &self.mirror {
            let mut pkt = [0u8; 20];
            pkt[0..4].copy_from_slice(&series.to_le_bytes());
            pkt[4..12].copy_from_slice(&sample.ts.to_le_bytes());
            pkt[12..20].copy_from_slice(&sample.value.to_bits().to_le_bytes());
            match sock.send(&pkt) {
                Ok(_) => self.mirror_sent.fetch_add(1, Ordering::Relaxed),
                Err(_) => self.mirror_failed.fetch_add(1, Ordering::Relaxed),
            };
        }
    }

    /// Mirror statistics (v0.3): send success/failure counters.
    pub fn mirror_stats(&self) -> MirrorStats {
        MirrorStats {
            sent: self.mirror_sent.load(Ordering::Relaxed),
            failed: self.mirror_failed.load(Ordering::Relaxed),
        }
    }

    /// Total number of series evicted by LRU under the Deterministic profile (v0.3; always 0 under Balanced).
    pub fn lru_evictions(&self) -> u64 {
        self.shared.state.lock().unwrap().mem.lru_evictions()
    }

    /// Inject cold-tier storage (v0.4). Archiving and transparent read-back both go through this handle.
    pub fn set_cold_tier(&self, tier: Arc<dyn ColdTier>) {
        *self.shared.cold.lock().unwrap() = Some(tier);
    }

    /// Number of segments archived to the cold tier (v0.4).
    pub fn archived_segment_count(&self) -> usize {
        self.shared.state.lock().unwrap().segments.iter().filter(|e| e.is_archived()).count()
    }

    /// Cold-tier archiving (v0.4): move local segments whose zone.max_ts is strictly before `ts`
    /// into the cold tier — upload the bytes, delete the local file, update the catalog.
    ///
    /// Afterwards `scan` reads those time ranges back **transparently** (fetched from the cold tier and
    /// cached on hit; the zone map stays in the catalog, so predicate-based whole-segment skipping never triggers a read-back).
    ///
    /// Crash-safety note: a crash between upload and delete leaves both the local file and the cold-tier
    /// copy; open deduplicates in favor of the local one (scan results are unaffected — scans deduplicate by ts anyway).
    ///
    /// Errors: returns `Err` under the Deterministic profile (no file system) or when no cold tier was injected.
    pub fn archive_older_than(&self, ts: Timestamp) -> Result<usize> {
        if self.shared.config.profile == Profile::Deterministic {
            return Err(Error::Corrupt("archive is unavailable in Deterministic profile".into()));
        }
        let tier = self
            .shared
            .cold
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::Corrupt("no cold tier configured; call set_cold_tier first".into()))?;
        self.flush()?;
        let dir = self
            .shared
            .config
            .data_dir
            .as_ref()
            .ok_or_else(|| Error::Corrupt("archive requires data_dir = Some(..)".into()))?
            .clone();
        let mut state = self.shared.state.lock().unwrap();
        let mut catalog_append = String::new();
        let mut n = 0usize;
        for e in state.segments.iter_mut() {
            if e.is_archived() || e.zone.max_ts >= ts {
                continue;
            }
            let path = dir.join(&e.name);
            let data = fs::read(&path)?;
            tier.put_segment(&e.name, &data)?;
            fs::remove_file(&path)?;
            catalog_append.push_str(&format_catalog_line(e));
            e.loc = SegLoc::Archived(None);
            n += 1;
        }
        if n > 0 {
            use std::io::Write;
            let mut f = fs::OpenOptions::new().create(true).append(true).open(dir.join(CATALOG_FILE))?;
            f.write_all(catalog_append.as_bytes())?;
            f.sync_data()?;
        }
        Ok(n)
    }

    /// Block until all currently enqueued records are flushed to disk (WAL sync) and visible.
    pub fn flush(&self) -> Result<()> {
        self.check_err()?;
        loop {
            let e = self.shared.enqueued.load(Ordering::SeqCst);
            let a = self.shared.acked.load(Ordering::SeqCst);
            if a >= e {
                break;
            }
            self.check_err()?;
            std::thread::yield_now();
        }
        let mut state = self.shared.state.lock().unwrap();
        if let Some(wal) = &mut state.wal {
            wal.sync_now()?;
        }
        Ok(())
    }

    /// Scan samples of `series` within `[t0, t1]`.
    ///
    /// `pred` is pushed down to the decode layer; when `agg` is `Some`, returns a single-sample iterator
    /// (`ts = t0`, `value = aggregate result`). Semantics match [`rti_query::scan`].
    pub fn scan(
        &self,
        series: SeriesId,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<Pred>,
        agg: Option<Agg>,
    ) -> Result<Box<dyn Iterator<Item = Sample> + '_>> {
        let mut buf = self.take_buf();
        self.collect_into(series, t0, t1, pred, &mut buf)?;
        match agg {
            None => Ok(Box::new(ScanIter {
                pos: 0,
                buf: Some(buf),
                pool: Arc::clone(&self.shared.buf_pool),
            })),
            Some(a) => {
                let v = a.apply(&buf);
                self.give_buf(buf);
                match v {
                    Some(v) => Ok(Box::new(std::iter::once(Sample { ts: t0, value: v }))),
                    None => Ok(Box::new(std::iter::empty())),
                }
            }
        }
    }

    /// Newest visible sample for `series`, or `None` if the series is absent (v0.8, SPEC §3).
    ///
    /// O(1): a single hash lookup against the per-series head index — no flush, no
    /// filesystem access, no segment decode, no allocation on the read path. The head
    /// updates only on a strictly larger timestamp; the value at an equal maximum
    /// timestamp is unspecified across layers (the index keeps the first known sample,
    /// matching scan's first-writer-wins rule within one layer).
    ///
    /// Visibility follows the ingest pipeline, not the enqueue: after [`Db::put_durable`]
    /// returns the sample is guaranteed visible here; a bare [`Db::put`] becomes visible
    /// once the ingest thread has applied it. `latest()` never blocks waiting for the
    /// queue or WAL sync. In the Deterministic profile a series evicted as a whole by LRU
    /// reports `None`, matching scan visibility.
    ///
    /// Rebuild note: at open the index is rebuilt from the pre-read local segments plus
    /// WAL replay. Segments present only in the cold tier (`archive.catalog`, never read
    /// back) are not decoded at open — a series whose samples are exclusively archived
    /// reports `None` until it is rewritten.
    pub fn latest(&self, series: SeriesId) -> Result<Option<Sample>> {
        self.check_err()?;
        Ok(self.shared.latest.lock().unwrap().get(series))
    }

    /// Compact eligible local segments (v0.8, SPEC §1). Returns the number of input
    /// segments removed.
    ///
    /// Only local, never-archived same-series segments participate (any name present in
    /// `archive.catalog` is skipped); inputs merge in catalog order with first-writer-wins
    /// on duplicate timestamps, so scan results are sample-identical before/after. The
    /// output is a single same-series `RTISEG01` segment written via the existing
    /// write-temp/atomic-rename/SyncPolicy-fsync discipline and installed in one short
    /// critical section; superseded inputs are deleted best-effort afterwards. This is an
    /// explicit operations API — it never runs inside the ingest batch loop.
    ///
    /// Deterministic profile: returns `Ok(0)` and does not touch the filesystem.
    pub fn compact(&self) -> Result<usize> {
        self.check_err()?;
        compact::compact(&self.shared, None)
    }

    /// Compact eligible local segments for one series (v0.8, SPEC §1). Same semantics as
    /// [`Db::compact`], restricted to `series`; other series are untouched.
    pub fn compact_series(&self, series: SeriesId) -> Result<usize> {
        self.check_err()?;
        compact::compact(&self.shared, Some(series))
    }

    /// Cumulative compaction counters for observability (v0.8, SPEC §1).
    pub fn compaction_stats(&self) -> CompactionStats {
        *self.shared.compact_stats.lock().unwrap()
    }

    /// Current number of segments (loaded at open plus sealed at runtime).
    pub fn segment_count(&self) -> usize {
        self.shared.state.lock().unwrap().segments.len()
    }

    /// Current number of samples in the MemTable.
    pub fn memtable_len(&self) -> usize {
        self.shared.state.lock().unwrap().mem.len()
    }

    /// Manually trigger one MemTable seal (test/ops hook).
    ///
    /// No-op under the Deterministic profile (segment persistence is disabled).
    pub fn seal(&self) -> Result<()> {
        self.flush()?;
        if self.shared.config.profile == Profile::Deterministic {
            return Ok(());
        }
        let mut state = self.shared.state.lock().unwrap();
        seal_memtable(&self.shared, &mut state)?;
        // v0.6: after a manual seal the MemTable is empty, so every record in the WAL is covered
        // by segments (WAL invariant: the WAL protects exactly the current MemTable contents) —
        // truncate it to empty.
        if let Some(wal) = &mut state.wal {
            wal.checkpoint_keep(&[])?;
        }
        Ok(())
    }

    /// Take a result buffer from the pool (reused in steady state).
    fn take_buf(&self) -> Vec<Sample> {
        self.shared.buf_pool.lock().unwrap().pop().unwrap_or_default()
    }

    fn give_buf(&self, mut buf: Vec<Sample>) {
        buf.clear();
        if buf.capacity() > 0 {
            self.shared.buf_pool.lock().unwrap().push(buf);
        }
    }

    fn check_err(&self) -> Result<()> {
        if let Some(m) = self.shared.err.lock().unwrap().as_ref() {
            return Err(Error::Corrupt(format!("ingest worker failed: {m}")));
        }
        Ok(())
    }

    /// Merge memtable + segments, sorted and deduplicated by ts (flush happened before the call).
    fn collect_into(
        &self,
        series: SeriesId,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<Pred>,
        out: &mut Vec<Sample>,
    ) -> Result<()> {
        self.flush()?;
        let tier = self.shared.cold.lock().unwrap().clone();
        let mut state = self.shared.state.lock().unwrap();
        // memtable (new data)
        out.extend(state.mem.range(series, t0, t1).filter(|s| pred.map(|p| p.matches(s.value)).unwrap_or(true)));
        // segments (zone-map skipping + predicate pushdown at the decode layer; archived segments read back transparently)
        let pred_fn;
        let pred_ref: Option<&dyn Fn(f64) -> bool> = match pred {
            Some(p) => {
                pred_fn = move |v: f64| p.matches(v);
                Some(&pred_fn)
            }
            None => None,
        };
        for seg in state.segments.iter_mut() {
            if seg.series != series {
                continue;
            }
            if let Some(p) = &pred {
                if !p.zone_may_match(seg.zone.min_val, seg.zone.max_val) {
                    continue; // skip the whole segment (applies to archived segments too — no cold-tier read-back triggered)
                }
            }
            let reader: &SegmentReader = match &mut seg.loc {
                SegLoc::Local(r) => r,
                SegLoc::Archived(cache) => {
                    if cache.is_none() {
                        let t = tier.as_ref().ok_or_else(|| {
                            Error::Corrupt(format!(
                                "segment {} archived but no cold tier configured",
                                seg.name
                            ))
                        })?;
                        *cache = Some(SegmentReader::from_bytes(t.get_segment(&seg.name)?)?);
                    }
                    cache.as_ref().unwrap()
                }
            };
            reader.collect_range(t0, t1, pred_ref, out)?;
        }
        drop(state);
        out.sort_by_key(|s| s.ts);
        out.dedup_by_key(|s| s.ts);
        Ok(())
    }
}

impl ScanSource for Db {
    fn collect(
        &self,
        series: SeriesId,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<Pred>,
        out: &mut Vec<Sample>,
    ) -> Result<()> {
        self.collect_into(series, t0, t1, pred, out)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(w) = self.worker.take() {
            let _ = w.join();
        }
        // best-effort final flush to disk
        if let Ok(mut state) = self.shared.state.lock() {
            if let Some(wal) = &mut state.wal {
                let _ = wal.sync_now();
            }
        }
    }
}

/// Facade free function, verbatim per SPEC §3.
pub fn scan(
    db: &Db,
    series: SeriesId,
    t0: Timestamp,
    t1: Timestamp,
    pred: Option<Pred>,
    agg: Option<Agg>,
) -> Result<Box<dyn Iterator<Item = Sample> + '_>> {
    db.scan(series, t0, t1, pred, agg)
}

/// Lazy scan iterator: holds a pooled buffer and returns it on Drop (0 malloc in steady state).
struct ScanIter {
    pos: usize,
    buf: Option<Vec<Sample>>,
    pool: Arc<Mutex<Vec<Vec<Sample>>>>,
}

impl Iterator for ScanIter {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        let buf = self.buf.as_ref()?;
        let s = *buf.get(self.pos)?;
        self.pos += 1;
        Some(s)
    }
}

impl Drop for ScanIter {
    fn drop(&mut self) {
        if let Some(mut buf) = self.buf.take() {
            buf.clear();
            if buf.capacity() > 0 {
                self.pool.lock().unwrap().push(buf);
            }
        }
    }
}

/// Ingest thread main loop: drain as much of the ring as possible per wakeup (dynamic batching,
/// capped at [`BATCH_MAX`]) → whole-batch WAL append → due group commit → MemTable.
fn ingest_loop(
    shared: Arc<Shared>,
    consumer: rti_buffer::SpscConsumer<Record>,
    shutdown: Arc<AtomicBool>,
) {
    let mut batch: Vec<Record> = Vec::with_capacity(BATCH_MAX);
    loop {
        batch.clear();
        while batch.len() < BATCH_MAX {
            match consumer.pop() {
                Some(r) => batch.push(r),
                None => break,
            }
        }
        if batch.is_empty() {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }
            std::thread::yield_now();
            continue;
        }
        let n = batch.len() as u64;
        let r = apply_batch(&shared, &batch);
        if let Err(e) = r {
            *shared.err.lock().unwrap() = Some(e.to_string());
            return;
        }
        shared.acked.fetch_add(n, Ordering::SeqCst);
    }
    // drain before shutdown: keep processing the records left in the ring
    while let Some(r) = consumer.pop() {
        let _ = apply_batch(&shared, &[r]);
        shared.acked.fetch_add(1, Ordering::SeqCst);
    }
}

/// Apply one batch of records: WAL (one write for the whole batch + due group commit +
/// end-of-batch flush to the OS) → MemTable (seal when full); if a seal occurred within the batch,
/// checkpoint the WAL at batch end — truncate only the prefix already covered by segments, keeping
/// the tail that entered the new MemTable after the seal point (`keep`). The Deterministic profile skips the WAL and evicts by LRU when full.
fn apply_batch(shared: &Shared, batch: &[Record]) -> Result<()> {
    let deterministic = shared.config.profile == Profile::Deterministic;
    let mut state = shared.state.lock().unwrap();
    if let Some(wal) = &mut state.wal {
        match shared.config.wal_sync {
            // Always keeps per-record append (fsync per record, v0.1 semantics unchanged).
            SyncPolicy::Always => {
                for rec in batch {
                    wal.append(rec)?;
                }
            }
            // Group/None: the whole batch is encoded once and written once; fsync frequency is governed by
            // the interval (flushing at batch boundaries only when due), no longer an unconditional fsync per
            // batch; the end-of-batch flush_os guarantees applied records reach at least the OS page cache
            // (surviving process crash; the machine power-loss window is still governed by the interval).
            _ => {
                wal.append_batch(batch)?;
                wal.sync_if_due()?;
                wal.flush_os()?;
            }
        }
    }
    // Batch index of the last seal within the batch: records after the seal point still need WAL
    // protection (they live only in the new MemTable) and must be kept at checkpoint.
    let mut keep_from: Option<usize> = None;
    for (j, rec) in batch.iter().enumerate() {
        if deterministic {
            // Pure in-memory: evict the oldest series by LRU when full (counted internally); never seal.
            // v0.8: latest() follows scan visibility — an evicted series is dropped from the index.
            let victim = state.mem.insert_lru_report(rec.series, rec.sample)?;
            let mut latest = shared.latest.lock().unwrap();
            if let Some(v) = victim {
                latest.remove(v);
            }
            latest.apply(rec.series, rec.sample);
            continue;
        }
        if state.mem.is_full() {
            seal_memtable(shared, &mut state)?;
            keep_from = Some(j);
        }
        match state.mem.insert(rec.series, rec.sample) {
            Ok(()) => {}
            Err(Error::SeriesFull) => {
                // series count exceeds pool capacity: retry once after sealing
                seal_memtable(shared, &mut state)?;
                keep_from = Some(j);
                state.mem.insert(rec.series, rec.sample)?;
            }
            Err(e) => return Err(e),
        }
        // v0.8: applied (== visible) records update the head index before the watermark
        // advances, so put_durable's post-return visibility guarantee holds. Sealing does
        // not change visibility, so heads stay valid across seals.
        shared.latest.lock().unwrap().apply(rec.series, rec.sample);
    }
    // v0.6 WAL checkpoint: a seal occurred within this batch, so all WAL records before the last seal
    // point are already covered by segments (segments are fsynced to disk); atomically truncate while
    // keeping the tail after the seal point. Without a seal in the batch nothing is truncated — the
    // current MemTable's records are still protected by the WAL.
    if let Some(j) = keep_from {
        if let Some(wal) = &mut state.wal {
            wal.checkpoint_keep(&batch[j..])?;
        }
    }
    Ok(())
}

/// Persist the MemTable as segments (grouped by series, one per series) and register the reader.
///
/// This function does **not** truncate the WAL: the caller performs the WAL checkpoint once it knows
/// the boundary between the "covered prefix and the tail to keep" (see `keep_from` in `apply_batch`
/// and `Db::seal`); calls during recovery replay must not truncate either (replay has not finished reading).
fn seal_memtable(shared: &Shared, state: &mut DbState) -> Result<()> {
    let data: BTreeMap<SeriesId, Vec<Sample>> = state.mem.take();
    if data.is_empty() {
        return Ok(());
    }
    let dir = shared
        .config
        .data_dir
        .as_ref()
        .ok_or_else(|| Error::Corrupt("seal requires data_dir = Some(..)".into()))?;
    for (series, samples) in data {
        let mut seq = shared.seg_seq.lock().unwrap();
        let name = format!("seg-{seq:06}-s{series:06}.seg");
        *seq += 1;
        drop(seq);
        let path = dir.join(&name);
        // SyncPolicy::None is the "no flush" tier: segments are not fsynced either (v0.5 behavior),
        // avoiding the fsync tax for durability that was never requested; Group/Always use durable writes —
        // the crash-safety prerequisite for WAL checkpoint truncation.
        if shared.config.wal_sync == SyncPolicy::None {
            SegmentWriter::write_unsynced(&path, series, &samples)?;
        } else {
            SegmentWriter::write(&path, series, &samples)?;
        }
        state.segments.push(SegEntry::local(name, SegmentReader::open(&path)?));
    }
    Ok(())
}

// ------------------------------------------------- archive catalog (v0.4)

/// Catalog file name (inside data_dir).
const CATALOG_FILE: &str = "archive.catalog";

/// Parse a `seg-NNNNNN-sSSSSSS.seg` segment name, returning the sequence number `NNNNNN`
/// (v0.8 SPEC §1 pre-fix: sequence allocation is `max(NNNNNN) + 1` over local files and
/// cataloged names, so reopening after file deletion never reuses a number).
///
/// Returns `None` for any name that does not exactly match the writer's format.
fn parse_seg_seq(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("seg-")?;
    let (seq, rest) = rest.split_once('-')?;
    let series = rest.strip_prefix('s')?.strip_suffix(".seg")?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(seq) || !digits(series) {
        return None;
    }
    seq.parse().ok()
}

/// Catalog line: `A <name> <series> <min_ts> <max_ts> <min_val_bits> <max_val_bits> <count>`.
fn format_catalog_line(e: &SegEntry) -> String {
    format!(
        "A {} {} {} {} {} {} {}\n",
        e.name,
        e.series,
        e.zone.min_ts,
        e.zone.max_ts,
        e.zone.min_val.to_bits(),
        e.zone.max_val.to_bits(),
        e.zone.count
    )
}

/// Parse the catalog; **incomplete/malformed lines are silently skipped** (a crash mid-archive can
/// only leave a partial line, and the corresponding segment is either still a local `.seg` or will be
/// rewritten by the next archive — never read twice, because open deduplicates same-named entries in favor of the local one).
fn load_catalog(dir: &Path) -> Result<Vec<SegEntry>> {
    let path = dir.join(CATALOG_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(&path)?;
    let mut out = Vec::new();
    for line in text.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() != 8 || f[0] != "A" {
            continue; // partial line / unknown format: skip
        }
        let parse = || -> Option<SegEntry> {
            Some(SegEntry {
                name: f[1].to_string(),
                series: f[2].parse().ok()?,
                zone: ZoneMap {
                    min_ts: f[3].parse().ok()?,
                    max_ts: f[4].parse().ok()?,
                    min_val: f64::from_bits(f[5].parse().ok()?),
                    max_val: f64::from_bits(f[6].parse().ok()?),
                    count: f[7].parse().ok()?,
                },
                loc: SegLoc::Archived(None),
            })
        };
        if let Some(e) = parse() {
            out.push(e);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-db-test-{}-{}-{}",
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

    use rti_store::LocalFsColdTier;

    fn config(dir: PathBuf, memtable_max: usize) -> Config {
        Config {
            data_dir: Some(dir),
            memtable_max,
            wal_sync: SyncPolicy::Group { interval_us: 1_000 },
            ..Config::default()
        }
    }

    #[test]
    fn put_scan_read_your_writes() {
        let d = tmpdir("basic");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        for i in 0..1000 {
            db.put(1, Sample::new(i * 10, i as f64)).unwrap();
        }
        db.flush().unwrap();
        let got: Vec<Sample> = db.scan(1, 0, 9990, None, None).unwrap().collect();
        assert_eq!(got.len(), 1000);
        assert_eq!(got[500].value, 500.0);
        // aggregation path
        let agg: Vec<Sample> = db.scan(1, 0, 9990, None, Some(Agg::Max)).unwrap().collect();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].value, 999.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn memtable_seals_into_segments_and_scan_merges() {
        let d = tmpdir("seal");
        let db = Db::open(config(d.clone(), 256)).unwrap(); // small table to encourage a seal
        for i in 0..1000 {
            db.put(1, Sample::new(i, (i % 50) as f64)).unwrap();
        }
        db.flush().unwrap();
        assert!(db.segment_count() >= 3, "1000 points / 256 capacity should seal multiple times");
        // merge across segments + memtable
        let got: Vec<Sample> = db.scan(1, 0, 999, None, None).unwrap().collect();
        assert_eq!(got.len(), 1000);
        assert!(got.windows(2).all(|w| w[0].ts < w[1].ts));
        // predicate pushdown
        let pred: Vec<Sample> = db.scan(1, 0, 999, Some(Pred::Gt(40.0)), None).unwrap().collect();
        assert!(pred.iter().all(|s| s.value > 40.0));
        assert_eq!(pred.len(), 9 * 20); // 9 points (41..49) out of every 50, times 20 groups
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn reopen_recovers_wal_and_segments() {
        let d = tmpdir("recover");
        {
            let db = Db::open(config(d.clone(), 256)).unwrap();
            for i in 0..600 {
                db.put(7, Sample::new(i, i as f64 * 2.0)).unwrap();
            }
            db.flush().unwrap();
        } // drop: the worker exits and the WAL is synced
        let db = Db::open(config(d.clone(), 256)).unwrap();
        let got: Vec<Sample> = db.scan(7, 0, 599, None, None).unwrap().collect();
        assert_eq!(got.len(), 600, "no loss and no duplication after segment + WAL recovery");
        assert_eq!(got[300].value, 600.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// The heavy-load tests (1M put / UDP mirror) are mutually exclusive, avoiding CPU contention
    /// that could starve the mirror receiver thread and cause flaky failures.
    static HEAVY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Named by v0.3: 1M puts under Deterministic never touch the disk.
    ///
    /// Deliberately pass `data_dir = Some(..)`: not even the directory may be created.
    #[test]
    fn deterministic_never_touches_filesystem() {
        let _heavy = HEAVY_LOCK.lock().unwrap();
        let d = tmpdir("det-nofs");
        let data = d.join("should-not-exist");
        let cfg = Config {
            data_dir: Some(data.clone()),
            memtable_max: 1 << 14,
            profile: Profile::Deterministic,
            wal_sync: SyncPolicy::Always, // must be force-overridden to None
            ..Config::default()
        };
        let db = Db::open(cfg).unwrap();
        const PUTS: i64 = 1_000_000;
        let mut i = 0i64;
        while i < PUTS {
            match db.put((i % 8) as u32, Sample::new(i, i as f64)) {
                Ok(()) => i += 1,
                Err(Error::SeriesFull) => std::thread::yield_now(), // backpressure retry
                Err(e) => panic!("put failed: {e}"),
            }
        }
        db.flush().unwrap();
        assert!(!data.exists(), "the Deterministic profile must not create any file/directory");
        assert_eq!(db.segment_count(), 0);
        let ev = db.lru_evictions();
        assert!(ev > 0, "1M puts / 16K capacity must cause LRU evictions");
        // the recent window must remain queryable (LRU keeps the tails of recently touched series)
        let recent: Vec<Sample> = db.scan(7, PUTS - 100, PUTS, None, None).unwrap().collect();
        assert!(!recent.is_empty(), "the recent window must be queryable");
        assert!(recent.iter().all(|s| s.ts % 8 == 7));
        // seal is a no-op
        db.seal().unwrap();
        assert_eq!(db.segment_count(), 0);
        drop(db);
        assert!(!data.exists(), "still no files after drop");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Balanced with data_dir = None must fail (fail-fast on misconfiguration).
    #[test]
    fn balanced_requires_data_dir() {
        let cfg = Config { data_dir: None, ..Config::default() };
        assert!(matches!(Db::open(cfg), Err(Error::Corrupt(_))));
    }

    /// Named by v0.3: the mirror loopback receiver gets >99% of records, and datagrams are parseable.
    #[test]
    fn mirror_loopback_receives_nearly_all_records() {
        let _heavy = HEAVY_LOCK.lock().unwrap();
        let d = tmpdir("mirror");
        // bind the receiver first (loopback, port 0 auto-assigned)
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = rx.local_addr().unwrap();

        let mut cfg = Config::deterministic();
        cfg.memtable_max = 1 << 14;
        cfg.mirror = Some(rti_core::Mirror::new(addr));
        let db = Db::open(cfg).unwrap();

        // concurrent receiver thread: parse 20-byte datagrams and count them
        let stop = Arc::new(AtomicBool::new(false));
        let got = Arc::new(AtomicU64::new(0));
        let bad = Arc::new(AtomicU64::new(0));
        let th = {
            let stop = Arc::clone(&stop);
            let got = Arc::clone(&got);
            let bad = Arc::clone(&bad);
            std::thread::spawn(move || {
                rx.set_read_timeout(Some(std::time::Duration::from_millis(100))).unwrap();
                let mut buf = [0u8; 64];
                while !stop.load(Ordering::SeqCst) {
                    match rx.recv(&mut buf) {
                        Ok(20) => {
                            let series = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                            let ts = i64::from_le_bytes(buf[4..12].try_into().unwrap());
                            let bits = u64::from_le_bytes(buf[12..20].try_into().unwrap());
                            if ts >= 0 && series < 4 && f64::from_bits(bits) == ts as f64 {
                                got.fetch_add(1, Ordering::SeqCst);
                            } else {
                                bad.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        Ok(_) => {
                            bad.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut => {}
                        Err(_) => break,
                    }
                }
            })
        };

        // kernel rmem is limited (a few hundred datagrams on this machine), so bursts inevitably drop packets
        // — exactly the mirror's best-effort semantics. The test sends 64 datagrams per batch and yields
        // 500µs between batches so the receiver can drain, keeping loopback loss far below 1%.
        const PUTS: u64 = 2_000;
        for i in 0..PUTS as i64 {
            db.put((i % 4) as u32, Sample::new(i, i as f64)).unwrap();
            if i % 64 == 63 {
                std::thread::sleep(std::time::Duration::from_micros(500));
            }
        }
        let stats = db.mirror_stats();
        assert_eq!(stats.sent + stats.failed, PUTS, "every successful put is mirrored exactly once");
        // wait for the receiver thread to drain (100ms poll timeout)
        std::thread::sleep(std::time::Duration::from_millis(300));
        stop.store(true, Ordering::SeqCst);
        th.join().unwrap();

        let received = got.load(Ordering::SeqCst);
        assert_eq!(bad.load(Ordering::SeqCst), 0, "every datagram must be parseable");
        assert!(
            received as f64 >= stats.sent as f64 * 0.99,
            "loopback must receive >99% (received={received}, sent={})",
            stats.sent
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Stats are zero when no mirror is configured; put is unaffected.
    #[test]
    fn mirror_absent_stats_are_zero() {
        let d = tmpdir("mirror-off");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        db.put(1, Sample::new(1, 1.0)).unwrap();
        db.flush().unwrap();
        assert_eq!(db.mirror_stats(), MirrorStats { sent: 0, failed: 0 });
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.4: after archiving, local segments are deleted, the catalog is readable back,
    /// and scan results are identical to before archiving (including transparent read-back after restart).
    #[test]
    fn archive_then_scan_reads_through_cold_tier() {
        let d = tmpdir("archive");
        let data = d.join("data");
        let cold_dir = d.join("cold");
        let baseline: Vec<Vec<Sample>>;
        let seg_count: usize;
        {
            let db = Db::open(config(data.clone(), 128)).unwrap();
            let mut i = 0i64;
            while i < 1600 {
                match db.put((i % 4) as u32, Sample::new(i / 4, i as f64)) {
                    Ok(()) => i += 1,
                    Err(Error::SeriesFull) => std::thread::yield_now(),
                    Err(e) => panic!("put: {e}"),
                }
            }
            db.seal().unwrap();
            seg_count = db.segment_count();
            assert!(seg_count > 0);
            // baseline before archiving
            baseline = (0..4u32)
                .map(|s| db.scan(s, 0, i64::MAX, None, None).unwrap().collect())
                .collect();
            assert!(baseline.iter().all(|v| !v.is_empty()));

            // inject the cold tier and archive everything (ts threshold beyond all data)
            db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
            let n = db.archive_older_than(i64::MAX - 1).unwrap();
            assert_eq!(n, seg_count, "all segments should be archived");
            assert_eq!(db.archived_segment_count(), seg_count);
            // all local .seg files deleted, cold tier readable, catalog exists
            let local_segs = std::fs::read_dir(&data)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "seg").unwrap_or(false))
                .count();
            assert_eq!(local_segs, 0, "no local .seg may remain after archiving");
            let tier = LocalFsColdTier::new(&cold_dir).unwrap();
            assert_eq!(tier.list().unwrap().len(), seg_count, "the cold tier must hold all segments");
            assert!(data.join("archive.catalog").exists(), "catalog must be persisted");

            // scan transparent read-back: point-for-point identical to before archiving
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} scan must be identical before/after archiving");
            }
        }

        // restart: catalog reads back Archived entries, scan still transparent
        {
            let db = Db::open(config(data.clone(), 128)).unwrap();
            assert_eq!(db.archived_segment_count(), seg_count, "catalog must be readable after restart");
            // no cold tier injected: hitting an archived segment errors out (instead of silently losing data)
            assert!(db.scan(0, 0, i64::MAX, None, None).is_err());
            db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} scan must be identical after restart");
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Archive guard: errors both without an injected cold tier and under the Deterministic profile.
    #[test]
    fn archive_guards() {
        let d = tmpdir("archive-guard");
        let db = Db::open(config(d.join("data"), 128)).unwrap();
        db.put(1, Sample::new(1, 1.0)).unwrap();
        db.seal().unwrap();
        assert!(db.archive_older_than(100).is_err(), "missing cold tier must error");

        let db2 = Db::open(Config::deterministic()).unwrap();
        db2.set_cold_tier(Arc::new(LocalFsColdTier::new(d.join("cold")).unwrap()));
        assert!(db2.archive_older_than(100).is_err(), "the Deterministic profile must refuse archiving");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Predicate zone-map skipping works on archived segments too (no cold-tier read-back, no error).
    #[test]
    fn archived_segments_still_zone_skipped() {
        let d = tmpdir("archive-zone");
        let data = d.join("data");
        let db = Db::open(config(data.clone(), 64)).unwrap();
        for i in 0..100i64 {
            db.put(1, Sample::new(i, 10.0)).unwrap(); // constant value 10
        }
        db.seal().unwrap();
        db.set_cold_tier(Arc::new(LocalFsColdTier::new(d.join("cold")).unwrap()));
        let n = db.segment_count();
        assert!(n >= 1);
        assert_eq!(db.archive_older_than(1000).unwrap(), n);
        // predicate range [50, 60] does not intersect the segment zone [10,10] -> whole segment skipped -> empty result
        let got: Vec<Sample> = db.scan(1, 0, 1000, Some(Pred::Between(50.0, 60.0)), None).unwrap().collect();
        assert!(got.is_empty(), "disjoint zones must skip the whole segment (including archived ones)");
        // intersecting predicate -> transparent read-back
        let got: Vec<Sample> = db.scan(1, 0, 1000, Some(Pred::Between(5.0, 15.0)), None).unwrap().collect();
        assert_eq!(got.len(), 100);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.5: end-to-end test of `archive_older_than` with an S3 cold tier —
    /// embedded mock S3 server + Db archiving + scan read-back point-for-point identical
    /// (including catalog read-back after restart followed by transparent read-back).
    #[cfg(feature = "s3")]
    #[test]
    fn archive_to_s3_mock_end_to_end() {
        use rti_store::{MockS3Server, S3ColdTier, S3Config};
        let d = tmpdir("archive-s3");
        let data = d.join("data");
        let server = MockS3Server::start().unwrap();
        let mk_tier = || {
            Arc::new(
                S3ColdTier::from_config(S3Config::new(
                    server.endpoint(),
                    "us-east-1",
                    "rti-cold",
                    "AKIDEXAMPLE",
                    "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                ))
                .unwrap(),
            )
        };
        let baseline: Vec<Vec<Sample>>;
        let seg_count: usize;
        {
            let db = Db::open(config(data.clone(), 128)).unwrap();
            let mut i = 0i64;
            while i < 1600 {
                match db.put((i % 4) as u32, Sample::new(i / 4, i as f64)) {
                    Ok(()) => i += 1,
                    Err(Error::SeriesFull) => std::thread::yield_now(),
                    Err(e) => panic!("put: {e}"),
                }
            }
            db.seal().unwrap();
            seg_count = db.segment_count();
            assert!(seg_count > 0);
            baseline = (0..4u32)
                .map(|s| db.scan(s, 0, i64::MAX, None, None).unwrap().collect())
                .collect();
            assert!(baseline.iter().all(|v| !v.is_empty()));

            db.set_cold_tier(mk_tier());
            let n = db.archive_older_than(i64::MAX - 1).unwrap();
            assert_eq!(n, seg_count, "all segments should be archived to mock S3");
            assert_eq!(db.archived_segment_count(), seg_count);
            assert_eq!(server.object_count(), seg_count, "mock S3 must hold all segments");
            assert_eq!(server.rejected_requests(), 0, "signed requests must not be rejected by the mock");
            let local_segs = std::fs::read_dir(&data)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "seg").unwrap_or(false))
                .count();
            assert_eq!(local_segs, 0, "no local .seg may remain after archiving");
            assert!(data.join("archive.catalog").exists(), "catalog must be persisted");

            // scan transparent read-back (fetched from mock S3 over HTTP): point-for-point identical to before archiving
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} scan must be identical before/after archiving");
            }
            // the predicate + aggregation path also goes through transparent read-back
            let pred: Vec<Sample> = db
                .scan(1, 0, i64::MAX, Some(Pred::Gt(1500.0)), None)
                .unwrap()
                .collect();
            assert!(pred.iter().all(|s| s.value > 1500.0) && !pred.is_empty());
            let agg: Vec<Sample> = db.scan(1, 0, i64::MAX, None, Some(Agg::Count)).unwrap().collect();
            assert_eq!(agg.len(), 1);
            assert_eq!(agg[0].value, baseline[1].len() as f64);
        }

        // restart: catalog reads back Archived entries, cold-tier objects still in the mock, scan still identical
        {
            let db = Db::open(config(data.clone(), 128)).unwrap();
            assert_eq!(db.archived_segment_count(), seg_count, "catalog must be readable after restart");
            assert!(db.scan(0, 0, i64::MAX, None, None).is_err(), "no injected cold tier must error");
            db.set_cold_tier(mk_tier());
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} scan must be identical after restart");
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.6: points acknowledged by put_durable suffer zero loss after a simulated crash
    /// (no Drop executed, no final flush/sync — equivalent to kill -9) and restart.
    #[test]
    fn put_durable_survives_simulated_crash() {
        let d = tmpdir("durable-crash");
        let cfg = config(d.clone(), 1 << 16);
        {
            let db = Db::open(cfg.clone()).unwrap();
            for i in 0..500i64 {
                let wm = db
                    .put_durable(1, Sample::new(i, i as f64), Duration::from_secs(5))
                    .unwrap();
                assert!(wm >= (i + 1) as u64, "the returned watermark must be >= this record's sequence number");
            }
            assert_eq!(db.durable_watermark(), 500);
            // simulate kill -9: no Drop (ingest no longer drains, no final sync).
            // the ingest end-of-batch flush_os already guarantees acknowledged records reached the OS page cache.
            std::mem::forget(db);
        }
        let db = Db::open(cfg).unwrap();
        let got: Vec<Sample> = db.scan(1, 0, 499, None, None).unwrap().collect();
        assert_eq!(got.len(), 500, "points acknowledged by put_durable must suffer zero loss");
        assert_eq!(got[250].value, 250.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.6: durable_watermark is monotonically increasing and consistent with put/put_durable counts.
    #[test]
    fn durable_watermark_monotonic_and_matches_puts() {
        let d = tmpdir("watermark");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        let mut last = 0;
        for i in 0..200i64 {
            let wm = db
                .put_durable(3, Sample::new(i, i as f64), Duration::from_secs(5))
                .unwrap();
            assert!(wm >= last, "watermark must increase monotonically ({wm} < {last})");
            last = wm;
        }
        assert_eq!(db.durable_watermark(), 200);
        // mixed async puts: after flush the watermark covers all enqueued records
        for i in 200..400i64 {
            db.put(3, Sample::new(i, i as f64)).unwrap();
        }
        db.flush().unwrap();
        assert_eq!(db.durable_watermark(), 400);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6: put_durable times out with Err(Timeout) but no data is lost (still in the pipeline).
    #[test]
    fn put_durable_timeout_keeps_data_in_pipeline() {
        let d = tmpdir("durable-timeout");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        // zero timeout: may be Ok (happens to be applied already) or Timeout; both outcomes are legal
        let r = db.put_durable(5, Sample::new(1, 42.0), Duration::ZERO);
        match r {
            Ok(_) | Err(Error::Timeout) => {}
            Err(e) => panic!("only Ok or Timeout allowed, got {e}"),
        }
        // whether it timed out or not, the record must eventually be persisted and queryable
        db.flush().unwrap();
        let got: Vec<Sample> = db.scan(5, 0, 10, None, None).unwrap().collect();
        assert_eq!(got, vec![Sample::new(1, 42.0)], "a timeout does not mean data loss");
        assert_eq!(db.durable_watermark(), 1);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.6: after sealing, the WAL is truncated by checkpointing; steady-state on-disk data
    /// is approximately the segments plus at most one MemTable's WAL tail.
    #[test]
    fn wal_checkpoint_truncates_after_seal() {
        let d = tmpdir("wal-checkpoint");
        let wal = d.join("wal.log");
        let db = Db::open(config(d.clone(), 1024)).unwrap();
        for i in 0..10_000i64 {
            db.put(1, Sample::new(i, i as f64)).unwrap();
        }
        db.flush().unwrap();
        // about 9 automatic seals; the WAL keeps only the tail of the last, not-yet-full MemTable
        let mid = std::fs::metadata(&wal).unwrap().len();
        assert!(
            mid <= 1024 * 28,
            "the WAL must be checkpoint-truncated after automatic seals (actual {mid} bytes)"
        );
        assert!(db.segment_count() >= 9, "10_000 points / 1024 capacity should seal multiple times");
        // after manually sealing the remaining MemTable, the WAL must be empty
        db.seal().unwrap();
        let after = std::fs::metadata(&wal).unwrap().len();
        assert_eq!(after, 0, "the WAL must be empty after manual seal (actual {after} bytes)");
        // data integrity is unaffected by truncation
        assert_eq!(db.scan(1, 0, 9999, None, None).unwrap().count(), 10_000);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.6: total steady-state on-disk footprint <= 1.5x the segment size (the WAL no
    /// longer coexists as a second copy next to segments).
    #[test]
    fn steady_state_disk_within_1_5x_segments() {
        let d = tmpdir("disk-ratio");
        let db = Db::open(config(d.clone(), 4096)).unwrap();
        let mut i = 0i64;
        while i < 100_000 {
            match db.put((i % 8) as u32, Sample::new(i / 8, i as f64)) {
                Ok(()) => i += 1,
                Err(Error::SeriesFull) => std::thread::yield_now(),
                Err(e) => panic!("put: {e}"),
            }
        }
        db.flush().unwrap();
        // no manual seal: steady state = sealed segments + WAL tail (<= 1 MemTable)
        let mut seg_bytes = 0u64;
        let mut wal_bytes = 0u64;
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let ext = e.path().extension().map(|x| x.to_string_lossy().into_owned());
            match ext.as_deref() {
                Some("seg") => seg_bytes += e.metadata().unwrap().len(),
                _ => {
                    if e.file_name() == "wal.log" {
                        wal_bytes += e.metadata().unwrap().len();
                    }
                }
            }
        }
        assert!(seg_bytes > 0);
        assert!(wal_bytes <= 4096 * 28, "the WAL tail must not exceed one MemTable ({wal_bytes})");
        let total = seg_bytes + wal_bytes;
        assert!(
            total * 2 <= seg_bytes * 3,
            "steady-state total {total} must be <= 1.5x segment {seg_bytes}"
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Named by v0.6: mixed scenario around the truncation point — crash recovery stays correct
    /// after checkpointing (old data in segments + new data in the WAL tail, no loss, no duplication).
    #[test]
    fn recovery_correct_across_checkpoint_mixed() {
        let d = tmpdir("ckpt-mixed");
        let cfg = config(d.clone(), 1024);
        {
            let db = Db::open(cfg.clone()).unwrap();
            // first batch: triggers multiple automatic seals + checkpoints
            for i in 0..5_000i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
            }
            db.flush().unwrap();
            // second batch: enters the new WAL tail after the checkpoint (not yet sealed)
            for i in 5_000..5_700i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
            }
            db.flush().unwrap();
            assert!(std::fs::metadata(d.join("wal.log")).unwrap().len() <= 1024 * 28);
            // simulate a crash: no Drop (ingest has drained and flush_os'ed, no final sync)
            std::mem::forget(db);
        }
        let db = Db::open(cfg).unwrap();
        let got: Vec<Sample> = db.scan(1, 0, 5699, None, None).unwrap().collect();
        assert_eq!(got.len(), 5_700, "recovery after truncation must have no loss and no duplication");
        assert!(got.windows(2).all(|w| w[0].ts < w[1].ts));
        assert_eq!(got[5_650].value, 5_650.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn free_function_matches_spec_signature() {
        let d = tmpdir("freefn");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        db.put(3, Sample::new(1, 42.0)).unwrap();
        db.flush().unwrap();
        let got: Vec<Sample> = scan(&db, 3, 0, 10, None, None).unwrap().collect();
        assert_eq!(got, vec![Sample::new(1, 42.0)]);
        // go through the rti_query::scan generic path via the ScanSource trait
        let via_query: Vec<Sample> = rti_query::scan(&db, 3, 0, 10, None, Some(Agg::Sum))
            .unwrap()
            .collect();
        assert_eq!(via_query.len(), 1);
        assert_eq!(via_query[0].value, 42.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }
}
