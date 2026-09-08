//! rti-db 门面：`Db::open` / `put` / `scan`。
//!
//! 写路径（SPEC §4.1）：`put` 只做一次无锁 SPSC 入队（~20ns 级）；
//! 后台 ingest 线程批量出队 → 批量 WAL append → 按 [`SyncPolicy`]
//! 组提交 → 写入 MemTable；MemTable 满则 seal 为列式 segment。
//!
//! 读路径（SPEC §4.2）：`scan` 先 `flush` 保证读己之写，然后
//! zone map 跳过无关 segment、谓词下推到 decode 层；结果缓冲
//! 取自对象池，稳态扫描 0 次 malloc。

#![forbid(unsafe_code)]

//! ## v0.3：确定性配置档与镜像
//!
//! [`Profile::Deterministic`] 下：强制 [`SyncPolicy::None`]、纯内存运行
//! （不建目录、不开 WAL、不写 segment，即使 `data_dir = Some` 也绝不
//! 触碰文件系统）、MemTable 满按 LRU 丢弃最老序列并计数
//! （[`Db::lru_evictions`]）；[`Mirror`] 使 `put` 入队成功的同时以
//! 非阻塞 UDP 发送 20 字节镜像数据报（best-effort，失败仅计数，
//! 见 [`Db::mirror_stats`]）。

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

#[cfg(feature = "alloc-count")]
pub use rti_mem::alloc_count::{alloc_count, CountingAllocator};

/// ingest ring 默认容量（2 的幂）。
const RING_CAP: usize = 1 << 16;
/// ingest 线程单批最大处理条数（v0.6 动态批：单次唤醒尽量 drain
/// ring，以此上限分批；fsync 频率由 SyncPolicy interval 决定而非批大小）。
const BATCH_MAX: usize = 8192;
/// MemTable 序列槽位数上限。
const MAX_SERIES: usize = 4096;

/// 内部共享状态（ingest 线程与查询线程共享）。
struct Shared {
    state: Mutex<DbState>,
    /// 已入队计数（put 成功后 +1；即下一张水位票据的基数）。
    enqueued: AtomicU64,
    /// 已被 ingest 线程应用计数（= durable 水位，v0.6）。
    acked: AtomicU64,
    /// ingest 线程致命错误（set 后 put/scan 失败）。
    err: Mutex<Option<String>>,
    /// 扫描结果缓冲池（稳态复用，0 malloc）。
    buf_pool: Arc<Mutex<Vec<Vec<Sample>>>>,
    /// segment 文件序号。
    seg_seq: Mutex<u64>,
    /// 冷分层存储（v0.4；`set_cold_tier` 注入）。
    cold: Mutex<Option<Arc<dyn ColdTier>>>,
    config: Config,
}

/// segment 位置：本地（内存已解析）或冷层（按需取回并缓存）。
enum SegLoc {
    /// 本地 segment（v0.1 语义：打开时全量读入内存）。
    Local(SegmentReader),
    /// 已归档冷层；`Option` 为透明读回缓存（首次命中后驻留内存）。
    Archived(Option<SegmentReader>),
}

/// catalog 中的一条 segment 记录（v0.4 冷分层）。
struct SegEntry {
    /// segment 文件名（冷层对象名）。
    name: String,
    series: SeriesId,
    /// zone map：归档后也保留在 catalog，scan 仍可整段跳过。
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
    /// Balanced 档为 `Some`；Deterministic 纯内存运行，为 `None`。
    wal: Option<Wal>,
    mem: MemTable,
    segments: Vec<SegEntry>,
}

/// 镜像统计（v0.3）：UDP 镜像数据报发送计数。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MirrorStats {
    /// 成功交给 socket 的数据报数。
    pub sent: u64,
    /// 发送失败（非阻塞拒绝/错误）的数据报数。
    pub failed: u64,
}

/// 数据库句柄。`Send + Sync`，可跨线程共享（如 rti-net server）。
///
/// `put` 经 `Mutex<SpscProducer>` 入队：单写者（推荐部署）时锁无竞争，
/// 开销与无锁入队同量级；多写者并发时退化为短临界区（有界）。
pub struct Db {
    shared: Arc<Shared>,
    producer: Mutex<rti_buffer::SpscProducer<Record>>,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    /// 非阻塞 UDP 镜像 socket（`Some` 时 put 入队成功后发送镜像）。
    mirror: Option<UdpSocket>,
    mirror_sent: AtomicU64,
    mirror_failed: AtomicU64,
}

impl Db {
    /// 打开（或创建）数据库：建目录 → 加载已有 segment → WAL 崩溃恢复。
    ///
    /// [`Profile::Deterministic`] 下跳过全部文件系统操作（纯内存），
    /// 并强制 [`SyncPolicy::None`]；[`Profile::Balanced`] 下
    /// `config.data_dir` 必须为 `Some`。
    pub fn open(config: Config) -> Result<Db> {
        let deterministic = config.profile == Profile::Deterministic;
        // 确定性档强制 SyncPolicy::None（无 WAL，刷盘语义为空操作）。
        let mut config = config;
        if deterministic {
            config.wal_sync = SyncPolicy::None;
        }

        let (wal, segments, seg_seq) = if deterministic {
            // 纯内存：不建目录、不开 WAL、不加载 segment。
            (None, Vec::new(), 0)
        } else {
            let dir = config
                .data_dir
                .as_ref()
                .ok_or_else(|| Error::Corrupt("Balanced profile requires data_dir = Some(..)".into()))?;
            fs::create_dir_all(dir)?;

            // 加载已有 segment（按文件名序 = 时间序）
            let mut segments = Vec::new();
            let mut seg_seq = 0u64;
            let mut names: Vec<PathBuf> = fs::read_dir(dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
                .collect();
            names.sort();
            for p in names {
                let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                segments.push(SegEntry::local(name, SegmentReader::open(&p)?));
                seg_seq += 1;
            }
            // 读回归档 catalog：冷层 segment 注册为 Archived（lazy 读回）。
            // 若同名文件仍在本地（归档-删除间崩溃），以本地为准并跳过。
            for entry in load_catalog(dir)? {
                if !segments.iter().any(|e: &SegEntry| e.name == entry.name) {
                    segments.push(entry);
                }
            }

            let wal = Wal::open(dir.join("wal.log"), config.wal_sync)?;
            (Some(wal), segments, seg_seq)
        };

        let shared = Arc::new(Shared {
            state: Mutex::new(DbState { wal, mem: MemTable::new(config.memtable_max, MAX_SERIES), segments }),
            enqueued: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            err: Mutex::new(None),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            seg_seq: Mutex::new(seg_seq),
            cold: Mutex::new(None),
            config: config.clone(),
        });

        // WAL 崩溃恢复：直接回放进 memtable（必要时中途 seal）
        if let Some(dir) = config.data_dir.as_ref().filter(|_| !deterministic) {
            let mut state = shared.state.lock().unwrap();
            for rec in Wal::recover(dir.join("wal.log"))? {
                if state.mem.is_full() {
                    // 恢复重放期间禁止 checkpoint WAL（重放尚未读完）。
                    seal_memtable(&shared, &mut state)?;
                }
                state.mem.insert(rec.series, rec.sample)?;
            }
        }

        // 镜像 socket：非阻塞，配置错误（绑定失败等）在 open 时即报错。
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

    /// 写入一个采样点：无锁 SPSC 入队，O(1)，无堆分配。
    ///
    /// ring 满（消费速度跟不上）时返回 [`Error::SeriesFull`] 作为背压信号。
    /// 配置了 [`rti_core::Mirror`] 时，入队成功的同时以非阻塞 UDP
    /// 发送一条 20 字节镜像数据报（best-effort：失败仅计数，
    /// 不阻塞、不重试、不影响返回值）。
    pub fn put(&self, series: SeriesId, sample: Sample) -> Result<()> {
        self.check_err()?;
        // 占位计数与入队在同一临界区：序号（水位票据）与 ring 顺序一致，
        // 这是 put_durable 等待语义的正确性基础；单写者时锁无竞争。
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

    /// 写入一个采样点并**阻塞等待其持久化**（v0.6，崩溃不丢档）。
    ///
    /// 与 [`Db::put`] 同样先入队（低延迟路径不变），随后等待 ingest
    /// 线程完成「应用 MemTable + WAL append」（是否含 fsync 由当前
    /// [`SyncPolicy`] 决定：`Always` 含每条刷盘；`Group` 受组提交窗口
    /// 约束，进程崩溃时 OS 页缓存内的已写数据仍在，但机器掉电语义
    /// 以 interval 为上界；`None` 不刷盘）。返回时的 durable 水位序号
    /// （≥ 本记录的序号，单调递增）。
    ///
    /// 超时返回 [`Error::Timeout`]，但**数据不丢**：记录仍在 ingest
    /// 管线中，稍后会持久化；调用方可稍后用 [`Db::durable_watermark`]
    /// 复查。ring 满返回 [`Error::SeriesFull`]（背压，未入队，可重试）。
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

    /// 当前 durable 水位（v0.6）：已被 ingest 线程「应用 MemTable +
    /// WAL append」的记录总数（按入队序号），单调递增。
    ///
    /// [`Db::put_durable`] 返回的水位 ≥ 其记录序号即表示该记录已持久化
    /// （fsync 与否取决于 [`SyncPolicy`]，同上）。
    pub fn durable_watermark(&self) -> u64 {
        self.shared.acked.load(Ordering::SeqCst)
    }

    /// 镜像发送：20 字节小端数据报（series | ts | value bits）。
    ///
    /// 非阻塞 socket；任何失败只计数——镜像不进入热路径延迟预算。
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

    /// 镜像统计（v0.3）：发送成功/失败计数。
    pub fn mirror_stats(&self) -> MirrorStats {
        MirrorStats {
            sent: self.mirror_sent.load(Ordering::Relaxed),
            failed: self.mirror_failed.load(Ordering::Relaxed),
        }
    }

    /// Deterministic 档 LRU 丢弃的序列总数（v0.3；Balanced 档恒 0）。
    pub fn lru_evictions(&self) -> u64 {
        self.shared.state.lock().unwrap().mem.lru_evictions()
    }

    /// 注入冷分层存储（v0.4）。归档/透明读回都经此句柄。
    pub fn set_cold_tier(&self, tier: Arc<dyn ColdTier>) {
        *self.shared.cold.lock().unwrap() = Some(tier);
    }

    /// 已归档到冷层的 segment 数（v0.4）。
    pub fn archived_segment_count(&self) -> usize {
        self.shared.state.lock().unwrap().segments.iter().filter(|e| e.is_archived()).count()
    }

    /// 冷分层归档（v0.4）：把 zone.max_ts 严格早于 `ts` 的本地 segment
    /// 移入冷层——上传字节、删除本地文件、更新 catalog。
    ///
    /// 之后 `scan` 对这些时间段**透明读回**（命中时从冷层取回并缓存；
    /// zone map 仍在 catalog，谓词整段跳过不触发读回）。
    ///
    /// 崩溃安全说明：上传与删除之间崩溃 → 本地文件与冷层同时存在，
    /// open 时以本地为准去重（scan 结果不受影响，scan 本就按 ts 去重）。
    ///
    /// 错误：Deterministic 档（无文件系统）或未注入冷层时返回 `Err`。
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

    /// 阻塞直到当前已入队的记录全部落盘（WAL sync）并可见。
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

    /// 扫描 `series` 在 `[t0, t1]` 的样本。
    ///
    /// `pred` 下推到 decode 层；`agg` 为 `Some` 时返回单样本迭代器
    /// （`ts = t0`，`value = 聚合结果`）。语义同 [`rti_query::scan`]。
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

    /// 当前 segment 数量（含已加载与运行期 seal 的）。
    pub fn segment_count(&self) -> usize {
        self.shared.state.lock().unwrap().segments.len()
    }

    /// 当前 MemTable 中的样本数。
    pub fn memtable_len(&self) -> usize {
        self.shared.state.lock().unwrap().mem.len()
    }

    /// 手动触发一次 MemTable seal（测试/运维钩子）。
    ///
    /// Deterministic 档为 no-op（segment 落盘禁用）。
    pub fn seal(&self) -> Result<()> {
        self.flush()?;
        if self.shared.config.profile == Profile::Deterministic {
            return Ok(());
        }
        let mut state = self.shared.state.lock().unwrap();
        seal_memtable(&self.shared, &mut state)?;
        // v0.6：手动 seal 后 MemTable 必为空 ⇒ WAL 中记录全部被
        // segment 覆盖（WAL 不变式：WAL 恰好保护当前 MemTable 的内容），
        // 直接截断为空。
        if let Some(wal) = &mut state.wal {
            wal.checkpoint_keep(&[])?;
        }
        Ok(())
    }

    /// 从缓冲池取一个结果缓冲（稳态复用）。
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

    /// 合并 memtable + segments，按 ts 排序去重（调用前已 flush）。
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
        // memtable（新数据）
        out.extend(state.mem.range(series, t0, t1).filter(|s| pred.map(|p| p.matches(s.value)).unwrap_or(true)));
        // segments（zone map 跳过 + decode 层谓词下推；归档段透明读回）
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
                    continue; // 整段跳过（归档段同样适用——不触发冷层读回）
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
        // 尽力最终落盘
        if let Ok(mut state) = self.shared.state.lock() {
            if let Some(wal) = &mut state.wal {
                let _ = wal.sync_now();
            }
        }
    }
}

/// 与 SPEC §3 逐字一致的门面自由函数。
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

/// 惰性扫描迭代器：持有一个池化缓冲，Drop 时归还（稳态 0 malloc）。
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

/// ingest 线程主循环：单次唤醒尽量 drain ring（动态批，上限
/// [`BATCH_MAX`]）→ 整批 WAL append → 到期组提交 → MemTable。
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
    // 关闭前排空：ring 中剩余记录继续处理
    while let Some(r) = consumer.pop() {
        let _ = apply_batch(&shared, &[r]);
        shared.acked.fetch_add(1, Ordering::SeqCst);
    }
}

/// 应用一批记录：WAL（整批一次写 + 到期组提交 + 批末 flush 到 OS）→
/// MemTable（满则 seal）；批内发生过 seal 时，批末对 WAL 做
/// checkpoint——只截断已被 segment 覆盖的前缀，保留 seal 点后进入
/// 新 MemTable 的尾巴（`keep`）。Deterministic 档跳过 WAL，满则 LRU 丢弃。
fn apply_batch(shared: &Shared, batch: &[Record]) -> Result<()> {
    let deterministic = shared.config.profile == Profile::Deterministic;
    let mut state = shared.state.lock().unwrap();
    if let Some(wal) = &mut state.wal {
        match shared.config.wal_sync {
            // Always 保持逐条 append（每条 fsync，v0.1 语义不变）。
            SyncPolicy::Always => {
                for rec in batch {
                    wal.append(rec)?;
                }
            }
            // Group/None：整批一次编码一次写；fsync 频率由 interval
            // 决定（批边界仅到期才刷），不再每批无条件 fsync；
            // 批末 flush_os 保证已应用记录至少进入 OS 页缓存
            // （进程崩溃不丢；机器掉电窗口仍由 interval 决定）。
            _ => {
                wal.append_batch(batch)?;
                wal.sync_if_due()?;
                wal.flush_os()?;
            }
        }
    }
    // 批内最后一次 seal 对应的 batch 下标：seal 点之后的记录仍需要
    // WAL 保护（它们只在新 MemTable 中），checkpoint 时必须保留。
    let mut keep_from: Option<usize> = None;
    for (j, rec) in batch.iter().enumerate() {
        if deterministic {
            // 纯内存：满则 LRU 丢弃最老序列（内部计数），永不 seal。
            state.mem.insert_lru(rec.series, rec.sample)?;
            continue;
        }
        if state.mem.is_full() {
            seal_memtable(shared, &mut state)?;
            keep_from = Some(j);
        }
        match state.mem.insert(rec.series, rec.sample) {
            Ok(()) => {}
            Err(Error::SeriesFull) => {
                // 序列数超池容量：seal 后重试一次
                seal_memtable(shared, &mut state)?;
                keep_from = Some(j);
                state.mem.insert(rec.series, rec.sample)?;
            }
            Err(e) => return Err(e),
        }
    }
    // v0.6 WAL checkpoint：本批发生过 seal ⇒ 最后一次 seal 点之前的
    // 全部 WAL 记录已被 segment 覆盖（segment 已 fsync 落盘），
    // 原子截断并保留 seal 点后的尾巴。批内无 seal 则不截断——
    // 当前 MemTable 的记录仍由 WAL 保护。
    if let Some(j) = keep_from {
        if let Some(wal) = &mut state.wal {
            wal.checkpoint_keep(&batch[j..])?;
        }
    }
    Ok(())
}

/// 将 MemTable 落盘为一个（按序列分组，每序列一个）segment，并登记 reader。
///
/// 本函数**不**截断 WAL：WAL checkpoint 由调用方在确知「被覆盖前缀
/// 与待保留尾巴」的边界后执行（见 `apply_batch` 的 `keep_from` 与
/// `Db::seal`）；恢复重放期间调用本函数后同样不得截断（重放未读完）。
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
        // SyncPolicy::None = 「不刷盘」档：segment 也不 fsync（v0.5 行为），
        // 避免为未要求的持久性付 fsync 税；Group/Always 走持久化写入，
        // 这是 WAL checkpoint 截断的崩溃安全前提。
        if shared.config.wal_sync == SyncPolicy::None {
            SegmentWriter::write_unsynced(&path, series, &samples)?;
        } else {
            SegmentWriter::write(&path, series, &samples)?;
        }
        state.segments.push(SegEntry::local(name, SegmentReader::open(&path)?));
    }
    Ok(())
}

// ------------------------------------------------- 归档 catalog（v0.4）

/// catalog 文件名（data_dir 内）。
const CATALOG_FILE: &str = "archive.catalog";

/// catalog 行：`A <name> <series> <min_ts> <max_ts> <min_val_bits> <max_val_bits> <count>`。
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

/// 解析 catalog；**不完整/畸形行静默跳过**（归档中途崩溃只可能留下
/// 残行，对应 segment 仍在本地 `.seg` 或下次归档时重写——不会双读，
/// 因为 open 对同名条目以本地为准去重）。
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
            continue; // 残行/未知格式：跳过
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
        // 聚合路径
        let agg: Vec<Sample> = db.scan(1, 0, 9990, None, Some(Agg::Max)).unwrap().collect();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].value, 999.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn memtable_seals_into_segments_and_scan_merges() {
        let d = tmpdir("seal");
        let db = Db::open(config(d.clone(), 256)).unwrap(); // 小表促 seal
        for i in 0..1000 {
            db.put(1, Sample::new(i, (i % 50) as f64)).unwrap();
        }
        db.flush().unwrap();
        assert!(db.segment_count() >= 3, "1000 点 / 256 容量应多次 seal");
        // 跨 segment + memtable 合并
        let got: Vec<Sample> = db.scan(1, 0, 999, None, None).unwrap().collect();
        assert_eq!(got.len(), 1000);
        assert!(got.windows(2).all(|w| w[0].ts < w[1].ts));
        // 谓词下推
        let pred: Vec<Sample> = db.scan(1, 0, 999, Some(Pred::Gt(40.0)), None).unwrap().collect();
        assert!(pred.iter().all(|s| s.value > 40.0));
        assert_eq!(pred.len(), 9 * 20); // 每 50 点中 41..49 共 9 个 × 20 组
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
        } // drop：worker 退出，wal 已 sync
        let db = Db::open(config(d.clone(), 256)).unwrap();
        let got: Vec<Sample> = db.scan(7, 0, 599, None, None).unwrap().collect();
        assert_eq!(got.len(), 600, "segment + WAL 恢复后应无丢失无重复");
        assert_eq!(got[300].value, 600.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// 重负载测试（1M put / UDP 镜像）互斥，避免并发时 CPU 竞争
    /// 饿死镜像接收线程造成抖动性失败。
    static HEAVY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// v0.3 点名：Deterministic 下 1M 次 put 全程不落盘。
    ///
    /// 故意传入 `data_dir = Some(..)`：目录都不应被创建。
    #[test]
    fn deterministic_never_touches_filesystem() {
        let _heavy = HEAVY_LOCK.lock().unwrap();
        let d = tmpdir("det-nofs");
        let data = d.join("should-not-exist");
        let cfg = Config {
            data_dir: Some(data.clone()),
            memtable_max: 1 << 14,
            profile: Profile::Deterministic,
            wal_sync: SyncPolicy::Always, // 必须被强制改写为 None
            ..Config::default()
        };
        let db = Db::open(cfg).unwrap();
        const PUTS: i64 = 1_000_000;
        let mut i = 0i64;
        while i < PUTS {
            match db.put((i % 8) as u32, Sample::new(i, i as f64)) {
                Ok(()) => i += 1,
                Err(Error::SeriesFull) => std::thread::yield_now(), // 背压重试
                Err(e) => panic!("put failed: {e}"),
            }
        }
        db.flush().unwrap();
        assert!(!data.exists(), "Deterministic 档不得创建任何文件/目录");
        assert_eq!(db.segment_count(), 0);
        let ev = db.lru_evictions();
        assert!(ev > 0, "1M put / 16K 容量必然发生 LRU 丢弃");
        // 最近窗口数据仍可查（LRU 保留最近触达的序列尾部）
        let recent: Vec<Sample> = db.scan(7, PUTS - 100, PUTS, None, None).unwrap().collect();
        assert!(!recent.is_empty(), "最近窗口必须可查");
        assert!(recent.iter().all(|s| s.ts % 8 == 7));
        // seal 为 no-op
        db.seal().unwrap();
        assert_eq!(db.segment_count(), 0);
        drop(db);
        assert!(!data.exists(), "drop 后仍不得有文件");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Balanced 档 data_dir 为 None 必须报错（误配置 fail-fast）。
    #[test]
    fn balanced_requires_data_dir() {
        let cfg = Config { data_dir: None, ..Config::default() };
        assert!(matches!(Db::open(cfg), Err(Error::Corrupt(_))));
    }

    /// v0.3 点名：镜像 loopback 接收端收到 >99% 记录，且报文格式可解析。
    #[test]
    fn mirror_loopback_receives_nearly_all_records() {
        let _heavy = HEAVY_LOCK.lock().unwrap();
        let d = tmpdir("mirror");
        // 接收端先绑定（loopback，端口 0 自动分配）
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = rx.local_addr().unwrap();

        let mut cfg = Config::deterministic();
        cfg.memtable_max = 1 << 14;
        cfg.mirror = Some(rti_core::Mirror::new(addr));
        let db = Db::open(cfg).unwrap();

        // 并发接收线程：20 字节数据报解析并计数
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

        // 内核 rmem 有限（本机折算容量仅数百报文），突发发送必然丢包
        // ——这正是镜像 best-effort 语义。测试以 64 报文为一批、批间
        // 让出 500µs 供接收线程排空，使 loopback 丢包率远低于 1%。
        const PUTS: u64 = 2_000;
        for i in 0..PUTS as i64 {
            db.put((i % 4) as u32, Sample::new(i, i as f64)).unwrap();
            if i % 64 == 63 {
                std::thread::sleep(std::time::Duration::from_micros(500));
            }
        }
        let stats = db.mirror_stats();
        assert_eq!(stats.sent + stats.failed, PUTS, "每次成功 put 恰好镜像一次");
        // 等接收线程排空（超时 100ms 轮询）
        std::thread::sleep(std::time::Duration::from_millis(300));
        stop.store(true, Ordering::SeqCst);
        th.join().unwrap();

        let received = got.load(Ordering::SeqCst);
        assert_eq!(bad.load(Ordering::SeqCst), 0, "报文格式必须全部可解析");
        assert!(
            received as f64 >= stats.sent as f64 * 0.99,
            "loopback 必须收到 >99%（received={received}, sent={})",
            stats.sent
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// 未配置镜像时统计为零；put 不受影响。
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

    /// v0.4 点名：归档后本地 segment 删除、catalog 可读回、
    /// scan 结果与归档前一致（含重启后透明读回）。
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
            // 归档前基线
            baseline = (0..4u32)
                .map(|s| db.scan(s, 0, i64::MAX, None, None).unwrap().collect())
                .collect();
            assert!(baseline.iter().all(|v| !v.is_empty()));

            // 注入冷层并归档全部（ts 阈值大于所有数据）
            db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
            let n = db.archive_older_than(i64::MAX - 1).unwrap();
            assert_eq!(n, seg_count, "全部 segment 都应归档");
            assert_eq!(db.archived_segment_count(), seg_count);
            // 本地 .seg 全删，冷层可读，catalog 存在
            let local_segs = std::fs::read_dir(&data)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "seg").unwrap_or(false))
                .count();
            assert_eq!(local_segs, 0, "归档后本地不得残留 .seg");
            let tier = LocalFsColdTier::new(&cold_dir).unwrap();
            assert_eq!(tier.list().unwrap().len(), seg_count, "冷层必须持有全部 segment");
            assert!(data.join("archive.catalog").exists(), "catalog 必须落盘");

            // scan 透明读回：与归档前逐点一致
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} 归档前后 scan 必须一致");
            }
        }

        // 重启：catalog 读回 Archived 条目，scan 仍透明
        {
            let db = Db::open(config(data.clone(), 128)).unwrap();
            assert_eq!(db.archived_segment_count(), seg_count, "重启后 catalog 必须读回");
            // 未注入冷层：命中归档段时报错（而非静默丢数据）
            assert!(db.scan(0, 0, i64::MAX, None, None).is_err());
            db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} 重启后 scan 必须一致");
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// 归档守卫：未注入冷层 / Deterministic 档均报错。
    #[test]
    fn archive_guards() {
        let d = tmpdir("archive-guard");
        let db = Db::open(config(d.join("data"), 128)).unwrap();
        db.put(1, Sample::new(1, 1.0)).unwrap();
        db.seal().unwrap();
        assert!(db.archive_older_than(100).is_err(), "无冷层必须报错");

        let db2 = Db::open(Config::deterministic()).unwrap();
        db2.set_cold_tier(Arc::new(LocalFsColdTier::new(d.join("cold")).unwrap()));
        assert!(db2.archive_older_than(100).is_err(), "Deterministic 档必须拒绝归档");
        std::fs::remove_dir_all(&d).ok();
    }

    /// 谓词 zone-map 跳过对归档段同样生效（不触发冷层读回也不出错）。
    #[test]
    fn archived_segments_still_zone_skipped() {
        let d = tmpdir("archive-zone");
        let data = d.join("data");
        let db = Db::open(config(data.clone(), 64)).unwrap();
        for i in 0..100i64 {
            db.put(1, Sample::new(i, 10.0)).unwrap(); // 值恒 10
        }
        db.seal().unwrap();
        db.set_cold_tier(Arc::new(LocalFsColdTier::new(d.join("cold")).unwrap()));
        let n = db.segment_count();
        assert!(n >= 1);
        assert_eq!(db.archive_older_than(1000).unwrap(), n);
        // 谓词值域 [50, 60] 与段 zone [10,10] 不相交 → 整段跳过 → 空结果
        let got: Vec<Sample> = db.scan(1, 0, 1000, Some(Pred::Between(50.0, 60.0)), None).unwrap().collect();
        assert!(got.is_empty(), "zone 不相交必须整段跳过（含归档段）");
        // 相交谓词 → 透明读回
        let got: Vec<Sample> = db.scan(1, 0, 1000, Some(Pred::Between(5.0, 15.0)), None).unwrap().collect();
        assert_eq!(got.len(), 100);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.5 点名：`archive_older_than` 配 S3 冷层的端到端测试——
    /// 内嵌 mock S3 server + Db 归档 + scan 读回逐点一致（含重启后
    /// catalog 读回再透明读回）。
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
            assert_eq!(n, seg_count, "全部 segment 都应归档到 mock S3");
            assert_eq!(db.archived_segment_count(), seg_count);
            assert_eq!(server.object_count(), seg_count, "mock S3 必须持有全部 segment");
            assert_eq!(server.rejected_requests(), 0, "签名请求不应被 mock 拒签");
            let local_segs = std::fs::read_dir(&data)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "seg").unwrap_or(false))
                .count();
            assert_eq!(local_segs, 0, "归档后本地不得残留 .seg");
            assert!(data.join("archive.catalog").exists(), "catalog 必须落盘");

            // scan 透明读回（经 HTTP 从 mock S3 取回）：与归档前逐点一致
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} 归档前后 scan 必须一致");
            }
            // 谓词 + 聚合路径同样走透明读回
            let pred: Vec<Sample> = db
                .scan(1, 0, i64::MAX, Some(Pred::Gt(1500.0)), None)
                .unwrap()
                .collect();
            assert!(pred.iter().all(|s| s.value > 1500.0) && !pred.is_empty());
            let agg: Vec<Sample> = db.scan(1, 0, i64::MAX, None, Some(Agg::Count)).unwrap().collect();
            assert_eq!(agg.len(), 1);
            assert_eq!(agg[0].value, baseline[1].len() as f64);
        }

        // 重启：catalog 读回 Archived 条目，冷层对象仍在 mock，scan 仍一致
        {
            let db = Db::open(config(data.clone(), 128)).unwrap();
            assert_eq!(db.archived_segment_count(), seg_count, "重启后 catalog 必须读回");
            assert!(db.scan(0, 0, i64::MAX, None, None).is_err(), "未注入冷层必须报错");
            db.set_cold_tier(mk_tier());
            for (s, want) in baseline.iter().enumerate() {
                let got: Vec<Sample> = db.scan(s as u32, 0, i64::MAX, None, None).unwrap().collect();
                assert_eq!(&got, want, "series {s} 重启后 scan 必须一致");
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6 点名：put_durable 确认的点在模拟崩溃（不执行 Drop、
    /// 无最终 flush/sync，等价 kill -9）后重启 0 丢失。
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
                assert!(wm >= (i + 1) as u64, "返回水位必须 ≥ 本记录序号");
            }
            assert_eq!(db.durable_watermark(), 500);
            // 模拟 kill -9：不执行 Drop（ingest 不再 drain、无最终 sync）。
            // ingest 批末 flush_os 已保证已确认记录进入 OS 页缓存。
            std::mem::forget(db);
        }
        let db = Db::open(cfg).unwrap();
        let got: Vec<Sample> = db.scan(1, 0, 499, None, None).unwrap().collect();
        assert_eq!(got.len(), 500, "put_durable 已确认的点必须 0 丢失");
        assert_eq!(got[250].value, 250.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6 点名：durable_watermark 单调递增，且与 put/put_durable 计数一致。
    #[test]
    fn durable_watermark_monotonic_and_matches_puts() {
        let d = tmpdir("watermark");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        let mut last = 0;
        for i in 0..200i64 {
            let wm = db
                .put_durable(3, Sample::new(i, i as f64), Duration::from_secs(5))
                .unwrap();
            assert!(wm >= last, "水位必须单调递增（{wm} < {last}）");
            last = wm;
        }
        assert_eq!(db.durable_watermark(), 200);
        // 混合异步 put：flush 后水位覆盖全部已入队记录
        for i in 200..400i64 {
            db.put(3, Sample::new(i, i as f64)).unwrap();
        }
        db.flush().unwrap();
        assert_eq!(db.durable_watermark(), 400);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6：put_durable 超时返回 Err(Timeout) 但数据不丢（仍在管线中）。
    #[test]
    fn put_durable_timeout_keeps_data_in_pipeline() {
        let d = tmpdir("durable-timeout");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        // 零超时：可能 Ok（恰好已应用）或 Timeout；两种结果都合法
        let r = db.put_durable(5, Sample::new(1, 42.0), Duration::ZERO);
        match r {
            Ok(_) | Err(Error::Timeout) => {}
            Err(e) => panic!("只允许 Ok 或 Timeout，得到 {e}"),
        }
        // 无论是否超时，记录最终必须持久可查
        db.flush().unwrap();
        let got: Vec<Sample> = db.scan(5, 0, 10, None, None).unwrap().collect();
        assert_eq!(got, vec![Sample::new(1, 42.0)], "超时不等于丢数据");
        assert_eq!(db.durable_watermark(), 1);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6 点名：seal 后 WAL 被 checkpoint 截断，稳态落盘
    /// ≈ segment + 至多一个 MemTable 的 WAL 尾巴。
    #[test]
    fn wal_checkpoint_truncates_after_seal() {
        let d = tmpdir("wal-checkpoint");
        let wal = d.join("wal.log");
        let db = Db::open(config(d.clone(), 1024)).unwrap();
        for i in 0..10_000i64 {
            db.put(1, Sample::new(i, i as f64)).unwrap();
        }
        db.flush().unwrap();
        // 自动 seal 约 9 次；WAL 只剩最后一个未满 MemTable 的尾巴
        let mid = std::fs::metadata(&wal).unwrap().len();
        assert!(
            mid <= 1024 * 28,
            "自动 seal 后 WAL 必须被 checkpoint 截断（实际 {mid} 字节）"
        );
        assert!(db.segment_count() >= 9, "10_000 点 / 1024 容量应多次 seal");
        // 手动 seal 剩余 MemTable 后 WAL 必须为空
        db.seal().unwrap();
        let after = std::fs::metadata(&wal).unwrap().len();
        assert_eq!(after, 0, "手动 seal 后 WAL 必须为空（实际 {after} 字节）");
        // 数据完整性不受截断影响
        assert_eq!(db.scan(1, 0, 9999, None, None).unwrap().count(), 10_000);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6 点名：稳态总落盘 ≤ 1.5× segment 大小（WAL 不再与
    /// segment 并存双份）。
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
        // 不做手动 seal：稳态 = 已 seal 的 segment + WAL 尾巴（≤ 1 个 MemTable）
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
        assert!(wal_bytes <= 4096 * 28, "WAL 尾巴不得超出一个 MemTable（{wal_bytes}）");
        let total = seg_bytes + wal_bytes;
        assert!(
            total * 2 <= seg_bytes * 3,
            "稳态总落盘 {total} 必须 ≤ 1.5× segment {seg_bytes}"
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// v0.6 点名：截断点前后混合场景——崩溃恢复在 checkpoint 后仍正确
    /// （segment 中的旧数据 + WAL 尾巴中的新数据，无丢失无重复）。
    #[test]
    fn recovery_correct_across_checkpoint_mixed() {
        let d = tmpdir("ckpt-mixed");
        let cfg = config(d.clone(), 1024);
        {
            let db = Db::open(cfg.clone()).unwrap();
            // 第一批：触发多次自动 seal + checkpoint
            for i in 0..5_000i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
            }
            db.flush().unwrap();
            // 第二批：checkpoint 之后进入新 WAL 尾巴（尚未 seal）
            for i in 5_000..5_700i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
            }
            db.flush().unwrap();
            assert!(std::fs::metadata(d.join("wal.log")).unwrap().len() <= 1024 * 28);
            // 模拟崩溃：不 Drop（ingest 已 drain 且 flush_os，无最终 sync）
            std::mem::forget(db);
        }
        let db = Db::open(cfg).unwrap();
        let got: Vec<Sample> = db.scan(1, 0, 5699, None, None).unwrap().collect();
        assert_eq!(got.len(), 5_700, "截断后恢复必须无丢失无重复");
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
        // 通过 ScanSource trait 走 rti_query::scan 泛型路径
        let via_query: Vec<Sample> = rti_query::scan(&db, 3, 0, 10, None, Some(Agg::Sum))
            .unwrap()
            .collect();
        assert_eq!(via_query.len(), 1);
        assert_eq!(via_query[0].value, 42.0);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }
}
