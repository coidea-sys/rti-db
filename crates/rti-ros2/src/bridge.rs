//! The long-running bridge (SPEC §2).
//!
//! Threading model: a main bridge thread plus a **durable lane** thread (plus the
//! caller). The main loop drains the transport into a bounded drop-oldest queue
//! (non-blocking; the transport sender is never blocked), then processes batches:
//! field extraction happens here, non-durable samples go straight to [`Db::put`], and
//! durable samples are handed to the durable lane, which calls the blocking
//! [`Db::put_durable`] — so a slow durability acknowledgement can never stall the
//! high-rate non-durable flow (per-topic order is preserved: a topic lives on exactly
//! one lane).
//!
//! ```text
//! Transport (ROS side)                main bridge thread                    rti-db
//!   try_recv ──▶ bounded drop-oldest queue ──▶ extract ──▶ non-durable ─────▶ put
//!              (never blocks; full ⇒ evict         │
//!               oldest, dropped += 1)              └─▶ durable lane queue ──▶ put_durable
//!                                                   (drop-oldest, dropped += 1)   (durable lane thread)
//! ```
//!
//! Backpressure contract: congestion is absorbed **inside** the bridge by evicting the
//! oldest queued message and counting it — the transport sender (DDS) never blocks. A
//! full `Db` ingest ring ([`Error::SeriesFull`]) is likewise counted as dropped, never
//! retried in place.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rti_core::{Error, Result, Sample, SeriesId, Timestamp};
use rti_db::Db;

use crate::transport::{InProcessSender, InProcessTransport, RawMessage, Transport};
use crate::TopicBinding;

/// How many queued messages the bridge thread processes per batch before re-draining
/// the transport. Batching amortizes queue-locking; re-draining between batches keeps
/// the transport side freshly emptied (drop-oldest decisions see the newest messages).
const PROCESS_BATCH: usize = 256;

/// Bridge node configuration (the transport-agnostic part of a ROS 2 node config).
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Node name (diagnostics; a rclrs backend uses it as the ROS node name).
    pub name: String,
    /// Capacity of the bounded ingest queue. When full, the oldest queued message is
    /// evicted and counted ([`BridgeStats::dropped`]).
    pub queue_capacity: usize,
    /// Timeout passed to [`Db::put_durable`] for `durable` bindings. Per the `Db`
    /// contract a timeout loses no data (the record stays in the ingest pipeline), so a
    /// timed-out write is still counted as written.
    pub durable_timeout: Duration,
    /// Sliding-window size (samples) of the lag histogram behind [`BridgeStats::lag_p99`].
    pub lag_window: usize,
    /// Fault-injection hook: artificial per-message delay in the bridge loop. Used by
    /// backpressure tests to simulate a slow consumer; **must be zero in production**.
    pub writer_delay: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            name: "rti_ros2d".into(),
            queue_capacity: 1 << 16,
            durable_timeout: Duration::from_secs(5),
            lag_window: 4096,
            writer_delay: Duration::ZERO,
        }
    }
}

/// Bridge counters snapshot (SPEC §2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BridgeStats {
    /// Messages received from the transport (all topics, known or not).
    pub msgs_in: u64,
    /// Samples successfully handed to `Db` (`put` accepted, or `put_durable` enqueued —
    /// a durability timeout still counts, per the no-loss `Db` contract).
    pub points_written: u64,
    /// Messages lost to backpressure: oldest-queue evictions plus `Db` ring-full
    /// rejections. This is the only loss the bridge ever reports.
    pub dropped: u64,
    /// Messages skipped without loss semantics: unknown topic (logged-and-skipped per
    /// SPEC) or unextractable field.
    pub skipped: u64,
    /// p99 of the sliding-window transport→write lag (message timestamp → sample handed
    /// to `Db`). Zero when no lag samples have been recorded yet.
    pub lag_p99: Duration,
}

/// Sliding-window microsecond lag samples behind [`BridgeStats::lag_p99`].
struct LagWindow {
    cap: usize,
    us: VecDeque<u64>,
}

impl LagWindow {
    fn new(cap: usize) -> Self {
        LagWindow {
            cap: cap.max(1),
            us: VecDeque::new(),
        }
    }

    fn record(&mut self, us: u64) {
        if self.us.len() == self.cap {
            self.us.pop_front();
        }
        self.us.push_back(us);
    }

    /// Extend with a batch of samples (one lock acquisition per batch on the caller
    /// side keeps the per-message cost off the hot path).
    fn extend(&mut self, us: &[u64]) {
        for &u in us {
            self.record(u);
        }
    }

    fn p99(&self) -> Duration {
        if self.us.is_empty() {
            return Duration::ZERO;
        }
        let mut v: Vec<u64> = self.us.iter().copied().collect();
        v.sort_unstable();
        // nearest-rank percentile
        let idx = (v.len() * 99).div_ceil(100) - 1;
        Duration::from_micros(v[idx])
    }
}

/// Interior shared state: counters + lag window + shutdown flag.
struct Shared {
    msgs_in: AtomicU64,
    points_written: AtomicU64,
    dropped: AtomicU64,
    skipped: AtomicU64,
    lag: Mutex<LagWindow>,
    shutdown: AtomicBool,
}

impl Shared {
    fn stats(&self) -> BridgeStats {
        BridgeStats {
            msgs_in: self.msgs_in.load(Ordering::Relaxed),
            points_written: self.points_written.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            lag_p99: self.lag.lock().unwrap().p99(),
        }
    }
}

/// Bounded queue with **drop-oldest** push semantics: when full, the oldest element is
/// evicted (reported via the return value) to make room — pushers never block.
struct DropOldestQueue<T> {
    q: Mutex<VecDeque<T>>,
    cap: usize,
}

impl<T> DropOldestQueue<T> {
    fn new(cap: usize) -> Self {
        DropOldestQueue {
            q: Mutex::new(VecDeque::new()),
            cap: cap.max(1),
        }
    }

    /// Push `item`; returns `true` when an oldest element was evicted to make room.
    fn push_drop_oldest(&self, item: T) -> bool {
        let mut q = self.q.lock().unwrap();
        let evicted = if q.len() >= self.cap {
            q.pop_front();
            true
        } else {
            false
        };
        q.push_back(item);
        evicted
    }

    /// Move up to `max` oldest elements into `out` (non-blocking).
    fn pop_batch(&self, out: &mut Vec<T>, max: usize) {
        let mut q = self.q.lock().unwrap();
        let n = q.len().min(max);
        out.extend(q.drain(..n));
    }

    fn len(&self) -> usize {
        self.q.lock().unwrap().len()
    }
}

/// Current wall clock as nanoseconds (the `RawMessage::ts` clock domain).
fn now_ns() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as Timestamp)
        .unwrap_or(0)
}

/// Handle to a running [`Ros2Bridge::replay`] task: progress query + cooperative stop.
pub struct ReplayHandle {
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    published: Arc<AtomicU64>,
    total: u64,
    join: Option<JoinHandle<()>>,
}

impl ReplayHandle {
    /// `(published, total)` sample counts.
    pub fn progress(&self) -> (u64, u64) {
        (self.published.load(Ordering::Relaxed), self.total)
    }

    /// `true` when the window has been fully published (or the task was stopped).
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    /// Ask the replay task to stop; in-flight pacing sleep finishes first.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Drop for ReplayHandle {
    fn drop(&mut self) {
        self.stop();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Long-running bridge. Owns a `Db` handle opened by the caller (SPEC §2, opaque).
///
/// Construct via [`Ros2Bridge::open`] (default in-process transport) or
/// [`Ros2Bridge::open_with_transport`] (any [`Transport`] backend — identical public
/// API, per the SPEC implementation note). Dropping the bridge stops the bridge thread
/// after it has drained what remains queued.
pub struct Ros2Bridge {
    db: Arc<Db>,
    transport: Arc<dyn Transport>,
    cfg: NodeConfig,
    shared: Arc<Shared>,
    queue: Arc<DropOldestQueue<RawMessage>>,
    main: Option<JoinHandle<()>>,
    durable_lane: Option<JoinHandle<()>>,
    /// Injection handle, present only for the default in-process transport.
    sender: Option<InProcessSender>,
}

impl Ros2Bridge {
    /// Open a bridge on the default in-process transport (SPEC §2 signature).
    ///
    /// Use [`Ros2Bridge::sender`] to obtain the injection handle of the in-process
    /// transport (tests / embedding); use [`Ros2Bridge::open_with_transport`] to bring
    /// a real backend.
    pub fn open(db: Arc<Db>, bindings: Vec<TopicBinding>, node_cfg: NodeConfig) -> Result<Self> {
        let (transport, sender) = InProcessTransport::new();
        Self::spawn(db, Arc::new(transport), Some(sender), bindings, node_cfg)
    }

    /// Open a bridge on an explicit transport backend (same public contract).
    pub fn open_with_transport(
        db: Arc<Db>,
        transport: Arc<dyn Transport>,
        bindings: Vec<TopicBinding>,
        node_cfg: NodeConfig,
    ) -> Result<Self> {
        Self::spawn(db, transport, None, bindings, node_cfg)
    }

    fn spawn(
        db: Arc<Db>,
        transport: Arc<dyn Transport>,
        sender: Option<InProcessSender>,
        bindings: Vec<TopicBinding>,
        node_cfg: NodeConfig,
    ) -> Result<Self> {
        if node_cfg.queue_capacity == 0 {
            return Err(Error::Corrupt("NodeConfig::queue_capacity must be >= 1".into()));
        }
        let mut by_topic = HashMap::with_capacity(bindings.len());
        for b in bindings {
            if by_topic.insert(b.topic.clone(), b).is_some() {
                return Err(Error::Corrupt("duplicate topic in bindings".into()));
            }
        }
        let by_topic = Arc::new(by_topic);
        let shared = Arc::new(Shared {
            msgs_in: AtomicU64::new(0),
            points_written: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            lag: Mutex::new(LagWindow::new(node_cfg.lag_window)),
            shutdown: AtomicBool::new(false),
        });
        let queue = Arc::new(DropOldestQueue::new(node_cfg.queue_capacity));
        let lane = Arc::new(DropOldestQueue::new(node_cfg.queue_capacity));
        let main = {
            let db = Arc::clone(&db);
            let transport = Arc::clone(&transport);
            let shared = Arc::clone(&shared);
            let queue = Arc::clone(&queue);
            let lane = Arc::clone(&lane);
            let cfg = node_cfg.clone();
            std::thread::Builder::new()
                .name("rti-ros2-bridge".into())
                .spawn(move || bridge_loop(transport, db, by_topic, shared, queue, lane, cfg))
                .map_err(Error::Io)?
        };
        let durable_lane = {
            let db = Arc::clone(&db);
            let shared = Arc::clone(&shared);
            let timeout = node_cfg.durable_timeout;
            std::thread::Builder::new()
                .name("rti-ros2-durable".into())
                .spawn(move || durable_lane_loop(db, shared, lane, timeout))
                .map_err(Error::Io)?
        };
        Ok(Ros2Bridge {
            db,
            transport,
            cfg: node_cfg,
            shared,
            queue,
            main: Some(main),
            durable_lane: Some(durable_lane),
            sender,
        })
    }

    /// Injection handle of the default in-process transport (`None` for other backends).
    pub fn sender(&self) -> Option<InProcessSender> {
        self.sender.clone()
    }

    /// Transport backend name (diagnostics).
    pub fn transport_name(&self) -> &str {
        self.transport.name()
    }

    /// Node configuration the bridge was opened with.
    pub fn config(&self) -> &NodeConfig {
        &self.cfg
    }

    /// Publish a historical window back onto a topic, for replay/sim (SPEC §2).
    ///
    /// Samples of `series` in `[start, end]` are re-published in storage order; pacing
    /// follows the stored nanosecond timestamps at `speed ×` the original timing
    /// (inter-sample gap `dt` becomes `dt / speed`; non-increasing timestamps are
    /// published back-to-back). Each replayed message is a `{"ts", "value"}` payload.
    ///
    /// Errors on `speed <= 0` / non-finite, or `start > end`.
    pub fn replay(
        &self,
        series: SeriesId,
        start: Timestamp,
        end: Timestamp,
        topic: &str,
        speed: f64,
    ) -> Result<ReplayHandle> {
        if !(speed.is_finite() && speed > 0.0) {
            return Err(Error::Corrupt(format!(
                "replay speed must be positive and finite, got {speed}"
            )));
        }
        if start > end {
            return Err(Error::Corrupt(format!(
                "replay window empty: start {start} > end {end}"
            )));
        }
        let samples: Vec<Sample> = self.db.scan(series, start, end, None, None)?.collect();
        let total = samples.len() as u64;
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let published = Arc::new(AtomicU64::new(0));
        let join = {
            let transport = Arc::clone(&self.transport);
            let topic = topic.to_string();
            let stop = Arc::clone(&stop);
            let done = Arc::clone(&done);
            let published = Arc::clone(&published);
            std::thread::Builder::new()
                .name("rti-ros2-replay".into())
                .spawn(move || {
                    // Cumulative pacing schedule anchored at the first publication:
                    // sample with stored timestamp `ts` is due at
                    // `t0 + (ts - first_ts) / speed`. Anchoring (instead of chaining
                    // per-gap sleeps) makes pacing immune to systematic sleep overshoot.
                    let mut t0: Option<std::time::Instant> = None;
                    let mut first_ts: Timestamp = 0;
                    for s in samples {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        match t0 {
                            None => {
                                t0 = Some(std::time::Instant::now());
                                first_ts = s.ts;
                            }
                            Some(start) => {
                                let due_ns = (s.ts.saturating_sub(first_ts).max(0) as f64 / speed) as u64;
                                pace_until(start + Duration::from_nanos(due_ns));
                            }
                        }
                        if transport
                            .publish(&topic, serde_json::json!({ "ts": s.ts, "value": s.value }))
                            .is_err()
                        {
                            break;
                        }
                        published.fetch_add(1, Ordering::Relaxed);
                    }
                    done.store(true, Ordering::Relaxed);
                })
                .map_err(Error::Io)?
        };
        Ok(ReplayHandle {
            stop,
            done,
            published,
            total,
            join: Some(join),
        })
    }

    /// Counters snapshot (SPEC §2).
    pub fn stats(&self) -> BridgeStats {
        self.shared.stats()
    }

    /// Number of messages currently sitting in the bounded ingest queue (test/ops hook).
    pub fn queued(&self) -> usize {
        self.queue.len()
    }
}

impl Drop for Ros2Bridge {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Relaxed);
        if let Some(j) = self.main.take() {
            let _ = j.join();
        }
        if let Some(j) = self.durable_lane.take() {
            let _ = j.join();
        }
    }
}

/// One extracted durable sample in flight to the durable lane.
struct DurableItem {
    series: SeriesId,
    ts: Timestamp,
    value: f64,
}

/// Wait until `target`: coarse-sleep while far away, spin for the last ~1 ms.
///
/// `thread::sleep` alone overshoots unpredictably on a loaded machine (observed: a 5 ms
/// sleep waking after 8+ ms), which would break the replay pacing contract; the spin
/// tail bounds the error to scheduler jitter at the wake point instead.
fn pace_until(target: std::time::Instant) {
    const SPIN_TAIL: Duration = Duration::from_millis(1);
    loop {
        let now = std::time::Instant::now();
        if now >= target {
            return;
        }
        let rem = target - now;
        if rem > SPIN_TAIL * 2 {
            std::thread::sleep(rem - SPIN_TAIL);
        } else {
            std::hint::spin_loop();
        }
    }
}

/// The main bridge loop: drain the transport (never blocking it), process batches.
/// Durable samples are forwarded to the durable lane; everything else is written here.
///
/// Per-message outcome accounting (feeds [`BridgeStats`]):
/// - full ingest queue on drain ⇒ oldest evicted, `dropped`;
/// - unknown topic ⇒ `skipped` (SPEC: unknown topics are logged and skipped);
/// - field not extractable ⇒ `skipped`;
/// - full durable lane ⇒ oldest evicted, `dropped`;
/// - `put` ok ⇒ `points_written` + lag sample;
/// - `Db` ring full ([`Error::SeriesFull`], backpressure) or any other `Db` error ⇒
///   `dropped` (the sample did not land; never retried in place — the bridge must not
///   stall on a sick engine).
fn bridge_loop(
    transport: Arc<dyn Transport>,
    db: Arc<Db>,
    bindings: Arc<HashMap<String, TopicBinding>>,
    shared: Arc<Shared>,
    queue: Arc<DropOldestQueue<RawMessage>>,
    lane: Arc<DropOldestQueue<DurableItem>>,
    cfg: NodeConfig,
) {
    let mut batch: Vec<RawMessage> = Vec::with_capacity(PROCESS_BATCH);
    let mut lags: Vec<u64> = Vec::with_capacity(PROCESS_BATCH);
    loop {
        // Drain the transport into the bounded queue. Never blocks the sender: a full
        // queue evicts the oldest message (counted) instead of waiting.
        let mut drained = 0u32;
        while let Some(msg) = transport.try_recv() {
            shared.msgs_in.fetch_add(1, Ordering::Relaxed);
            if queue.push_drop_oldest(msg) {
                shared.dropped.fetch_add(1, Ordering::Relaxed);
            }
            drained += 1;
        }
        // Process one batch.
        batch.clear();
        queue.pop_batch(&mut batch, PROCESS_BATCH);
        if batch.is_empty() {
            if drained == 0 {
                // Fully idle: brief sleep, far below any lag-gate threshold. On
                // shutdown, drain whatever is still queued before exiting.
                if shared.shutdown.load(Ordering::Relaxed) {
                    queue.pop_batch(&mut batch, PROCESS_BATCH);
                    if batch.is_empty() {
                        return;
                    }
                } else {
                    std::thread::sleep(Duration::from_micros(50));
                    continue;
                }
            } else {
                continue;
            }
        }
        lags.clear();
        for msg in batch.drain(..) {
            let Some(binding) = bindings.get(&msg.topic) else {
                shared.skipped.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let value = match binding.field.extract(&msg.payload) {
                Ok(v) => v,
                Err(_) => {
                    shared.skipped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            if binding.durable {
                let item = DurableItem {
                    series: binding.series,
                    ts: msg.ts,
                    value,
                };
                if lane.push_drop_oldest(item) {
                    shared.dropped.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
            match db.put(binding.series, Sample::new(msg.ts, value)) {
                Ok(()) => {
                    shared.points_written.fetch_add(1, Ordering::Relaxed);
                    lags.push(now_ns().saturating_sub(msg.ts).max(0) as u64 / 1_000);
                }
                Err(_) => {
                    shared.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            if cfg.writer_delay > Duration::ZERO {
                std::thread::sleep(cfg.writer_delay);
            }
        }
        if !lags.is_empty() {
            shared.lag.lock().unwrap().extend(&lags);
        }
    }
}

/// The durable lane: blocking [`Db::put_durable`] writes off the main loop's critical
/// path. A durability **timeout** still counts as written (no data lost, per the `Db`
/// contract); a full `Db` ring or any other error counts as `dropped`.
fn durable_lane_loop(
    db: Arc<Db>,
    shared: Arc<Shared>,
    lane: Arc<DropOldestQueue<DurableItem>>,
    timeout: Duration,
) {
    let mut batch: Vec<DurableItem> = Vec::with_capacity(PROCESS_BATCH);
    let mut lags: Vec<u64> = Vec::with_capacity(PROCESS_BATCH);
    loop {
        batch.clear();
        lane.pop_batch(&mut batch, PROCESS_BATCH);
        if batch.is_empty() {
            if shared.shutdown.load(Ordering::Relaxed) {
                return; // lane drained (main loop exited first, nothing new arrives)
            }
            std::thread::sleep(Duration::from_micros(100));
            continue;
        }
        lags.clear();
        for item in batch.drain(..) {
            let outcome = match db.put_durable(item.series, Sample::new(item.ts, item.value), timeout) {
                Ok(_) | Err(Error::Timeout) => Ok(()),
                Err(e) => Err(e),
            };
            match outcome {
                Ok(()) => {
                    shared.points_written.fetch_add(1, Ordering::Relaxed);
                    lags.push(now_ns().saturating_sub(item.ts).max(0) as u64 / 1_000);
                }
                Err(_) => {
                    shared.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if !lags.is_empty() {
            shared.lag.lock().unwrap().extend(&lags);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_oldest_queue_evicts_oldest() {
        let q = DropOldestQueue::new(2);
        assert!(!q.push_drop_oldest(1));
        assert!(!q.push_drop_oldest(2));
        assert!(q.push_drop_oldest(3)); // evicts 1
        let mut out = Vec::new();
        q.pop_batch(&mut out, 16);
        assert_eq!(out, vec![2, 3]);
        q.pop_batch(&mut out, 16);
        assert_eq!(out.len(), 2); // unchanged: queue empty
    }

    #[test]
    fn lag_window_p99() {
        let mut w = LagWindow::new(4);
        for us in [10, 20, 30, 40, 50] {
            w.record(us); // cap 4: 10 is evicted
        }
        assert_eq!(w.p99(), Duration::from_micros(50));
    }
}
