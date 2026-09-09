//! Bridge contract tests (SPEC §2): backpressure (drop-oldest, never blocks the
//! transport sender), unknown-topic skip accounting, and `BridgeStats` semantics.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rti_core::{Config, Timestamp};
use rti_db::Db;
use rti_ros2::{BridgeStats, FieldSelector, NodeConfig, Ros2Bridge, TopicBinding};
use serde_json::json;

fn now_ns() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as Timestamp
}

fn test_db() -> Arc<Db> {
    Arc::new(
        Db::open(Config {
            memtable_max: 1 << 20,
            ..Config::deterministic()
        })
        .unwrap(),
    )
}

fn binding(topic: &str, series: u32, durable: bool, field: &str) -> TopicBinding {
    TopicBinding {
        topic: topic.into(),
        series,
        durable,
        field: FieldSelector::parse(field).unwrap(),
    }
}

/// Poll `f` until it returns true or `timeout` elapses; returns the last value.
fn wait_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    f()
}

/// Backpressure contract: with a slow consumer (fault-injected writer delay) and a
/// small bounded queue, the bridge drops the oldest queued messages and counts them —
/// while the transport sender is never blocked.
#[test]
fn backpressure_drop_oldest_never_blocks_sender() {
    let db = test_db();
    let cfg = NodeConfig {
        queue_capacity: 8,
        // slow-consumer simulation: 20 ms per write ⇒ the pump outruns the writer
        writer_delay: Duration::from_millis(20),
        ..NodeConfig::default()
    };
    let bridge = Ros2Bridge::open(db, vec![binding("/a", 1, false, "value")], cfg).unwrap();
    let tx = bridge.sender().unwrap();

    const N: u64 = 200;
    // The sender (the "ROS side") must never block: unbounded hand-off into the bridge.
    let send_start = Instant::now();
    for i in 0..N {
        tx.send_value("/a", now_ns(), json!({ "value": i as f64 })).unwrap();
    }
    let send_elapsed = send_start.elapsed();
    assert!(
        send_elapsed < Duration::from_secs(2),
        "transport sender blocked for {send_elapsed:?} (backpressure must never reach the sender)"
    );

    // Every injected message arrived at the pump...
    assert!(wait_until(|| bridge.stats().msgs_in == N, Duration::from_secs(10)));
    // ...and each one was either written or drop-oldest-evicted (never silently lost).
    assert!(wait_until(
        || {
            let s = bridge.stats();
            s.points_written + s.dropped + s.skipped == N
        },
        // 200 msgs at 20 ms writer delay would take 4 s without drops; drops make it fast.
        Duration::from_secs(30),
    ));
    let s = bridge.stats();
    assert!(s.dropped > 0, "expected drop-oldest evictions under overload: {s:?}");
    assert_eq!(s.skipped, 0);
    assert_eq!(s.points_written + s.dropped, N);
    // Sanity: with capacity 8 and a 20 ms writer, most of the 200 messages are evicted.
    assert!(s.dropped >= N / 2, "unexpectedly few drops: {s:?}");
}

/// Unknown topics are counted as skipped (SPEC: "unknown topics are logged and
/// skipped") — never a panic, never registered implicitly; the bridge keeps flowing.
#[test]
fn unknown_topic_is_skipped_not_panicked() {
    let db = test_db();
    let bridge = Ros2Bridge::open(
        db,
        vec![binding("/known", 1, true, "value")],
        NodeConfig::default(),
    )
    .unwrap();
    let tx = bridge.sender().unwrap();

    tx.send_value("/unknown", now_ns(), json!({ "value": 1.0 })).unwrap();
    tx.send_value("/known", now_ns(), json!({ "value": 2.0 })).unwrap();
    tx.send_value("/also_unknown", now_ns(), json!({ "value": 3.0 })).unwrap();

    assert!(wait_until(
        || {
            let s = bridge.stats();
            s.msgs_in == 3 && s.points_written + s.skipped + s.dropped == 3
        },
        Duration::from_secs(10),
    ));
    let s = bridge.stats();
    assert_eq!(s.skipped, 2);
    assert_eq!(s.points_written, 1);
    assert_eq!(s.dropped, 0);

    // Bridge is still alive and keeps processing after unknown topics.
    tx.send_value("/known", now_ns(), json!({ "value": 4.0 })).unwrap();
    assert!(wait_until(|| bridge.stats().points_written == 2, Duration::from_secs(10)));
}

/// `BridgeStats` counter semantics: exact msgs_in / points_written / dropped / skipped
/// accounting across known, unknown, durable, non-durable and malformed messages.
#[test]
fn bridge_stats_counter_semantics() {
    let db = test_db();
    let bridge = Ros2Bridge::open(
        Arc::clone(&db),
        vec![
            binding("/a", 1, true, "value"),
            binding("/b", 2, false, "pose/x"),
        ],
        NodeConfig::default(),
    )
    .unwrap();
    assert_eq!(bridge.stats(), BridgeStats::default());
    let tx = bridge.sender().unwrap();

    for i in 0..5 {
        tx.send_value("/a", now_ns(), json!({ "value": i as f64 })).unwrap(); // durable ok
    }
    for i in 0..3 {
        tx.send_value("/b", now_ns(), json!({ "pose": { "x": i as f64 } })).unwrap(); // non-durable ok
    }
    tx.send_value("/unknown", now_ns(), json!({ "value": 0.0 })).unwrap(); // skipped: topic
    tx.send_value("/a", now_ns(), json!({ "other": 1.0 })).unwrap(); // skipped: field missing
    tx.send_value("/b", now_ns(), json!({ "pose": { "x": "NaNish" } })).unwrap(); // skipped: not a number

    const TOTAL: u64 = 11;
    assert!(wait_until(
        || {
            let s = bridge.stats();
            s.msgs_in == TOTAL && s.points_written + s.skipped + s.dropped == TOTAL
        },
        Duration::from_secs(10),
    ));
    let s = bridge.stats();
    assert_eq!(s.msgs_in, TOTAL);
    assert_eq!(s.points_written, 8);
    assert_eq!(s.skipped, 3);
    assert_eq!(s.dropped, 0);
    // lag_p99 is a real measurement: positive, and absurdly below any gate threshold here.
    assert!(s.lag_p99 > Duration::ZERO && s.lag_p99 < Duration::from_secs(5));

    // Values actually landed on the right series (field extraction → series mapping).
    // The caller keeps its own `Arc<Db>` handle (`Ros2Bridge` is opaque by SPEC).
    let a: Vec<_> = db.scan(1, 0, i64::MAX, None, None).unwrap().collect();
    assert_eq!(a.len(), 5);
    assert_eq!(a[4].value, 4.0);
    let b: Vec<_> = db.scan(2, 0, i64::MAX, None, None).unwrap().collect();
    assert_eq!(b.len(), 3);
}
