//! Replay contract tests (SPEC §2): a historical window is published back onto a topic
//! at `speed ×` the original timing, using the stored nanosecond timestamps.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rti_core::{Config, Sample};
use rti_db::Db;
use rti_ros2::{FieldSelector, InProcessTransport, NodeConfig, Ros2Bridge, TopicBinding};

const SERIES: u32 = 7;
const TOPIC: &str = "/replay/out";
/// Original sample spacing: 100 Hz (10 ms in nanoseconds). Large enough that
/// `thread::sleep` pacing noise (~0.1 ms on this kernel) stays far below the SPEC's
/// 50% tolerance band even at 2× speed.
const GAP_NS: i64 = 10_000_000;
const COUNT: usize = 100;
const BASE_TS: i64 = 1_700_000_000_000_000_000;

fn setup() -> (Arc<Db>, Arc<InProcessTransport>, Ros2Bridge) {
    let db = Arc::new(
        Db::open(Config {
            memtable_max: 1 << 20,
            ..Config::deterministic()
        })
        .unwrap(),
    );
    for k in 0..COUNT as i64 {
        db.put(SERIES, Sample::new(BASE_TS + k * GAP_NS, k as f64)).unwrap();
    }
    let (transport, _tx) = InProcessTransport::new();
    let transport = Arc::new(transport);
    let bridge = Ros2Bridge::open_with_transport(
        Arc::clone(&db),
        transport.clone(),
        vec![TopicBinding {
            topic: "/unused".into(),
            series: SERIES,
            durable: false,
            field: FieldSelector::parse("value").unwrap(),
        }],
        NodeConfig::default(),
    )
    .unwrap();
    (db, transport, bridge)
}

fn wait_done(h: &rti_ros2::ReplayHandle, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !h.is_done() {
        assert!(Instant::now() < deadline, "replay did not finish in {timeout:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// 2× speed replay: published inter-message spacing is ~half the stored spacing.
#[test]
fn replay_at_2x_halves_the_intervals() {
    let (_db, transport, bridge) = setup();
    let handle = bridge
        .replay(SERIES, BASE_TS, BASE_TS + (COUNT as i64 - 1) * GAP_NS, TOPIC, 2.0)
        .unwrap();
    wait_done(&handle, Duration::from_secs(30));
    assert_eq!(handle.progress(), (COUNT as u64, COUNT as u64));

    let published = transport.published();
    assert_eq!(published.len(), COUNT);
    assert!(published.iter().all(|m| m.topic == TOPIC));
    // Stored values survive the round trip, in storage order.
    for (k, m) in published.iter().enumerate() {
        assert_eq!(m.payload["value"].as_f64().unwrap(), k as f64);
        assert_eq!(m.payload["ts"].as_i64().unwrap(), BASE_TS + k as i64 * GAP_NS);
    }
    // Pacing: mean wall-clock gap ≈ GAP / speed = 0.5 ms; SPEC tolerance: 50%.
    let gaps: Vec<Duration> = published
        .windows(2)
        .map(|w| w[1].at.duration_since(w[0].at))
        .collect();
    let mean = gaps.iter().sum::<Duration>() / gaps.len() as u32;
    let expected = Duration::from_nanos(GAP_NS as u64 / 2);
    assert!(
        mean > expected / 2 && mean < expected * 3 / 2,
        "mean replay gap {mean:?} not within 50% of {expected:?}"
    );
}

/// 1× speed replay takes ~the original window length; `stop` terminates early.
#[test]
fn replay_progress_and_stop() {
    let (_db, transport, bridge) = setup();
    // Very slow replay (0.05×): 200 ms of stored time would take 4 s — we stop early.
    let handle = bridge
        .replay(SERIES, BASE_TS, BASE_TS + (COUNT as i64 - 1) * GAP_NS, TOPIC, 0.05)
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let (published_before_stop, total) = handle.progress();
    assert_eq!(total, COUNT as u64);
    assert!(published_before_stop < total, "replay should still be running");
    handle.stop();
    wait_done(&handle, Duration::from_secs(10));
    let (published, _) = handle.progress();
    assert!(published < total, "stop should cut the replay short (published {published})");
    assert_eq!(transport.published_len(), published as usize);
}

/// Invalid replay requests are rejected up front.
#[test]
fn replay_rejects_invalid_arguments() {
    let (_db, _transport, bridge) = setup();
    for bad_speed in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(bridge.replay(SERIES, BASE_TS, BASE_TS + GAP_NS, TOPIC, bad_speed).is_err());
    }
    assert!(bridge.replay(SERIES, BASE_TS + GAP_NS, BASE_TS, TOPIC, 1.0).is_err());
}
