//! End-to-end v0.7 AI-integration example:
//! ROS 2-style topic feed → rti-db flight recorder → VLA working-memory reads →
//! LeRobot episode export → timed replay.
//!
//! Runs without a system ROS 2 installation by using `InProcessTransport`; swap the
//! transport for the `ros2-rclrs` backend in production. Run with:
//!
//! ```bash
//! cargo run -p rti-ros2 --example ai_pipeline
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rti_core::{Config, Sample};
use rti_db::Db;
use rti_export::{export_episode, EpisodeSpec, LeRobotParquetSink};
use rti_ros2::{
    FieldSelector, InProcessTransport, NodeConfig, Ros2Bridge, TopicBinding, Transport,
};
use rti_vla::{latest, window, TimeSpan};
use serde_json::json;

const SERIES: u32 = 7;
const TOPIC: &str = "/joint_states";
const REPLAY_TOPIC: &str = "/joint_states_replay";
const BASE_TS: i64 = 1_700_000_000_000_000_000;
const N: i64 = 100;

fn unique_tmp() -> PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("rti-v07-ai-pipeline-{}-{n}", std::process::id()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_tmp();
    let db_path = root.join("rti-db");
    let dataset_path = root.join("lerobot-dataset");

    // 1. Open the flight recorder.
    let db = Arc::new(Db::open(Config {
        data_dir: Some(db_path.clone()),
        ..Config::default()
    })?);

    // 2. Bridge one explicit topic → series mapping. The concrete transport stays
    // inspectable so the example can assert what replay published.
    let (transport, sender) = InProcessTransport::new();
    let transport = Arc::new(transport);
    let transport_trait: Arc<dyn Transport> = transport.clone();
    let bindings = vec![TopicBinding {
        topic: TOPIC.into(),
        series: SERIES,
        durable: true,
        field: FieldSelector::parse("effort[3]")?,
    }];
    let bridge = Ros2Bridge::open_with_transport(
        db.clone(),
        transport_trait,
        bindings,
        NodeConfig::default(),
    )?;

    // 3. Feed 100 ROS-style messages at 1 kHz into the durable flight-recorder lane.
    for i in 0..N {
        sender.send_value(
            TOPIC,
            BASE_TS + i * 1_000_000,
            json!({ "name": ["j0", "j1", "j2", "j3"], "effort": [0.0, 0.1, 0.2, i as f64] }),
        )?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while bridge.stats().points_written < N as u64 {
        if Instant::now() > deadline {
            return Err(format!(
                "bridge wrote only {}/{} points",
                bridge.stats().points_written,
                N
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    db.flush()?;

    // 4. Inference-time working memory: S1 latest-state read + S2 episodic window.
    let last: Sample = latest(&db, SERIES)?.expect("bridge ingested samples");
    assert_eq!(last.ts, BASE_TS + (N - 1) * 1_000_000);
    assert_eq!(last.value, (N - 1) as f64);
    let span = TimeSpan::new(BASE_TS, BASE_TS + N * 1_000_000);
    let episodic: Vec<Sample> = window(&db, &[SERIES], span)?.collect();
    assert_eq!(episodic.len(), N as usize);

    // 5. Training-time export: one LeRobot-layout Parquet episode at 1 kHz.
    let spec = EpisodeSpec {
        name: "demo_episode".into(),
        series: vec![("joint3_effort".into(), SERIES)],
        start: BASE_TS,
        end: BASE_TS + (N - 1) * 1_000_000,
        frame_hz: Some(1_000),
    };
    let sink = LeRobotParquetSink::new(&dataset_path, 0, &spec)?;
    let meta = export_episode(&db, &spec, sink)?;
    assert_eq!(meta.frames, N as u64);
    assert!(dataset_path
        .join("data/chunk-000/episode_000000.parquet")
        .exists());
    assert!(dataset_path.join("meta/info.json").exists());

    // 6. Governance-time replay: publish the stored window back with stored-timing
    // pacing (accelerated 1000× to keep the example fast).
    let replay = bridge.replay(
        SERIES,
        BASE_TS,
        BASE_TS + (N - 1) * 1_000_000,
        REPLAY_TOPIC,
        1_000.0,
    )?;
    let replay_deadline = Instant::now() + Duration::from_secs(5);
    while !replay.is_done() {
        if Instant::now() > replay_deadline {
            return Err("replay did not finish within 5s".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(replay.progress(), (N as u64, N as u64));
    assert_eq!(transport.published_len(), N as usize);
    assert!(transport
        .published()
        .iter()
        .all(|m| m.topic == REPLAY_TOPIC));

    println!(
        "v0.7 AI pipeline OK: {} durable points, latest={:?}, {} episodic samples, {} frames -> {}",
        bridge.stats().points_written,
        last,
        episodic.len(),
        meta.frames,
        meta.path.display()
    );

    drop(replay);
    drop(bridge);
    drop(db);
    std::fs::remove_dir_all(&root).ok();
    Ok(())
}
