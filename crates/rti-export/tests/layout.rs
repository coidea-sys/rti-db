//! Acceptance test 4: LeRobot dataset layout on disk.

mod common;

use std::io::Read;

use rti_core::Sample;
use rti_export::{episode_parquet_path, export_episode, EpisodeSpec, LeRobotParquetSink};

use common::{mem_db, put_retry};

#[test]
fn lerobot_dataset_layout() {
    let db = mem_db(1 << 12);
    for i in 0..100i64 {
        put_retry(&db, 3, Sample::new(i * 1_000_000, i as f64 * 0.5));
    }
    db.flush().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let spec = EpisodeSpec {
        name: "layout_ep".into(),
        series: vec![("joint0.pos".into(), 3)],
        start: 0,
        end: 99_000_000,
        frame_hz: Some(1000),
    };
    let sink = LeRobotParquetSink::new(root, 0, &spec).unwrap();
    let meta = export_episode(&db, &spec, sink).unwrap();

    // data/chunk-000/episode_000000.parquet
    let parquet = root.join("data/chunk-000/episode_000000.parquet");
    assert!(parquet.is_file(), "{} must exist", parquet.display());
    assert_eq!(meta.path, parquet);
    assert_eq!(episode_parquet_path(root, 0), parquet);

    // Parquet magic at both ends of the file.
    let mut f = std::fs::File::open(&parquet).unwrap();
    let mut head = [0u8; 4];
    f.read_exact(&mut head).unwrap();
    assert_eq!(&head, b"PAR1");
    let len = f.metadata().unwrap().len();
    let mut tail = [0u8; 4];
    use std::io::{Seek, SeekFrom};
    f.seek(SeekFrom::Start(len - 4)).unwrap();
    f.read_exact(&mut tail).unwrap();
    assert_eq!(&tail, b"PAR1");

    // meta/info.json
    let info: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("meta/info.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(info["name"], "layout_ep");
    assert_eq!(info["fps"], 1000);
    assert_eq!(info["total_frames"], meta.frames);
    assert_eq!(info["features"]["timestamp"]["dtype"], "int64");
    assert_eq!(info["features"]["joint0.pos"]["dtype"], "float64");

    // meta/stats.json + meta/episodes.jsonl
    let stats: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("meta/stats.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(stats["frames"], meta.frames);
    assert_eq!(stats["frame_samples"].as_array().unwrap().len() as u64, meta.frames);
    assert_eq!(stats["series_samples"]["joint0.pos"], 100);

    let episodes = std::fs::read_to_string(root.join("meta/episodes.jsonl")).unwrap();
    let line: serde_json::Value = serde_json::from_str(episodes.trim()).unwrap();
    assert_eq!(line["episode_index"], 0);
    assert_eq!(line["name"], "layout_ep");
    assert_eq!(line["length"], meta.frames);
}
