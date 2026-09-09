//! Acceptance test 5: CLI smoke test.
//!
//! Builds a small on-disk engine, writes an `episodes.toml`, runs the compiled
//! `rti-export` binary, and checks the dataset layout it produces.

use std::process::Command;

use rti_core::{Config, Error, Sample};
use rti_db::Db;

fn put_retry(db: &Db, series: u32, s: Sample) {
    loop {
        match db.put(series, s) {
            Ok(()) => return,
            Err(Error::SeriesFull) => std::thread::yield_now(),
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

#[test]
fn cli_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let db_dir = dir.path().join("db");
    let out_dir = dir.path().join("dataset");
    let spec_path = dir.path().join("episodes.toml");

    // persistent engine (Balanced profile): two series, 200 samples each at 1kHz.
    {
        let cfg = Config { data_dir: Some(db_dir.clone()), ..Config::default() };
        let db = Db::open(cfg).unwrap();
        for i in 0..200i64 {
            put_retry(&db, 7, Sample::new(i * 1_000_000, i as f64));
            put_retry(&db, 8, Sample::new(i * 1_000_000, 10_000.0 + i as f64));
        }
        db.flush().unwrap();
    } // drop closes the engine; the CLI re-opens it from disk.

    std::fs::write(
        &spec_path,
        r#"
[[episode]]
name = "cli_ep"
start_ns = 0
end_ns = 199_000_000
frame_hz = 1000

[[episode.series]]
column = "joint0.pos"
id = 7

[[episode.series]]
column = "joint1.pos"
id = 8
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rti-export"))
        .arg("--db")
        .arg(&db_dir)
        .arg("--spec")
        .arg(&spec_path)
        .arg("--out")
        .arg(&out_dir)
        .output()
        .expect("failed to spawn rti-export");
    assert!(
        output.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cli_ep"), "unexpected stdout: {stdout}");

    // dataset layout produced by the CLI run
    assert!(out_dir.join("data/chunk-000/episode_000000.parquet").is_file());
    let stats: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out_dir.join("meta/stats.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(stats["episode"], "cli_ep");
    assert_eq!(stats["frame_hz"], 1000);
    assert_eq!(stats["frames"], 200);
    assert_eq!(stats["series_samples"]["joint0.pos"], 200);
    assert_eq!(stats["series_samples"]["joint1.pos"], 200);
}

#[test]
fn cli_rejects_bad_args() {
    let output = Command::new(env!("CARGO_BIN_EXE_rti-export"))
        .arg("--db")
        .output()
        .expect("failed to spawn rti-export");
    assert!(!output.status.success());
}
