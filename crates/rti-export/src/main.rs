//! `rti-export` CLI: export rti-db series as LeRobot episodes.
//!
//! Usage: `rti-export --db PATH --spec episodes.toml --out dataset/`
//!
//! Every `[[episode]]` entry of the spec file is exported to
//! `<out>/data/chunk-000/episode_<index:06>.parquet` (index = position in the file)
//! with dataset metadata under `<out>/meta/`.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use rti_core::Config;
use rti_db::Db;
use rti_export::{export_episode, load_spec_file, LeRobotParquetSink};

const USAGE: &str = "usage: rti-export --db PATH --spec episodes.toml --out dataset/";

/// Minimal hand-rolled argument parser (no external CLI dependency).
fn parse_args() -> Result<(PathBuf, PathBuf, PathBuf), String> {
    let mut db = None;
    let mut spec = None;
    let mut out = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--db" => db = Some(args.next().ok_or("--db requires a value")?),
            "--spec" => spec = Some(args.next().ok_or("--spec requires a value")?),
            "--out" => out = Some(args.next().ok_or("--out requires a value")?),
            "-h" | "--help" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown argument {other:?}\n{USAGE}")),
        }
    }
    match (db, spec, out) {
        (Some(db), Some(spec), Some(out)) => Ok((PathBuf::from(db), PathBuf::from(spec), PathBuf::from(out))),
        _ => Err(USAGE.to_string()),
    }
}

fn run() -> rti_export::Result<()> {
    let (db_path, spec_path, out) = parse_args().map_err(rti_export::Error::Spec)?;

    let specs = load_spec_file(&spec_path)?;
    if specs.is_empty() {
        return Err(rti_export::Error::Spec(format!(
            "{}: no [[episode]] entries",
            spec_path.display()
        )));
    }

    let cfg = Config { data_dir: Some(db_path), ..Config::default() };
    let db = Db::open(cfg)?;

    for (i, spec) in specs.iter().enumerate() {
        let sink = LeRobotParquetSink::new(&out, i as u32, spec)?;
        let meta = export_episode(&db, spec, sink)?;
        let samples: u64 = meta.series_samples.iter().map(|(_, n)| n).sum();
        println!(
            "episode {i} {:?}: {} frames, {samples} original samples -> {}",
            meta.name,
            meta.frames,
            meta.path.display()
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rti-export: {e}");
            ExitCode::FAILURE
        }
    }
}
