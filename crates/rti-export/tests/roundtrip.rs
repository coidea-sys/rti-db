//! Acceptance test 1: 1M-point round-trip.
//!
//! 3 series, 1_000_000 samples in total -> export (no resampling: union grid, so every
//! original timestamp is a frame) -> read the Parquet file back with arrow2/parquet2 ->
//! the (ts, value) samples must match point for point.

mod common;

use std::fs::File;

use arrow2::array::{Float64Array, Int64Array};
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};

use rti_core::Sample;
use rti_export::{export_episode, EpisodeSpec, ExportMeta, LeRobotParquetSink};

use common::{mem_db, put_retry};

const SERIES: [u32; 3] = [11, 22, 33];
/// 333_334 + 333_333 + 333_333 = 1_000_000.
const COUNTS: [usize; 3] = [333_334, 333_333, 333_333];
const TS_STEP: i64 = 1_000;

fn value(series_idx: usize, i: usize) -> f64 {
    series_idx as f64 * 1e9 + i as f64
}

/// Read `timestamp` + all Float64 columns back from an episode Parquet file.
fn read_parquet(path: &std::path::Path) -> (Vec<i64>, Vec<Vec<f64>>) {
    let mut file = File::open(path).unwrap();
    let metadata = read_metadata(&mut file).unwrap();
    let schema = infer_schema(&metadata).unwrap();
    let n_cols = schema.fields.len();
    assert_eq!(schema.fields[0].name, "timestamp");
    let reader = FileReader::new(file, metadata.row_groups, schema, None, None, None);

    let mut ts: Vec<i64> = Vec::new();
    let mut cols: Vec<Vec<f64>> = vec![Vec::new(); n_cols - 1];
    for chunk in reader {
        let chunk = chunk.unwrap();
        let arrays = chunk.arrays();
        let t = arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();
        ts.extend(t.values_iter());
        for (c, arr) in arrays[1..].iter().enumerate() {
            let v = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            cols[c].extend(v.iter().map(|x| x.copied().unwrap_or(f64::NAN)));
        }
    }
    (ts, cols)
}

#[test]
fn million_point_round_trip_no_resample() {
    let total: usize = COUNTS.iter().sum();
    assert_eq!(total, 1_000_000, "hard requirement: exactly 1M points");

    let db = mem_db(1 << 21); // 2M slots: no LRU eviction
    for (s, (&id, &count)) in SERIES.iter().zip(&COUNTS).enumerate() {
        for i in 0..count {
            put_retry(&db, id, Sample::new(i as i64 * TS_STEP, value(s, i)));
        }
    }
    db.flush().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let spec = EpisodeSpec {
        name: "round_trip".into(),
        series: SERIES.iter().enumerate().map(|(s, id)| (format!("col{s}"), *id)).collect(),
        start: 0,
        end: (COUNTS[0] as i64 - 1) * TS_STEP,
        frame_hz: None,
    };
    let sink = LeRobotParquetSink::new(dir.path(), 0, &spec).unwrap();
    let meta: ExportMeta = export_episode(&db, &spec, sink).unwrap();

    // union grid: one frame per distinct timestamp (series 0 is the longest).
    assert_eq!(meta.frames, COUNTS[0] as u64);
    assert_eq!(meta.name, "round_trip");
    for (s, ((name, samples), &count)) in meta.series_samples.iter().zip(&COUNTS).enumerate() {
        assert_eq!(name, &format!("col{s}"));
        assert_eq!(*samples, count as u64, "col{s} original sample count");
    }

    // read back and compare point for point.
    let (ts, cols) = read_parquet(&meta.path);
    assert_eq!(ts.len(), COUNTS[0]);
    for (f, &t) in ts.iter().enumerate() {
        assert_eq!(t, f as i64 * TS_STEP, "frame {f} timestamp");
    }
    for (s, &count) in COUNTS.iter().enumerate() {
        assert_eq!(cols[s].len(), COUNTS[0]);
        for (i, &v) in cols[s].iter().enumerate() {
            // shared timestamps -> LVCF is identity inside the series' own range;
            // past the last sample the final value is carried forward.
            let expected = value(s, i.min(count - 1));
            assert_eq!(v, expected, "col{s} frame {i}");
        }
    }
}

#[test]
fn round_trip_with_frame_hz_matches_lvcf() {
    // One series, 1MHz samples over 100ms, exported at 100kHz: every 10th sample lands
    // on the grid; frames in between must carry the last value forward.
    let db = mem_db(1 << 17); // > 100_001 samples: no LRU eviction
    for i in 0..=100_000i64 {
        put_retry(&db, 7, Sample::new(i * 1_000, i as f64)); // 1µs step
    }
    db.flush().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let spec = EpisodeSpec {
        name: "hz".into(),
        series: vec![("s".into(), 7)],
        start: 0,
        end: 100_000_000, // 100ms
        frame_hz: Some(100_000),
    };
    let sink = LeRobotParquetSink::new(dir.path(), 0, &spec).unwrap();
    let meta = export_episode(&db, &spec, sink).unwrap();

    let (ts, cols) = read_parquet(&meta.path);
    assert_eq!(ts.len(), 10_001); // 100ms at 100Hz, inclusive
    for (f, (&t, &v)) in ts.iter().zip(&cols[0]).enumerate() {
        assert_eq!(t, f as i64 * 10_000, "frame {f} grid ts"); // 100kHz = 10µs step
        // last sample with ts <= t: samples every 1µs -> sample index t / 1µs = f * 10.
        assert_eq!(v, (f * 10) as f64, "frame {f} LVCF value");
    }
}
