//! Acceptance test 2: `frame_hz` resampling aligns two different-rate series to a
//! common 50Hz grid with last-value-carry-forward semantics, and `meta/stats.json`
//! records the per-frame original sample counts.

mod common;

use std::path::PathBuf;

use rti_core::Sample;
use rti_export::{export_episode, Chunk, EpisodeSink, EpisodeSpec, LeRobotParquetSink, Result};

use common::{mem_db, put_retry};

const MS: i64 = 1_000_000;

/// In-memory sink recording every chunk (no Parquet involved).
#[derive(Default)]
struct VecSink {
    chunks: Vec<Chunk>,
}

impl VecSink {
    fn record(&mut self, cols: &Chunk) {
        self.chunks.push(Chunk {
            ts: cols.ts.clone(),
            cols: cols.cols.iter().map(|(n, v)| (n.clone(), v.clone())).collect(),
            frame_samples: cols.frame_samples.clone(),
            series_samples: cols.series_samples.clone(),
        });
    }
}

/// Borrowing adapter so the recorded chunks stay accessible after the export.
struct Borrow<'a>(&'a mut VecSink);

impl EpisodeSink for Borrow<'_> {
    fn write_chunk(&mut self, cols: &Chunk) -> Result<()> {
        self.0.record(cols);
        Ok(())
    }
    fn close(self) -> Result<PathBuf> {
        Ok(PathBuf::from("/dev/null"))
    }
}

/// Engine with series "fast" (100Hz, 101 samples over 1s, value = index) and
/// series "slow" (25Hz, 26 samples over 1s, value = 1000 + index).
fn two_rate_db() -> rti_db::Db {
    let db = mem_db(1 << 10);
    for i in 0..=100i64 {
        put_retry(&db, 1, Sample::new(i * 10 * MS, i as f64));
    }
    for i in 0..=25i64 {
        put_retry(&db, 2, Sample::new(i * 40 * MS, 1000.0 + i as f64));
    }
    db.flush().unwrap();
    db
}

fn spec_50hz() -> EpisodeSpec {
    EpisodeSpec {
        name: "align".into(),
        series: vec![("fast".into(), 1), ("slow".into(), 2)],
        start: 0,
        end: 1_000 * MS, // 1s
        frame_hz: Some(50),
    }
}

#[test]
fn two_rates_aligned_to_50hz_grid() {
    let db = two_rate_db();
    let mut sink = VecSink::default();
    let meta = export_episode(&db, &spec_50hz(), Borrow(&mut sink)).unwrap();

    assert_eq!(meta.frames, 51); // 0ms, 20ms, ..., 1000ms
    assert_eq!(meta.series_samples, vec![("fast".to_string(), 101), ("slow".to_string(), 26)]);

    assert_eq!(sink.chunks.len(), 1, "51 frames fit into one chunk");
    let chunk = &sink.chunks[0];

    // grid: exactly 0, 20, 40, ..., 1000 ms
    for (f, &t) in chunk.ts.iter().enumerate() {
        assert_eq!(t, f as i64 * 20 * MS, "frame {f} grid timestamp");
    }

    let fast = &chunk.cols[0].1;
    let slow = &chunk.cols[1].1;
    for f in 0..=50usize {
        // fast (100Hz, samples every 10ms): exact sample at every 20ms grid point -> value 2f.
        assert_eq!(fast[f], (2 * f) as f64, "fast frame {f}");
        // slow (25Hz, samples every 40ms): last sample <= 20f ms is index floor(f/2).
        assert_eq!(slow[f], 1000.0 + (f / 2) as f64, "slow frame {f} (LVCF)");
    }

    // per-frame original sample counts:
    // frame 0 folds the ts=0 samples of both series -> 2.
    // frame f>=1: fast contributes 2 (at 20f-10 and 20f ms); slow contributes 1 iff f is even.
    assert_eq!(chunk.frame_samples[0], 2);
    for f in 1..=50usize {
        let expect = 2 + u32::from(f % 2 == 0);
        assert_eq!(chunk.frame_samples[f], expect, "frame_samples[{f}]");
    }
    assert_eq!(chunk.series_samples, vec![101, 26]);
}

#[test]
fn stats_json_records_original_sample_counts() {
    let db = two_rate_db();
    let dir = tempfile::tempdir().unwrap();
    let spec = spec_50hz();
    let sink = LeRobotParquetSink::new(dir.path(), 0, &spec).unwrap();
    let meta = export_episode(&db, &spec, sink).unwrap();
    assert_eq!(meta.frames, 51);

    let stats: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("meta/stats.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(stats["episode"], "align");
    assert_eq!(stats["frame_hz"], 50);
    assert_eq!(stats["frames"], 51);
    assert_eq!(stats["series_samples"]["fast"], 101);
    assert_eq!(stats["series_samples"]["slow"], 26);

    let frame_samples: Vec<u64> =
        serde_json::from_value(stats["frame_samples"].clone()).unwrap();
    assert_eq!(frame_samples.len(), 51);
    assert_eq!(frame_samples[0], 2);
    for (f, &n) in frame_samples.iter().enumerate().skip(1) {
        let expect = 2 + u64::from(f % 2 == 0);
        assert_eq!(n, expect, "frame_samples[{f}]");
    }
    let total: u64 = frame_samples.iter().sum();
    assert_eq!(total, 101 + 26, "every original sample accounted exactly once");

    // per-column stats over the resampled values: fast goes 0, 2, 4, ..., 100.
    assert_eq!(stats["columns"]["fast"]["min"], 0.0);
    assert_eq!(stats["columns"]["fast"]["max"], 100.0);
    assert_eq!(stats["columns"]["fast"]["count"], 51);
}
