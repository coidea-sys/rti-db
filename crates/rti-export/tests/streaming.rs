//! Acceptance test 3: streaming / O(chunk) behavior.
//!
//! The export pipeline must feed the sink in bounded chunks (<= CHUNK_FRAMES frames)
//! so peak memory does not grow linearly with episode length. We assert this via
//! chunk counting: the number of `write_chunk` calls scales linearly with the frame
//! count while every chunk stays bounded, for both grid modes.

mod common;

use std::path::PathBuf;

use rti_core::Sample;
use rti_export::{export_episode, Chunk, EpisodeSink, EpisodeSpec, Result, CHUNK_FRAMES};

use common::{mem_db, put_retry};

/// Sink that only records chunk shapes (values are dropped immediately).
#[derive(Default)]
struct CountingSink {
    chunk_frames: Vec<usize>,
}

impl EpisodeSink for CountingSink {
    fn write_chunk(&mut self, cols: &Chunk) -> Result<()> {
        assert!(!cols.ts.is_empty(), "empty chunks must never be emitted");
        assert!(
            cols.ts.len() <= CHUNK_FRAMES,
            "chunk of {} frames exceeds CHUNK_FRAMES={CHUNK_FRAMES}",
            cols.ts.len()
        );
        self.chunk_frames.push(cols.ts.len());
        Ok(())
    }
    fn close(self) -> Result<PathBuf> {
        Ok(PathBuf::from("/dev/null"))
    }
}

fn sparse_db(n_samples: i64, span_ns: i64) -> rti_db::Db {
    let db = mem_db(1 << 16);
    for i in 0..n_samples {
        put_retry(&db, 5, Sample::new(i * span_ns / n_samples, i as f64));
    }
    db.flush().unwrap();
    db
}

fn frames_for(hz: u32, span_ns: i64) -> u64 {
    span_ns as u64 * hz as u64 / 1_000_000_000 + 1
}

fn expected_chunks(frames: u64) -> u64 {
    frames.div_ceil(CHUNK_FRAMES as u64)
}

#[test]
fn chunk_count_scales_with_episode_not_chunk_size() {
    // 10kHz grid; ~100k frames vs ~200k frames of episode, only ~1k samples each:
    // the sample count stays tiny while the chunk count must double.
    let hz = 10_000u32;
    for (span_ns, label) in [(1_000_000_000i64, "1s"), (2_000_000_000, "2s")] {
        let db = sparse_db(1_000, span_ns);
        let spec = EpisodeSpec {
            name: label.into(),
            series: vec![("s".into(), 5)],
            start: 0,
            end: span_ns,
            frame_hz: Some(hz),
        };
        let meta = export_episode(&db, &spec, CountingSink::default()).unwrap();
        let frames = frames_for(hz, span_ns);
        assert_eq!(meta.frames, frames, "{label} frame count");
        // sink is consumed; re-run through a shared recorder to inspect chunk shapes.
        let mut sink = CountingSink::default();
        struct Borrow<'a>(&'a mut CountingSink);
        impl EpisodeSink for Borrow<'_> {
            fn write_chunk(&mut self, cols: &Chunk) -> Result<()> {
                self.0.write_chunk(cols)
            }
            fn close(self) -> Result<PathBuf> {
                Ok(PathBuf::from("/dev/null"))
            }
        }
        export_episode(&db, &spec, Borrow(&mut sink)).unwrap();
        assert_eq!(
            sink.chunk_frames.len() as u64,
            expected_chunks(frames),
            "{label}: one write_chunk call per {CHUNK_FRAMES} frames"
        );
        assert_eq!(sink.chunk_frames.iter().sum::<usize>() as u64, frames);
        // all chunks but the last are exactly CHUNK_FRAMES
        for (i, &n) in sink.chunk_frames[..sink.chunk_frames.len() - 1].iter().enumerate() {
            assert_eq!(n, CHUNK_FRAMES, "{label} chunk {i}");
        }
    }
}

#[test]
fn union_grid_also_streams_in_bounded_chunks() {
    // frame_hz = None: union grid = every sample timestamp; 20k samples -> 3 chunks.
    let n = 20_000i64;
    let db = sparse_db(n, n * 1_000);
    let spec = EpisodeSpec {
        name: "union".into(),
        series: vec![("s".into(), 5)],
        start: 0,
        end: (n - 1) * 1_000,
        frame_hz: None,
    };
    let mut sink = CountingSink::default();
    struct Borrow<'a>(&'a mut CountingSink);
    impl EpisodeSink for Borrow<'_> {
        fn write_chunk(&mut self, cols: &Chunk) -> Result<()> {
            self.0.write_chunk(cols)
        }
        fn close(self) -> Result<PathBuf> {
            Ok(PathBuf::from("/dev/null"))
        }
    }
    let meta = export_episode(&db, &spec, Borrow(&mut sink)).unwrap();
    assert_eq!(meta.frames, n as u64);
    assert_eq!(sink.chunk_frames.len() as u64, expected_chunks(n as u64));
    assert_eq!(sink.chunk_frames.iter().sum::<usize>(), n as usize);
}
