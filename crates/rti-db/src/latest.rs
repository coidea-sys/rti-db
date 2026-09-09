//! Per-series O(1) latest index (v0.8, SPEC §3).
//!
//! The index maps each visible series to its newest known sample. Updates happen only on
//! the ingest / recovery path (ingest apply, WAL replay, segment preload at open); the
//! read path ([`Db::latest`]) is a single hash lookup — no allocation, no filesystem
//! access, no flush, no segment decode.
//!
//! Semantics (SPEC §3): only a **strictly larger** timestamp replaces the head, so at an
//! equal maximum timestamp the first known value is kept (the cross-layer duplicate value
//! choice is explicitly unspecified). LRU eviction of a whole series (Deterministic
//! profile) removes the entry, mirroring scan visibility.

use std::collections::HashMap;

use rti_core::{Result, Sample, SeriesId};
use rti_store::SegmentReader;

use crate::Db;

/// Per-series head index: `SeriesId → newest known Sample`.
///
/// Bounded by the number of visible series. Never reverse-decodes the compressed segment
/// format: rebuild at open decodes each pre-read segment forward once.
#[derive(Default)]
pub struct LatestIndex {
    heads: HashMap<SeriesId, Sample>,
}

impl LatestIndex {
    /// An empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a newly visible sample: replaces the head only on a strictly larger
    /// timestamp (first-known value kept at an equal maximum ts).
    ///
    /// Hot-path note: an existing series is a `get_mut` (no allocation); only a series'
    /// first-ever sample inserts into the map.
    pub fn apply(&mut self, series: SeriesId, sample: Sample) {
        match self.heads.get_mut(&series) {
            Some(h) => {
                if sample.ts > h.ts {
                    *h = sample;
                }
            }
            None => {
                self.heads.insert(series, sample);
            }
        }
    }

    /// Drop a series entirely (Deterministic LRU whole-series eviction).
    pub fn remove(&mut self, series: SeriesId) {
        self.heads.remove(&series);
    }

    /// O(1) read: newest known sample for `series` (copied out, no allocation).
    pub fn get(&self, series: SeriesId) -> Option<Sample> {
        self.heads.get(&series).copied()
    }

    /// Rebuild the head of one pre-read segment by decoding it forward once and applying
    /// the sample at its maximum timestamp (open path only, SPEC §3: O(total persisted
    /// samples) at open, no file I/O beyond the existing segment preload). At an equal
    /// maximum ts inside the segment the first decoded sample wins (scan-compatible).
    pub fn apply_segment(&mut self, reader: &SegmentReader) -> Result<()> {
        let mut best: Option<Sample> = None;
        for s in reader.iter()? {
            if best.map(|b| s.ts > b.ts).unwrap_or(true) {
                best = Some(s);
            }
        }
        if let Some(s) = best {
            self.apply(reader.series(), s);
        }
        Ok(())
    }
}

/// Facade free function, matching the existing facade style (SPEC §3).
///
/// Newest visible sample for `series`, or `None` if the series is absent. O(1), no
/// filesystem access, no allocation on the read path.
pub fn latest(db: &Db, series: SeriesId) -> Result<Option<Sample>> {
    db.latest(series)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rti_core::{Config, SyncPolicy};
    use rti_store::{ColdTier, LocalFsColdTier};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-db-latest-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn config(dir: PathBuf, memtable_max: usize) -> Config {
        Config {
            data_dir: Some(dir),
            memtable_max,
            wal_sync: SyncPolicy::Group { interval_us: 1_000 },
            ..Config::default()
        }
    }

    /// SPEC §4: for unique timestamps latest() matches scan(...).last() exactly, under
    /// ordered writes, durable writes, automatic and manual seals, and reopen.
    #[test]
    fn latest_matches_scan_last_unique_ts() {
        let d = tmpdir("unique");
        let cfg = config(d.clone(), 256);
        {
            let db = Db::open(cfg.clone()).unwrap();
            for i in 0..1000i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
                db.put(2, Sample::new(i * 2, (i * 2) as f64)).unwrap();
            }
            // durable-write visibility: after put_durable returns, latest() must see the sample
            db.put_durable(1, Sample::new(1000, 1000.5), Duration::from_secs(5))
                .unwrap();
            assert_eq!(db.latest(1).unwrap(), Some(Sample::new(1000, 1000.5)));
            db.flush().unwrap();
            assert!(
                db.segment_count() >= 3,
                "automatic seals must have occurred"
            );
            for s in [1u32, 2] {
                let scan_last = db.scan(s, 0, i64::MAX, None, None).unwrap().last();
                assert_eq!(
                    db.latest(s).unwrap(),
                    scan_last,
                    "series {s}: latest must match scan.last"
                );
            }
            assert_eq!(db.latest(9).unwrap(), None, "unknown series");
            // the head stays valid across a manual seal (memtable -> segments)
            db.seal().unwrap();
            assert_eq!(db.latest(1).unwrap(), Some(Sample::new(1000, 1000.5)));
            assert_eq!(db.latest(2).unwrap(), Some(Sample::new(1998, 1998.0)));
        }
        // reopen: the index is rebuilt from the pre-read segments + WAL replay
        {
            let db = Db::open(cfg).unwrap();
            assert_eq!(db.latest(1).unwrap(), Some(Sample::new(1000, 1000.5)));
            for s in [1u32, 2] {
                let scan_last = db.scan(s, 0, i64::MAX, None, None).unwrap().last();
                assert_eq!(db.latest(s).unwrap(), scan_last, "series {s}: after reopen");
            }
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §4: duplicate timestamps pin the maximum timestamp and visibility; within one
    /// layer the first writer wins (scan-compatible); the cross-layer value choice is
    /// unspecified after a seal boundary.
    #[test]
    fn latest_duplicate_ts_pins_max_ts_and_visibility() {
        let d = tmpdir("dup");
        let cfg = config(d.clone(), 64);
        {
            let db = Db::open(cfg.clone()).unwrap();
            db.put(1, Sample::new(100, 1.0)).unwrap();
            db.put(1, Sample::new(100, 2.0)).unwrap(); // duplicate ts, same layer
            db.put(1, Sample::new(50, 0.5)).unwrap(); // older ts must not move the head
            db.flush().unwrap();
            let h = db.latest(1).unwrap().expect("visible after flush");
            assert_eq!(h.ts, 100, "the maximum timestamp is pinned");
            assert_eq!(h.value, 1.0, "same-layer duplicates keep first-writer-wins");
            db.seal().unwrap();
            let h = db.latest(1).unwrap().expect("visible after seal");
            assert_eq!(h.ts, 100, "seal must not change the pinned max ts");
        }
        {
            let db = Db::open(cfg).unwrap();
            let h = db.latest(1).unwrap().expect("visible after reopen");
            assert_eq!(h.ts, 100, "reopen must keep the pinned max ts");
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §3/§4: under the Deterministic profile, a series evicted as a whole by LRU is
    /// dropped from the index — latest() follows scan visibility and returns None.
    #[test]
    fn latest_none_after_deterministic_lru_eviction() {
        let mut cfg = Config::deterministic();
        cfg.memtable_max = 4;
        let db = Db::open(cfg).unwrap();
        db.put(1, Sample::new(1, 1.0)).unwrap(); // tick 1
        db.put(2, Sample::new(1, 2.0)).unwrap(); // tick 2
        db.put(1, Sample::new(2, 1.5)).unwrap(); // tick 3 -> series 1 newer
        db.put(3, Sample::new(1, 3.0)).unwrap(); // tick 4, table full
        db.flush().unwrap();
        assert_eq!(db.latest(1).unwrap(), Some(Sample::new(2, 1.5)));
        assert_eq!(db.latest(2).unwrap(), Some(Sample::new(1, 2.0)));

        // 5th sample evicts series 2 (least-recently-written)
        db.put(3, Sample::new(2, 3.5)).unwrap();
        db.flush().unwrap();
        assert_eq!(db.lru_evictions(), 1);
        assert_eq!(
            db.latest(2).unwrap(),
            None,
            "an LRU-evicted series must report None"
        );
        assert_eq!(
            db.scan(2, 0, i64::MAX, None, None).unwrap().count(),
            0,
            "scan agrees"
        );
        assert_eq!(db.latest(3).unwrap(), Some(Sample::new(2, 3.5)));

        // re-adding an evicted series installs the new head (evicts series 1 this time)
        db.put(2, Sample::new(5, 9.0)).unwrap();
        db.flush().unwrap();
        assert_eq!(db.latest(2).unwrap(), Some(Sample::new(5, 9.0)));
        assert_eq!(db.latest(1).unwrap(), None, "series 1 evicted in turn");
        drop(db);
    }

    /// SPEC §1 pre-fix: after reopening with some local segments deleted (as compaction
    /// will do), the sequence continues at max(NNNNNN)+1 and never reuses a number.
    #[test]
    fn seg_seq_not_reused_after_reopen() {
        let d = tmpdir("segseq");
        let cfg = config(d.clone(), 1 << 10);
        {
            let db = Db::open(cfg.clone()).unwrap();
            for i in 0..3i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
                db.seal().unwrap();
            }
            assert_eq!(db.segment_count(), 3);
            assert!(d.join("seg-000002-s000001.seg").exists());
        }
        // simulate compaction deleting superseded inputs: keep only the highest-numbered segment
        std::fs::remove_file(d.join("seg-000000-s000001.seg")).unwrap();
        std::fs::remove_file(d.join("seg-000001-s000001.seg")).unwrap();
        {
            let db = Db::open(cfg).unwrap();
            assert_eq!(db.segment_count(), 1);
            db.put(1, Sample::new(3, 3.0)).unwrap();
            db.seal().unwrap();
            db.put(1, Sample::new(4, 4.0)).unwrap();
            db.seal().unwrap();
            assert!(
                d.join("seg-000003-s000001.seg").exists(),
                "next seq must be max+1 = 3"
            );
            assert!(d.join("seg-000004-s000001.seg").exists());
            // the surviving segment was not overwritten by a reused sequence number
            let got: Vec<Sample> = db.scan(1, 0, i64::MAX, None, None).unwrap().collect();
            assert_eq!(
                got,
                vec![
                    Sample::new(2, 2.0),
                    Sample::new(3, 3.0),
                    Sample::new(4, 4.0)
                ]
            );
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §1 pre-fix: segment names recorded in archive.catalog also advance the
    /// sequence, so reopening with zero local .seg files cannot collide with cold-tier
    /// object keys.
    #[test]
    fn seg_seq_not_reused_with_archive_catalog() {
        let d = tmpdir("segseq-catalog");
        let data = d.join("data");
        let cold_dir = d.join("cold");
        let cfg = config(data.clone(), 1 << 10);
        {
            let db = Db::open(cfg.clone()).unwrap();
            for i in 0..2i64 {
                db.put(1, Sample::new(i, i as f64)).unwrap();
                db.seal().unwrap();
            }
            db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
            assert_eq!(db.archive_older_than(i64::MAX - 1).unwrap(), 2);
            assert!(data.join("archive.catalog").exists());
            // no local .seg files remain: a count-based sequence would restart at 0
            assert_eq!(
                std::fs::read_dir(&data)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().map(|x| x == "seg").unwrap_or(false))
                    .count(),
                0
            );
        }
        {
            let db = Db::open(cfg).unwrap();
            db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
            db.put(1, Sample::new(2, 2.0)).unwrap();
            db.seal().unwrap();
            assert!(
                data.join("seg-000002-s000001.seg").exists(),
                "cataloged names must advance the sequence past the cold-tier keys"
            );
            // the cold-tier objects were not clobbered and all data is still readable
            let tier = LocalFsColdTier::new(&cold_dir).unwrap();
            assert_eq!(tier.list().unwrap().len(), 2);
            let got: Vec<Sample> = db.scan(1, 0, i64::MAX, None, None).unwrap().collect();
            assert_eq!(
                got,
                (0..3i64)
                    .map(|i| Sample::new(i, i as f64))
                    .collect::<Vec<_>>()
            );
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// The facade free function delegates to Db::latest.
    #[test]
    fn latest_free_function() {
        let d = tmpdir("freefn");
        let db = Db::open(config(d.clone(), 1 << 10)).unwrap();
        db.put(3, Sample::new(7, 42.0)).unwrap();
        db.flush().unwrap();
        assert_eq!(latest(&db, 3).unwrap(), Some(Sample::new(7, 42.0)));
        assert_eq!(latest(&db, 4).unwrap(), None);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }
}
