//! v0.8 O(1) latest integration tests (SPEC §3/§4): `rti_vla::latest` delegates to
//! `Db::latest`, so these tests pin the delegated semantics through the rti-vla facade
//! on a persistent profile — scan-last parity for unique timestamps across automatic
//! seals, a manual seal, durable writes, and reopen; duplicate-timestamp rules; and the
//! documented cold-only archived-series limitation.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rti_core::{Config, Sample, SyncPolicy};
use rti_db::Db;
use rti_store::LocalFsColdTier;
use rti_vla::latest;

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "rti-vla-latest-test-{}-{}-{}",
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
/// ordered writes, automatic seals (small memtable), durable writes, a manual seal,
/// and reopen.
#[test]
fn latest_matches_scan_last_across_seals_reopen_and_durable_writes() {
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
        assert_eq!(latest(&db, 1).unwrap(), Some(Sample::new(1000, 1000.5)));
        db.flush().unwrap();
        assert!(
            db.segment_count() >= 3,
            "automatic seals must have occurred"
        );
        for s in [1u32, 2] {
            let scan_last = db.scan(s, 0, i64::MAX, None, None).unwrap().last();
            assert_eq!(
                latest(&db, s).unwrap(),
                scan_last,
                "series {s}: latest must match scan.last"
            );
        }
        assert_eq!(latest(&db, 9).unwrap(), None, "unknown series");
        // the head stays valid across a manual seal (memtable -> segments)
        db.seal().unwrap();
        assert_eq!(latest(&db, 1).unwrap(), Some(Sample::new(1000, 1000.5)));
        assert_eq!(latest(&db, 2).unwrap(), Some(Sample::new(1998, 1998.0)));
    }
    // reopen: the index is rebuilt from the pre-read segments + WAL replay
    {
        let db = Db::open(cfg).unwrap();
        assert_eq!(latest(&db, 1).unwrap(), Some(Sample::new(1000, 1000.5)));
        for s in [1u32, 2] {
            let scan_last = db.scan(s, 0, i64::MAX, None, None).unwrap().last();
            assert_eq!(
                latest(&db, s).unwrap(),
                scan_last,
                "series {s}: after reopen"
            );
        }
    }
    std::fs::remove_dir_all(&d).ok();
}

/// SPEC §3/§4: duplicate timestamps pin the maximum timestamp and visibility; within
/// one layer the first writer wins (scan-compatible); the cross-layer value choice is
/// unspecified after a seal boundary, so only the ts is pinned there.
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
        let h = latest(&db, 1).unwrap().expect("visible after flush");
        assert_eq!(h.ts, 100, "the maximum timestamp is pinned");
        assert_eq!(h.value, 1.0, "same-layer duplicates keep first-writer-wins");
        db.seal().unwrap();
        // cross-layer duplicate value choice is unspecified: pin ts + visibility only
        let h = latest(&db, 1).unwrap().expect("visible after seal");
        assert_eq!(h.ts, 100, "seal must not change the pinned max ts");
    }
    {
        let db = Db::open(cfg).unwrap();
        let h = latest(&db, 1).unwrap().expect("visible after reopen");
        assert_eq!(h.ts, 100, "reopen must keep the pinned max ts");
    }
    std::fs::remove_dir_all(&d).ok();
}

/// SPEC §3 documented limitation: archived (cold-only) segments are not pre-read at
/// open, so a series whose samples are exclusively archived reports `None` from
/// latest() after a reopen, even though scan() reads them back transparently from the
/// cold tier. This test pins the disclosure in the rti-vla rustdoc.
#[test]
fn latest_cold_only_archived_series_reports_none_after_reopen() {
    let d = tmpdir("cold-only");
    let data = d.join("data");
    let cold_dir = d.join("cold");
    let cfg = config(data.clone(), 1 << 10);
    {
        let db = Db::open(cfg.clone()).unwrap();
        for i in 0..4i64 {
            db.put(1, Sample::new(i, i as f64)).unwrap();
            db.seal().unwrap();
        }
        assert_eq!(latest(&db, 1).unwrap(), Some(Sample::new(3, 3.0)));
        db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
        assert_eq!(db.archive_older_than(i64::MAX - 1).unwrap(), 4);
    }
    {
        let db = Db::open(cfg).unwrap();
        db.set_cold_tier(Arc::new(LocalFsColdTier::new(&cold_dir).unwrap()));
        // scan still reads the archived samples back transparently...
        let got: Vec<Sample> = db.scan(1, 0, i64::MAX, None, None).unwrap().collect();
        assert_eq!(
            got,
            (0..4i64)
                .map(|i| Sample::new(i, i as f64))
                .collect::<Vec<_>>()
        );
        // ...but latest() has no head for a cold-only series (documented limitation)
        assert_eq!(
            latest(&db, 1).unwrap(),
            None,
            "a series living only in archived segments reports None after reopen"
        );
        // rewriting the series installs a fresh head again
        db.put_durable(1, Sample::new(4, 4.0), Duration::from_secs(5))
            .unwrap();
        assert_eq!(latest(&db, 1).unwrap(), Some(Sample::new(4, 4.0)));
    }
    std::fs::remove_dir_all(&d).ok();
}
