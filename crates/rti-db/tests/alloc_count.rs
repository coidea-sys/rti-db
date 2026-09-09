//! v0.3 hot-path allocation counting verification (feature `alloc-count`, standalone test binary).
//!
//! Uses a global counting allocator to verify: **zero heap-allocation growth on the Deterministic steady-state put hot path**.
//! A standalone binary guarantees a single test process, so counts are not disturbed by other tests.

#![cfg(feature = "alloc-count")]

use rti_core::{Config, Error, Sample};
use rti_db::{alloc_count, CountingAllocator, Db};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn put_retry(db: &Db, series: u32, s: Sample) {
    loop {
        match db.put(series, s) {
            Ok(()) => return,
            Err(Error::SeriesFull) => std::thread::yield_now(), // backpressure retry
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

#[test]
fn deterministic_put_steady_state_zero_alloc_growth() {
    let mut cfg = Config::deterministic();
    // capacity large enough: no LRU eviction during warmup + measurement window, series Vecs no longer grow.
    cfg.memtable_max = 1 << 20;
    let db = Db::open(cfg).unwrap();

    const SERIES: i64 = 4;
    // warmup 600k: ring pipeline / ingest batch buffers / series Vec capacities all reach steady state
    // (150k samples per series -> Vec capacity 262144; measurement adds only 50k more).
    for i in 0..600_000i64 {
        put_retry(&db, (i % SERIES) as u32, Sample::new(i, 1.0));
    }
    db.flush().unwrap();

    let before = alloc_count();
    for j in 0..200_000i64 {
        put_retry(&db, (j % SERIES) as u32, Sample::new(600_000 + j, 2.0));
    }
    db.flush().unwrap();
    let after = alloc_count();

    assert_eq!(
        after, before,
        "steady-state put hot path must show 0 heap-allocation growth (before={before}, after={after})"
    );

    // data integrity spot check (also covers that LRU did not fire spuriously)
    assert_eq!(db.lru_evictions(), 0, "no eviction should occur under large capacity");
    let got: Vec<Sample> = db.scan(3, 799_990, 800_000, None, None).unwrap().collect();
    assert!(!got.is_empty());
}

/// v0.8 SPEC §4: 100k-point series, 10k `latest()` calls — zero heap-allocation growth
/// on the O(1) read path (a single hash lookup; no flush, no decode, no I/O).
#[test]
fn latest_read_path_zero_alloc_growth() {
    let mut cfg = Config::deterministic();
    cfg.memtable_max = 1 << 17; // fits the whole 100k-point series; no LRU eviction
    let db = Db::open(cfg).unwrap();

    for i in 0..100_000i64 {
        put_retry(&db, 1, Sample::new(i, i as f64));
    }
    db.flush().unwrap();
    let want = Some(Sample::new(99_999, 99_999.0));
    assert_eq!(db.latest(1).unwrap(), want); // warm-up (first call)

    let before = alloc_count();
    for _ in 0..10_000 {
        assert_eq!(db.latest(1).unwrap(), want);
    }
    let after = alloc_count();

    assert_eq!(
        after, before,
        "latest() read path must show 0 heap-allocation growth (before={before}, after={after})"
    );
}
