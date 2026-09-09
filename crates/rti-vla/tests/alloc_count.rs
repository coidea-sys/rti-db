//! v0.7 S1 reflex-path allocation verification (feature `alloc-count`, standalone test binary).
//!
//! Uses the rti-db/rti-mem global counting allocator to verify: **zero heap-allocation
//! growth on steady-state `latest()` calls under concurrent ingest load**, within the
//! documented envelope (series of at most 256 samples — the std stable-sort small-slice
//! threshold inside rti-db's collect path; see the rti-vla crate rustdoc). A standalone
//! binary guarantees a single test process, so counts are not disturbed by other tests.

#![cfg(feature = "alloc-count")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rti_core::{Config, Error, Sample};
use rti_db::{alloc_count, CountingAllocator, Db};
use rti_vla::latest;

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
fn latest_steady_state_zero_alloc_growth_under_ingest() {
    let mut cfg = Config::deterministic();
    // large capacity: no LRU eviction during warmup + measurement.
    cfg.memtable_max = 1 << 20;
    let db = Arc::new(Db::open(cfg).unwrap());

    // The S1 working set under test: 128 points, well within the zero-alloc envelope.
    for i in 0..128i64 {
        put_retry(&db, 0, Sample::new(i, 1.0));
    }
    // Pre-grow the ingest-side series 1..=3 (the allocation counter is process-global,
    // so the writer thread must also be at steady state — same pattern as rti-db's own
    // alloc test): 100k points per series -> per-series Vec capacity 131072; the writer
    // below adds only 20k more per series, so no buffer grows during measurement.
    for i in 0..300_000i64 {
        put_retry(&db, 1 + (i % 3) as u32, Sample::new(i, 2.0));
    }
    db.flush().unwrap();

    // warmup: the thread-local result buffer reaches steady-state capacity.
    for _ in 0..1_000 {
        std::hint::black_box(latest(&db, 0).unwrap());
    }

    // concurrent ingest load on OTHER series during the measurement window
    // (mirrors the SPEC §5 VLA gate shape: reads racing a busy ingest pipeline).
    // Bounded to 60k puts (20k per series) so the pre-grown capacities cannot be exceeded.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut i = 0i64;
            while i < 60_000 {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                put_retry(&db, 1 + (i % 3) as u32, Sample::new(300_000 + i, 3.0));
                i += 1;
            }
            while !stop.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
        })
    };

    let before = alloc_count();
    for _ in 0..10_000 {
        let s = latest(&db, 0).unwrap();
        std::hint::black_box(s);
    }
    let after = alloc_count();

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    assert_eq!(
        after, before,
        "steady-state latest() must show 0 heap-allocation growth under ingest load (before={before}, after={after})"
    );
}
