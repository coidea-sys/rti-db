//! S1 perf smoke (NOT a hard gate): prints the mean latency of `latest()` over 1k
//! calls. Run with `cargo test -p rti-vla --release --test latest_perf -- --nocapture`.
//!
//! Context: `latest()` is currently O(n) in the series' sample count (see the crate
//! rustdoc), so the < 1 µs target of SPEC §4 is only meaningful for small working
//! series; this test uses a 64-point series as the S1 working-set proxy.

use std::time::Instant;

use rti_core::{Config, Error, Sample};
use rti_db::Db;
use rti_vla::latest;

#[test]
fn latest_perf_smoke_1k_calls() {
    let mut cfg = Config::deterministic();
    cfg.memtable_max = 1 << 16;
    let db = Db::open(cfg).unwrap();
    for i in 0..64i64 {
        loop {
            match db.put(1, Sample::new(i, i as f64)) {
                Ok(()) => break,
                Err(Error::SeriesFull) => std::thread::yield_now(),
                Err(e) => panic!("put failed: {e}"),
            }
        }
    }
    db.flush().unwrap();

    // warmup (TLS buffer init + ingest pipeline steady state)
    for _ in 0..1_000 {
        std::hint::black_box(latest(&db, 1).unwrap());
    }

    const N: u32 = 1_000;
    let t0 = Instant::now();
    let mut p99_bound = std::time::Duration::ZERO;
    for _ in 0..N {
        let c0 = Instant::now();
        std::hint::black_box(latest(&db, 1).unwrap());
        p99_bound = p99_bound.max(c0.elapsed());
    }
    let mean = t0.elapsed() / N;
    println!(
        "latest() perf smoke: mean = {mean:?} over {N} calls (64-point series), max = {p99_bound:?} (target: mean < 1 us, informational only)"
    );
}
