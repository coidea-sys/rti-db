//! S1 perf smoke (NOT a hard CI gate): reports the mean and p99 latency of `latest()`
//! over 10k calls against a 100k-point series. Run with
//! `cargo test -p rti-vla --release --test latest_perf -- --nocapture`.
//!
//! Since v0.8 `latest()` is O(1) (rti-db's per-series head index, SPEC §3), so latency
//! is independent of the series' sample count. SPEC §4 lists the p99 < 1 µs check as a
//! **release validation** gate on the validation host — this smoke prints the numbers
//! for the record but deliberately makes no timing assertion (shared CI hosts are too
//! noisy for a hard sub-microsecond gate).

use std::time::{Duration, Instant};

use rti_core::{Config, Error, Sample};
use rti_db::Db;
use rti_vla::latest;

#[test]
fn latest_perf_smoke_10k_calls() {
    let mut cfg = Config::deterministic();
    cfg.memtable_max = 1 << 20;
    let db = Db::open(cfg).unwrap();
    for i in 0..100_000i64 {
        loop {
            match db.put(1, Sample::new(i, i as f64)) {
                Ok(()) => break,
                Err(Error::SeriesFull) => std::thread::yield_now(),
                Err(e) => panic!("put failed: {e}"),
            }
        }
    }
    db.flush().unwrap();

    // warmup (ingest pipeline steady state + head-index lookups warmed)
    for _ in 0..1_000 {
        std::hint::black_box(latest(&db, 1).unwrap());
    }

    const N: usize = 10_000;
    let mut lat = Vec::with_capacity(N);
    for _ in 0..N {
        let c0 = Instant::now();
        std::hint::black_box(latest(&db, 1).unwrap());
        lat.push(c0.elapsed());
    }
    lat.sort_unstable();
    let mean: Duration = lat.iter().sum::<Duration>() / N as u32;
    let p99 = lat[N * 99 / 100];
    let max = lat[N - 1];
    println!(
        "latest() perf smoke: mean = {mean:?}, p99 = {p99:?}, max = {max:?} over {N} calls \
         (100k-point series; release-validation target: p99 < 1 us on the validation host — informational only here)"
    );
}
