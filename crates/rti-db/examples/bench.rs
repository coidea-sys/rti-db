//! rti-db benchmarks (std::time version, runs offline; SPEC §4.5).
//!
//! Output: put throughput, p50/p99/p999 write latency, scan throughput (1M points), compression ratio.
//!
//! Run: `cargo run --release --example bench -p rti-db`

use std::time::Instant;

use rti_core::{Config, Sample, SyncPolicy};
use rti_db::Db;
use rti_query::Agg;
use rti_store::encode::{encode_ts, encode_vals, TsDecoder, ValDecoder};
use rti_store::{decode_ts_block, decode_val_block};

const PUTS: usize = 1_000_000;
const SERIES: u32 = 8;

fn main() {
    let dir = std::env::temp_dir().join(format!("rti-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = Config {
        data_dir: Some(dir.clone()),
        memtable_max: 1 << 18, // 256K samples / MemTable
        wal_sync: SyncPolicy::Group { interval_us: 1_000 },
        pool_bytes: 16 << 20,
        ..Config::default()
    };
    let db = Db::open(cfg).unwrap();

    // ---- write path: put throughput + latency distribution -------------------------------------
    let mut lat = Vec::with_capacity(PUTS);
    let t0 = Instant::now();
    let mut i = 0usize;
    while i < PUTS {
        let s = Sample::new(i as i64 * 1_000, 20.0 + (i as f64 * 0.001).sin());
        let start = Instant::now();
        match db.put((i % SERIES as usize) as u32, s) {
            Ok(()) => {
                lat.push(start.elapsed().as_nanos() as u64);
                i += 1;
            }
            Err(rti_core::Error::SeriesFull) => std::thread::yield_now(), // backpressure
            Err(e) => panic!("put failed: {e}"),
        }
    }
    let enqueue_wall = t0.elapsed();
    db.flush().unwrap();
    let durable_wall = t0.elapsed();

    lat.sort_unstable();
    let pct = |p: f64| lat[(p * (lat.len() - 1) as f64) as usize];
    println!("== rti-db bench (std::time) ==");
    println!(
        "put enqueue : {:>12.0} ops/s   ({} ops in {:?})",
        PUTS as f64 / enqueue_wall.as_secs_f64(),
        PUTS,
        enqueue_wall
    );
    println!(
        "put durable : {:>12.0} ops/s   (incl. flush+group commit, {:?})",
        PUTS as f64 / durable_wall.as_secs_f64(),
        durable_wall
    );
    println!(
        "put latency : p50={}ns p99={}ns p999={}ns max={}ns",
        pct(0.50),
        pct(0.99),
        pct(0.999),
        lat[lat.len() - 1]
    );

    // ---- compression ratio -----------------------------------------------------------
    db.seal().unwrap();
    let mut seg_bytes = 0u64;
    let mut seg_count = 0u64;
    for e in std::fs::read_dir(&dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().map(|x| x == "seg").unwrap_or(false) {
            seg_bytes += std::fs::metadata(&p).unwrap().len();
            seg_count += 1;
        }
    }
    let raw_bytes = (PUTS * 16) as u64;
    println!(
        "compression : {} segments, {} B on disk vs {} B raw (16 B/sample), ratio {:.2}x",
        seg_count,
        seg_bytes,
        raw_bytes,
        raw_bytes as f64 / seg_bytes as f64
    );

    // ---- read path: scan throughput (1M points, with predicate and aggregation) -------------------------
    let t1 = Instant::now();
    let mut scanned = 0usize;
    for s in 0..SERIES {
        scanned += db.scan(s, 0, i64::MAX, None, None).unwrap().count();
    }
    let scan_wall = t1.elapsed();
    println!(
        "scan        : {:>12.0} pts/s   ({} pts in {:?})",
        scanned as f64 / scan_wall.as_secs_f64(),
        scanned,
        scan_wall
    );

    let t2 = Instant::now();
    let mut matched = 0usize;
    for s in 0..SERIES {
        matched += db
            .scan(s, 0, i64::MAX, Some(rti_query::Pred::Gt(20.5)), None)
            .unwrap()
            .count();
    }
    let pred_wall = t2.elapsed();
    println!(
        "scan+pred   : {:>12.0} pts/s   ({} matched in {:?})",
        PUTS as f64 / pred_wall.as_secs_f64(),
        matched,
        pred_wall
    );

    let t3 = Instant::now();
    let mut avgs = 0.0f64;
    for s in 0..SERIES {
        for smp in db.scan(s, 0, i64::MAX, None, Some(Agg::Avg)).unwrap() {
            avgs += smp.value;
        }
    }
    println!("agg (avg)   : {:?} for {} series (sum of avgs = {avgs:.3})", t3.elapsed(), SERIES);

    // ---- v0.2: scalar streaming decode vs 8-way unrolled block decode ---------------------------
    decode_bench();

    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}

/// v0.2 decode micro-benchmark: scalar streaming decoder vs 8-way unrolled block decode (same compressed column).
///
/// Takes the best of several rounds to reduce scheduling noise; `std::hint::black_box` prevents
/// the decoded results from being optimized away.
fn decode_bench() {
    const N: usize = 1_000_000;
    const ROUNDS: usize = 5;
    let samples: Vec<Sample> = (0..N)
        .map(|i| Sample::new(1_700_000_000_000_000_000 + i as i64 * 1_000, 20.0 + (i as f64 * 0.001).sin()))
        .collect();
    let mut ts_col = Vec::new();
    let mut val_col = Vec::new();
    encode_ts(&samples, &mut ts_col);
    encode_vals(&samples, &mut val_col);
    println!(
        "decode input: {} pts, ts col {} B, val col {} B",
        N,
        ts_col.len(),
        val_col.len()
    );

    // --- timestamp column ---
    let mut scalar_ts = f64::MAX;
    let mut acc = 0i64;
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let mut dec = TsDecoder::new(&ts_col).unwrap().unwrap();
        let mut n = 0usize;
        while let Some(ts) = dec.next_ts() {
            acc = acc.wrapping_add(std::hint::black_box(ts));
            n += 1;
        }
        std::hint::black_box(acc);
        assert_eq!(n, N);
        scalar_ts = scalar_ts.min(t.elapsed().as_secs_f64());
    }

    let mut out_ts = vec![0i64; N].into_boxed_slice();
    let out_ts: &mut [i64; N] = (&mut *out_ts).try_into().unwrap();
    let mut block_ts = f64::MAX;
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let n = decode_ts_block(&ts_col, out_ts).unwrap();
        std::hint::black_box(out_ts[0]);
        std::hint::black_box(out_ts[N - 1]);
        assert_eq!(n, N);
        block_ts = block_ts.min(t.elapsed().as_secs_f64());
    }
    println!(
        "decode ts   : scalar {:>7.1} Mpts/s | block(8-way) {:>7.1} Mpts/s  ({:.2}x)",
        N as f64 / scalar_ts / 1e6,
        N as f64 / block_ts / 1e6,
        scalar_ts / block_ts
    );

    // --- value column ---
    let mut scalar_val = f64::MAX;
    let mut facc = 0.0f64;
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let mut dec = ValDecoder::new(&val_col).unwrap().unwrap();
        let mut n = 0usize;
        // the value column's bitstream has zero padding in the last byte: the streaming decoder does not
        // know the point count (held by the caller, same as SegmentReader's DecodeIter) and reads exactly N.
        while n < N {
            facc += std::hint::black_box(dec.next_val().unwrap());
            n += 1;
        }
        std::hint::black_box(facc);
        assert_eq!(n, N);
        scalar_val = scalar_val.min(t.elapsed().as_secs_f64());
    }

    let mut out_val = vec![0.0f64; N].into_boxed_slice();
    let out_val: &mut [f64; N] = (&mut *out_val).try_into().unwrap();
    let mut block_val = f64::MAX;
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let n = decode_val_block(&val_col, out_val).unwrap();
        std::hint::black_box(out_val[0]);
        std::hint::black_box(out_val[N - 1]);
        assert_eq!(n, N);
        block_val = block_val.min(t.elapsed().as_secs_f64());
    }
    println!(
        "decode val  : scalar {:>7.1} Mpts/s | block(8-way) {:>7.1} Mpts/s  ({:.2}x)",
        N as f64 / scalar_val / 1e6,
        N as f64 / block_val / 1e6,
        scalar_val / block_val
    );
}
