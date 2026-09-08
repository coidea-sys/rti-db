//! WAL backend comparison benchmark (v0.5 Stream B): `uring-pipeline vs std write`.
//!
//! Compares throughput and p99 latency of three backends (real numbers; io_uring is
//! available on the sandbox's 6.6 kernel — when unavailable, a limitation note is printed and only std runs):
//!
//! - `std`            : StdWalWriter (v0.1 semantics, BufWriter + sync_data)
//! - `uring-sync`     : IoUringWalWriter (v0.2, blocks after SQE submission until completion)
//! - `uring-pipeline` : IoUringPipelinedWalWriter (v0.5, multiple batches in flight,
//!   CQE reap loop, sync waits only for its own group, depth 64)
//!
//! Scenarios:
//! - throughput   : write N records with SyncPolicy::None + one final sync (exercises the submit path)
//! - group-commit : sync_now every 64 records (measures p99 and throughput at durability boundaries)
//!
//! Run: `cargo run --release -p rti-wal --features io-uring --example wal_bench`

use std::time::Instant;

use rti_core::SyncPolicy;
use rti_wal::{
    IoUringPipelinedWalWriter, IoUringWalWriter, Record, StdWalWriter, Wal, WalWriter,
};

const N: u32 = 500_000;
const GROUP: u32 = 64;
const ROUNDS: usize = 3;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rti-wal-bench-{}-{}", tag, std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn rec(i: u32) -> Record {
    Record::new(i % 64, i as i64 * 1_000, 20.0 + (i as f64 * 0.001).sin())
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    sorted[(p * (sorted.len() - 1) as f64) as usize]
}

/// Scenario A: batched-submission throughput (one durability point at the end). Returns ops/s.
fn bench_throughput(w: &mut dyn WalWriter) -> f64 {
    let t = Instant::now();
    for i in 0..N {
        w.append(&rec(i)).unwrap();
    }
    w.sync_now().unwrap();
    let wall = t.elapsed();
    N as f64 / wall.as_secs_f64()
}

/// Scenario B: group commit (one durability boundary per GROUP records). Returns (ops/s, p99 group latency ns).
fn bench_group_commit(w: &mut dyn WalWriter) -> (f64, u64) {
    let mut lat = Vec::with_capacity((N / GROUP) as usize);
    let t = Instant::now();
    let mut i = 0u32;
    while i < N {
        for j in i..(i + GROUP).min(N) {
            w.append(&rec(j)).unwrap();
        }
        let s = Instant::now();
        w.sync_now().unwrap();
        lat.push(s.elapsed().as_nanos() as u64);
        i += GROUP;
    }
    let wall = t.elapsed();
    lat.sort_unstable();
    (N as f64 / wall.as_secs_f64(), pct(&lat, 0.99))
}

/// Validate the on-disk record count and spot-check contents, guarding against 'fast but wrong'.
fn verify(path: &std::path::Path) {
    let recs: Vec<Record> = Wal::recover(path).unwrap().collect();
    assert_eq!(recs.len(), N as usize, "on-disk record count mismatch");
    assert_eq!(recs[0], rec(0));
    assert_eq!(recs[N as usize - 1], rec(N - 1));
}

fn main() {
    println!("== wal bench: uring-pipeline vs std write (N={N}, group={GROUP}, best of {ROUNDS}) ==");

    // backend availability probe: when io_uring is blocked by kernel policy, note the limitation and run only std.
    let probe_dir = tmpdir("probe");
    let uring_ok = IoUringPipelinedWalWriter::open(
        probe_dir.join("probe.log"),
        SyncPolicy::None,
        rti_wal_uring::PipelineConfig::default(),
    )
    .is_ok();
    std::fs::remove_dir_all(&probe_dir).ok();
    if !uring_ok {
        println!("NOTE: io_uring unavailable in this environment; only std numbers are real.");
    }

    let backends: Vec<&str> = if uring_ok {
        vec!["std", "uring-sync", "uring-pipeline"]
    } else {
        vec!["std"]
    };

    println!(
        "{:<15} {:>16} {:>16} {:>14}",
        "backend", "thrput ops/s", "group ops/s", "group p99 (us)"
    );
    for name in backends {
        let mut thr = 0.0f64;
        let mut gops = 0.0f64;
        let mut gp99 = u64::MAX;
        for _ in 0..ROUNDS {
            let d = tmpdir(name);
            let p = d.join("wal.log");
            // scenario A
            {
                let mut w = open_backend(name, &p, SyncPolicy::None);
                thr = thr.max(bench_throughput(&mut *w));
            }
            verify(&p);
            std::fs::remove_file(&p).unwrap();
            // scenario B
            {
                let mut w = open_backend(name, &p, SyncPolicy::None);
                let (o, p99) = bench_group_commit(&mut *w);
                gops = gops.max(o);
                gp99 = gp99.min(p99);
            }
            verify(&p);
            std::fs::remove_dir_all(&d).ok();
        }
        println!("{name:<15} {thr:>16.0} {gops:>16.0} {:>14.1}", gp99 as f64 / 1e3);
    }
    println!("frames: 28 B/record, {} MiB total", N as usize * 28 / (1 << 20));
}

/// Open a backend by name; SyncPolicy::None + explicit sync_now (both scenarios control their own pacing).
fn open_backend(name: &str, p: &std::path::Path, sync: SyncPolicy) -> Box<dyn WalWriter> {
    match name {
        "std" => Box::new(StdWalWriter::open(p, sync).unwrap()),
        "uring-sync" => Box::new(IoUringWalWriter::open(p, sync).unwrap()),
        "uring-pipeline" => Box::new(
            IoUringPipelinedWalWriter::open(
                p,
                sync,
                rti_wal_uring::PipelineConfig {
                    max_in_flight: 64,
                    backpressure: rti_wal_uring::BackpressurePolicy::Block,
                    ..Default::default()
                },
            )
            .unwrap(),
        ),
        _ => unreachable!(),
    }
}
