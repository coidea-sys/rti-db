//! rti-db 评测：A 单点写延迟（3 个 SyncPolicy 变体）、B 批量写吞吐、C 扫描/聚合、D 存储/RSS。
//! 用法：rtidb [--smoke]

use bench_harness::*;
use rti_db::Db;
use rti_core::{Config, Sample, SyncPolicy};
use rti_query::Agg;
use std::time::Instant;

fn open(dir: &str, sync: SyncPolicy) -> Db {
    rm_rf(dir);
    let mut cfg = Config::default();
    cfg.data_dir = Some(std::path::PathBuf::from(dir));
    cfg.wal_sync = sync;
    Db::open(cfg).expect("Db::open")
}

fn put_retry(db: &Db, s: u32, ts: i64, v: f64) {
    loop {
        match db.put(s, Sample::new(ts, v)) {
            Ok(()) => return,
            Err(_) => std::thread::yield_now(), // SeriesFull 背压重试
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let (n_lat, n_tput, rounds) = if smoke { (10_000, 20_000, 1) } else { (200_000, 1_000_000, 3) };
    let n_scan = n_tput;

    let variants = [
        ("none", SyncPolicy::None),
        ("group1ms", SyncPolicy::Group { interval_us: 1_000 }),
        ("always", SyncPolicy::Always),
    ];

    for (name, policy) in variants {
        // ---- A: 单点写延迟 ----
        let mut round_lats = Vec::new();
        let mut round_tput = Vec::new();
        for r in 0..rounds {
            let dir = format!("/tmp/bench/rtidb-{name}-lat{r}");
            let db = open(&dir, policy);
            let warm = gen_interleaved(10_000 / SERIES as usize);
            for &(s, ts, v) in &warm {
                put_retry(&db, s, ts, v);
            }
            db.flush().unwrap();
            let data = gen_interleaved(n_lat / SERIES as usize);
            let mut lats = Vec::with_capacity(data.len());
            let t0 = Instant::now();
            for &(s, ts, v) in &data {
                let st = Instant::now();
                put_retry(&db, s, ts, v);
                lats.push(st.elapsed().as_nanos() as u64);
            }
            let wall = t0.elapsed().as_secs_f64();
            db.flush().unwrap();
            drop(db);
            if std::env::var_os("RTI_BENCH_DUMP_LATENCIES").is_some() {
                let p = format!("{RESULT_DIR}/latencies-rtidb-sync-{name}-r{r}.csv");
                let mut out = String::with_capacity(lats.len() * 8);
                for v in &lats {
                    out.push_str(&v.to_string());
                    out.push('\n');
                }
                std::fs::write(&p, out).unwrap();
                eprintln!("[dump] {} samples -> {}", lats.len(), p);
            }
            round_lats.push(hdr_stats(&lats));
            round_tput.push(data.len() as f64 / wall);
            rm_rf(&dir);
        }
        let mut res = BenchResult::new("rti-db", &format!("sync={name}"), n_lat as u64);
        res.latency_ns = Some(median_lat(&round_lats));
        res.throughput_ops = Some(median(round_tput));
        if name == "always" || name == "none" || name == "group1ms" {
            res.note("Db::put 为无锁 SPSC 入队（异步应用），WAL 组提交在后台 ingest 线程；延迟为入队延迟");
        }
        emit(&[res], "rtidb");

        // ---- B: 批量写吞吐（连续 put，flush 收尾）----
        // sync=always 为 fsync 受限（冒烟实测 ~760 pts/s），全量 1M 将超 20 分钟，
        // 按铁律 3 缩减为 10 万点并注明。
        let n_tput_v = if name == "always" && !smoke { 100_000 } else { n_tput };
        let dir = format!("/tmp/bench/rtidb-{name}-tput");
        let db = open(&dir, policy);
        let data = gen_interleaved(n_tput_v / SERIES as usize);
        let t0 = Instant::now();
        for &(s, ts, v) in &data {
            put_retry(&db, s, ts, v);
        }
        db.flush().unwrap();
        let wall = t0.elapsed().as_secs_f64();
        let tput = data.len() as f64 / wall;
        let mut res = BenchResult::new("rti-db", &format!("sync={name}"), n_tput_v as u64);
        res.throughput_ops = Some(tput);
        res.note("批量=连续单点 put（ingest 线程内部批量组提交），含最终 flush");
        if n_tput_v != n_tput { res.note(&format!("sync=always 为逐条 fsync 受限，本变体缩减为 {n_tput_v} 点（铁律3）")); }

        // ---- C: 扫描 + avg 聚合（同一实例数据）----
        db.seal().unwrap();
        let mut scan_tputs = Vec::new();
        let mut agg_tputs = Vec::new();
        for _ in 0..rounds {
            let t = Instant::now();
            let mut cnt = 0u64;
            for s in 0..SERIES {
                let n = db.scan(s, 0, i64::MAX, None, None).unwrap().count() as u64;
                cnt += n;
            }
            scan_tputs.push(cnt as f64 / t.elapsed().as_secs_f64());
            let t = Instant::now();
            let mut cnt = 0u64;
            for s in 0..SERIES {
                let it = db.scan(s, 0, i64::MAX, None, Some(Agg::Count)).unwrap();
                cnt += it.map(|x| x.value as u64).sum::<u64>();
            }
            // avg 聚合
            let ta = Instant::now();
            let mut npts = 0u64;
            for s in 0..SERIES {
                let it = db.scan(s, 0, i64::MAX, None, Some(Agg::Avg)).unwrap();
                for _ in it {}
                npts += 1;
            }
            let _ = npts;
            agg_tputs.push(cnt as f64 / ta.elapsed().as_secs_f64());
        }
        res.scan_pts_s = Some(median(scan_tputs));
        res.avg_agg_pts_s = Some(median(agg_tputs));

        // ---- D: 落盘字节 + RSS ----
        let disk = dir_bytes(&dir);
        res.disk_bytes = Some(disk);
        res.rss_bytes = self_rss();
        let raw = n_tput as u64 * 16;
        res.note(&format!("压缩比 vs 16B/点原始 = {:.2}x", raw as f64 / disk.max(1) as f64));
        drop(db);
        emit(&[res], "rtidb");
        rm_rf(&dir);
        eprintln!("[rtidb] variant {name} done");
    }
}
