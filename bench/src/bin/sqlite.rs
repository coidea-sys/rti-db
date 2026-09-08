//! SQLite 评测：A 单点写（prepared INSERT 逐行 autocommit，synchronous ∈ OFF/NORMAL/FULL，WAL）、
//! B 事务批（10k/批）、C 扫描 + AVG、D 存储/RSS。
//! 用法：sqlite [--smoke] [--only off|normal|full]

use bench_harness::*;
use rusqlite::Connection;
use std::time::Instant;

fn open(path: &str, sync: &str) -> Connection {
    rm_rf(path);
    rm_rf(&format!("{path}-wal"));
    rm_rf(&format!("{path}-shm"));
    let c = Connection::open(path).unwrap();
    c.pragma_update(None, "journal_mode", "WAL").unwrap();
    c.pragma_update(None, "synchronous", sync).unwrap();
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS points(series INTEGER NOT NULL, ts INTEGER NOT NULL, value REAL NOT NULL);",
    )
    .unwrap();
    c
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let only = arg_flag(&args, "--only");
    let (n_lat, n_tput, rounds) = if smoke { (10_000, 20_000, 1) } else { (200_000, 1_000_000, 3) };

    for sync in ["OFF", "NORMAL", "FULL"] {
        if let Some(o) = &only {
            if !o.eq_ignore_ascii_case(sync) {
                continue;
            }
        }
        let vname = format!("synchronous={sync},WAL");
        // FULL 档为 fsync 受限（冒烟实测 ~820 ops/s），200k×3 轮约 12 分钟，
        // 按铁律 3 缩减为 2 万点/轮并注明。
        let n_lat_v = if sync == "FULL" && !smoke { 20_000 } else { n_lat };

        // ---- A: 单点写延迟 ----
        let mut round_lats = Vec::new();
        let mut round_tput = Vec::new();
        for r in 0..rounds {
            let path = format!("/tmp/bench/sqlite-{sync}-lat{r}.db");
            let c = open(&path, sync);
            let data = gen_interleaved(n_lat_v / SERIES as usize);
            {
                let mut st = c.prepare_cached("INSERT INTO points VALUES(?1,?2,?3)").unwrap();
                // 预热：复用同一 prepared statement，先写 1 万点丢弃（并入延迟统计外）
                let warm = gen_interleaved(10_000 / SERIES as usize);
                for &(s, ts, v) in &warm {
                    st.execute((s, ts, v)).unwrap();
                }
                let mut lats = Vec::with_capacity(data.len());
                let t0 = Instant::now();
                for &(s, ts, v) in &data {
                    let t = Instant::now();
                    st.execute((s, ts, v)).unwrap();
                    lats.push(t.elapsed().as_nanos() as u64);
                }
                let wall = t0.elapsed().as_secs_f64();
                round_lats.push(hdr_stats(&lats));
                round_tput.push(data.len() as f64 / wall);
            }
            drop(c);
            rm_rf(&path);
            rm_rf(&format!("{path}-wal"));
            rm_rf(&format!("{path}-shm"));
        }
        let mut res = BenchResult::new("sqlite", &vname, n_lat_v as u64);
        res.latency_ns = Some(median_lat(&round_lats));
        res.throughput_ops = Some(median(round_tput));
        res.note("prepared INSERT 逐行 autocommit（每行一个隐式事务）");
        if n_lat_v != n_lat { res.note(&format!("FULL 档 fsync 受限，延迟测量缩减为 {n_lat_v} 点/轮（铁律3）")); }
        emit(&[res], "sqlite");

        // ---- B: 批量写（10k 点/事务）----
        let path = format!("/tmp/bench/sqlite-{sync}-tput.db");
        let c = open(&path, sync);
        let data = gen_interleaved(n_tput / SERIES as usize);
        let t0 = Instant::now();
        for chunk in data.chunks(10_000) {
            let tx = c.unchecked_transaction().unwrap();
            {
                let mut st = tx.prepare_cached("INSERT INTO points VALUES(?1,?2,?3)").unwrap();
                for &(s, ts, v) in chunk {
                    st.execute((s, ts, v)).unwrap();
                }
            }
            tx.commit().unwrap();
        }
        let wall = t0.elapsed().as_secs_f64();
        let mut res = BenchResult::new("sqlite", &vname, n_tput as u64);
        res.throughput_ops = Some(data.len() as f64 / wall);
        res.note("10k 点/事务批量提交");

        // ---- C: 扫描 + AVG ----
        c.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();
        let mut scan_tputs = Vec::new();
        let mut agg_tputs = Vec::new();
        for _ in 0..rounds {
            let t = Instant::now();
            let mut cnt = 0u64;
            {
                let mut st = c.prepare_cached("SELECT ts, value FROM points WHERE series=?1 ORDER BY ts").unwrap();
                for s in 0..SERIES {
                    let mut rows = st.query([s]).unwrap();
                    while rows.next().unwrap().is_some() {
                        cnt += 1;
                    }
                }
            }
            scan_tputs.push(cnt as f64 / t.elapsed().as_secs_f64());
            let t = Instant::now();
            let mut total = 0u64;
            {
                let mut st = c.prepare_cached("SELECT AVG(value), COUNT(*) FROM points WHERE series=?1").unwrap();
                for s in 0..SERIES {
                    let (_a, n): (f64, u64) = st.query_row([s], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                    total += n;
                }
            }
            agg_tputs.push(total as f64 / t.elapsed().as_secs_f64());
        }
        res.scan_pts_s = Some(median(scan_tputs));
        res.avg_agg_pts_s = Some(median(agg_tputs));

        // ---- D ----
        c.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();
        let disk = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
            + std::fs::metadata(format!("{path}-wal")).map(|m| m.len()).unwrap_or(0);
        res.disk_bytes = Some(disk);
        res.rss_bytes = self_rss();
        let raw = n_tput as u64 * 16;
        res.note(&format!("压缩比 vs 16B/点原始 = {:.2}x（无索引，SQLite 行存含页头/rowid 开销）", raw as f64 / disk.max(1) as f64));
        drop(c);
        emit(&[res], "sqlite");
        rm_rf(&path);
        rm_rf(&format!("{path}-wal"));
        rm_rf(&format!("{path}-shm"));
        eprintln!("[sqlite] variant {sync} done");
    }
}
