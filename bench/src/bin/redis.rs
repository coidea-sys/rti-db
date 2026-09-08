//! Redis 7.2.5 评测：ZADD series:{id} ts "ts:value"。
//! 持久化变体：无AOF / appendonly everysec / appendonly always（CONFIG SET 动态切换 + CONFIG REWRITE）。
//! 服务由 run_all.sh 启动（端口 6390，dir /tmp/bench/redis-data）。
//! 用法：redis [--smoke] [--port N] [--only none|everysec|always]

use bench_harness::*;
use redis::Commands;
use std::time::Instant;

fn set_persist(con: &mut redis::Connection, variant: &str) {
    match variant {
        "none" => {
            redis::cmd("CONFIG").arg("SET").arg("appendonly").arg("no").query::<String>(con).unwrap();
        }
        v => {
            redis::cmd("CONFIG").arg("SET").arg("appendonly").arg("yes").query::<String>(con).unwrap();
            redis::cmd("CONFIG").arg("SET").arg("appendfsync").arg(v).query::<String>(con).unwrap();
        }
    }
    redis::cmd("CONFIG").arg("REWRITE").query::<String>(con).unwrap();
}

fn server_pid(con: &mut redis::Connection) -> Option<u32> {
    let info: String = redis::cmd("INFO").arg("server").query(con).ok()?;
    for line in info.lines() {
        if let Some(pid) = line.strip_prefix("process_id:") {
            return pid.trim().parse().ok();
        }
    }
    None
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let only = arg_flag(&args, "--only");
    let port = arg_flag(&args, "--port").unwrap_or_else(|| "6390".into());
    let (n_lat, n_tput, rounds) = if smoke { (10_000, 20_000, 1) } else { (200_000, 1_000_000, 3) };

    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    let mut con = client.get_connection().unwrap();

    for variant in ["none", "everysec", "always"] {
        if let Some(o) = &only {
            if o != variant {
                continue;
            }
        }
        set_persist(&mut con, variant);
        let vname = match variant {
            "none" => "no-aof".to_string(),
            v => format!("aof-{v}"),
        };

        // ---- A: 单点写延迟 ----
        let mut round_lats = Vec::new();
        let mut round_tput = Vec::new();
        for _r in 0..rounds {
            redis::cmd("FLUSHALL").query::<()>(con_ref(&mut con)).unwrap();
            let warm = gen_interleaved(10_000 / SERIES as usize);
            for &(s, ts, v) in &warm {
                redis::cmd("ZADD").arg(format!("series:{s}")).arg(ts).arg(format!("{ts}:{v}"))
                    .query::<()>(con_ref(&mut con)).unwrap();
            }
            let data = gen_interleaved(n_lat / SERIES as usize);
            let mut lats = Vec::with_capacity(data.len());
            let t0 = Instant::now();
            for &(s, ts, v) in &data {
                let member = format!("{ts}:{v}");
                let t = Instant::now();
                redis::cmd("ZADD").arg(format!("series:{s}")).arg(ts).arg(member)
                    .query::<()>(con_ref(&mut con)).unwrap();
                lats.push(t.elapsed().as_nanos() as u64);
            }
            let wall = t0.elapsed().as_secs_f64();
            round_lats.push(hdr_stats(&lats));
            round_tput.push(data.len() as f64 / wall);
        }
        let mut res = BenchResult::new("redis", &vname, n_lat as u64);
        res.latency_ns = Some(median_lat(&round_lats));
        res.throughput_ops = Some(median(round_tput));
        res.note("ZADD 单命令 RTT（同机 loopback）；score 为 f64，ts≈1.7e18 超出 2^53 精度，member 含完整 ts 故不丢点");
        emit(&[res], "redis");

        // ---- B: pipeline 批量写 ----
        redis::cmd("FLUSHALL").query::<()>(con_ref(&mut con)).unwrap();
        let data = gen_interleaved(n_tput / SERIES as usize);
        let t0 = Instant::now();
        for chunk in data.chunks(10_000) {
            let mut pipe = redis::pipe();
            for &(s, ts, v) in chunk {
                pipe.cmd("ZADD").arg(format!("series:{s}")).arg(ts).arg(format!("{ts}:{v}")).ignore();
            }
            pipe.query::<()>(con_ref(&mut con)).unwrap();
        }
        let wall = t0.elapsed().as_secs_f64();
        let mut res = BenchResult::new("redis", &vname, n_tput as u64);
        res.throughput_ops = Some(data.len() as f64 / wall);
        res.note("10k 点/批 pipeline（非事务）");

        // ---- C: ZRANGEBYSCORE 扫描 + avg（客户端聚合）----
        // 等 AOF 落盘稳定
        std::thread::sleep(std::time::Duration::from_millis(1200));
        let mut scan_tputs = Vec::new();
        let mut agg_tputs = Vec::new();
        for _ in 0..rounds {
            let t = Instant::now();
            let mut cnt = 0u64;
            for s in 0..SERIES {
                let rows: Vec<(String, f64)> = con
                    .zrangebyscore_withscores(format!("series:{s}"), "-inf", "+inf")
                    .unwrap();
                cnt += rows.len() as u64;
            }
            scan_tputs.push(cnt as f64 / t.elapsed().as_secs_f64());
            let t = Instant::now();
            let mut cnt = 0u64;
            let mut sum = 0.0f64;
            for s in 0..SERIES {
                let rows: Vec<String> = con
                    .zrangebyscore(format!("series:{s}"), "-inf", "+inf")
                    .unwrap();
                for m in &rows {
                    if let Some(vs) = m.split(':').nth(1) {
                        sum += vs.parse::<f64>().unwrap_or(0.0);
                    }
                }
                cnt += rows.len() as u64;
            }
            let _avg = sum / cnt.max(1) as f64;
            agg_tputs.push(cnt as f64 / t.elapsed().as_secs_f64());
        }
        res.scan_pts_s = Some(median(scan_tputs));
        res.avg_agg_pts_s = Some(median(agg_tputs));

        // ---- D ----
        std::thread::sleep(std::time::Duration::from_millis(1200)); // everysec 落盘窗口
        let disk = dir_bytes("/tmp/bench/redis-data");
        res.disk_bytes = Some(disk);
        let pid = server_pid(con_ref(&mut con));
        res.rss_bytes = pid.and_then(pid_rss);
        let raw = n_tput as u64 * 16;
        if disk == 0 {
            res.note("无持久化档位：落盘 0 字节（数据仅在内存）");
        } else {
            res.note(&format!("磁盘/原始16B = {:.2}x（AOF 为文本协议回放日志，预期膨胀）", disk as f64 / raw as f64));
        }
        if variant != "none" {
            res.note("AOF 触发的是全量文本重写前增量；含 RDB 若生成");
        }
        emit(&[res], "redis");
        redis::cmd("FLUSHALL").query::<()>(con_ref(&mut con)).unwrap();
        eprintln!("[redis] variant {vname} done");
    }
}

fn con_ref(c: &mut redis::Connection) -> &mut redis::Connection {
    c
}
