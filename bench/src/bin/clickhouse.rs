//! ClickHouse 24.8 评测：HTTP JSONEachRow。
//! 服务由 run_all.sh 启动（clickhouse server -- --path=/tmp/ch-data，HTTP 8124）。
//! A：单行 INSERT 仅 1 万点采样（预期很差——这正是要记录的发现）。
//! 用法：clickhouse [--smoke] [--port N]

use bench_harness::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn http_post(port: u16, query: &str, body: &[u8]) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    stream.set_read_timeout(Some(Duration::from_secs(300))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(300))).ok();
    let path = format!("/?wait_end_of_query=1&query={}", urlencode(query));
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).map_err(|e| e.to_string())?;
    let status = String::from_utf8_lossy(&resp[..resp.len().min(64)]).lines().next().unwrap_or("").to_string();
    if !status.contains(" 200") {
        let text = String::from_utf8_lossy(&resp);
        return Err(format!("HTTP status [{status}] body: {}", &text[..text.len().min(500)]));
    }
    // 分离头部与原始 body
    let sep = b"\r\n\r\n";
    let hpos = resp.windows(4).position(|w| w == sep).ok_or("no header end")?;
    let head = String::from_utf8_lossy(&resp[..hpos]).to_lowercase();
    let raw = &resp[hpos + 4..];
    let body: Vec<u8> = if head.contains("transfer-encoding: chunked") {
        // chunked 解码
        let mut out = Vec::new();
        let mut pos = 0usize;
        loop {
            let eol = raw[pos..].windows(2).position(|w| w == b"\r\n").ok_or("bad chunk size")? + pos;
            let szline = String::from_utf8_lossy(&raw[pos..eol]);
            let sz = usize::from_str_radix(szline.trim().split(';').next().unwrap_or("0"), 16).map_err(|e| e.to_string())?;
            pos = eol + 2;
            if sz == 0 {
                break;
            }
            out.extend_from_slice(&raw[pos..pos + sz]);
            pos += sz + 2; // 数据 + CRLF
        }
        out
    } else {
        raw.to_vec()
    };
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let port: u16 = arg_flag(&args, "--port").unwrap_or_else(|| "8124".into()).parse().unwrap();
    let (n_single, n_tput, rounds) = if smoke { (1_000, 20_000, 1) } else { (10_000, 1_000_000, 3) };

    http_post(port, "CREATE TABLE IF NOT EXISTS bench (series UInt32, ts Int64, value Float64) ENGINE = MergeTree ORDER BY (series, ts)", b"").unwrap();
    http_post(port, "TRUNCATE TABLE bench", b"").unwrap();

    // ---- A: 单行 INSERT 延迟（1 万点采样，1 轮）----
    let data = gen_interleaved(n_single / SERIES as usize);
    let mut lats = Vec::with_capacity(data.len());
    let t0 = Instant::now();
    let mut fails = 0u64;
    for &(s, ts, v) in &data {
        let row = format!("{{\"series\":{s},\"ts\":{ts},\"value\":{v}}}\n");
        let t = Instant::now();
        if http_post(port, "INSERT INTO bench FORMAT JSONEachRow", row.as_bytes()).is_err() {
            fails += 1;
        }
        lats.push(t.elapsed().as_nanos() as u64);
    }
    let wall = t0.elapsed().as_secs_f64();
    let mut res = BenchResult::new("clickhouse", "default", n_single as u64);
    res.latency_ns = Some(hdr_stats(&lats));
    res.throughput_ops = Some(data.len() as f64 / wall);
    res.note("HTTP JSONEachRow 单行 INSERT，每请求新建 TCP 连接；仅 1 万点采样（SPEC A）");
    if fails > 0 {
        res.note(&format!("失败 {fails} 次"));
    }
    emit(&[res], "clickhouse");
    http_post(port, "TRUNCATE TABLE bench", b"").unwrap();

    // ---- B: 批量 INSERT（10k 行/请求）----
    let data = gen_interleaved(n_tput / SERIES as usize);
    let t0 = Instant::now();
    let mut body = String::with_capacity(10_000 * 40);
    for chunk in data.chunks(10_000) {
        body.clear();
        for &(s, ts, v) in chunk {
            use std::fmt::Write as _;
            let _ = write!(body, "{{\"series\":{s},\"ts\":{ts},\"value\":{v}}}\n");
        }
        http_post(port, "INSERT INTO bench FORMAT JSONEachRow", body.as_bytes()).unwrap();
    }
    let wall = t0.elapsed().as_secs_f64();
    let mut res = BenchResult::new("clickhouse", "default", n_tput as u64);
    res.throughput_ops = Some(data.len() as f64 / wall);
    res.note("10k 行/请求 JSONEachRow 批量 INSERT");

    // ---- C: 扫描 + avg（服务端聚合）----
    http_post(port, "OPTIMIZE TABLE bench FINAL", b"").ok();
    let mut scan_tputs = Vec::new();
    let mut agg_tputs = Vec::new();
    for _ in 0..rounds {
        let t = Instant::now();
        let mut cnt = 0u64;
        for s in 0..SERIES {
            let r = http_post(port, &format!("SELECT ts, value FROM bench WHERE series = {s}"), b"").unwrap();
            cnt += r.lines().filter(|l| !l.trim().is_empty()).count() as u64;
        }
        scan_tputs.push(cnt as f64 / t.elapsed().as_secs_f64());
        let t = Instant::now();
        let mut total = 0u64;
        for s in 0..SERIES {
            match http_post(port, &format!("SELECT avg(value), count() FROM bench WHERE series = {s}"), b"") {
                Ok(r) => {
                    let n: u64 = r.split('\t').nth(1).and_then(|x| x.trim().parse().ok()).unwrap_or_else(|| { eprintln!("[ch][dbg] avg parse fail body={r:?}"); 0 });
                    total += n;
                }
                Err(e) => eprintln!("[ch][dbg] avg query err: {e}"),
            }
        }
        agg_tputs.push(total as f64 / t.elapsed().as_secs_f64());
    }
    res.scan_pts_s = Some(median(scan_tputs));
    res.avg_agg_pts_s = Some(median(agg_tputs));

    // ---- D ----
    let disk: u64 = match http_post(port, "SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'bench' AND active", b"") {
        Ok(r) => r.trim().parse().unwrap_or_else(|_| { eprintln!("[ch][dbg] disk parse fail body={r:?}"); dir_bytes("/tmp/ch-data/store") }),
        Err(e) => { eprintln!("[ch][dbg] disk query err: {e}"); dir_bytes("/tmp/ch-data/store") }
    };
    res.disk_bytes = Some(disk);
    // clickhouse 服务进程 RSS（取主进程：进程树中最大的 clickhouse）
    let rss = std::process::Command::new("sh")
        .arg("-c")
        .arg("pgrep -f 'clickhouse' | while read p; do awk '/VmRSS/{print $2, FILENAME}' /proc/$p/status 2>/dev/null; done | sort -n | tail -1 | awk '{print $1}'")
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok())
        .map(|kb| kb * 1024);
    res.rss_bytes = rss;
    let raw = n_tput as u64 * 16;
    res.note(&format!("压缩比 vs 16B/点原始 = {:.2}x（system.parts bytes_on_disk，MergeTree 列存 + LZ4）", raw as f64 / disk.max(1) as f64));
    emit(&[res], "clickhouse");
    eprintln!("[clickhouse] done");
}
