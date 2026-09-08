//! E. 崩溃恢复：写 50 万点后 kill -9，测重启可用时间 + 已持久化点数（丢失率）。
//! 用法：
//!   recovery run [--smoke] [--n N]        —— 编排 rti-db / SQLite / Redis 三方
//!   recovery rtidb-ingest <dir> <n>       —— 子进程模式
//!   recovery rtidb-recover <dir>
//!   recovery sqlite-ingest <path> <n>
//!   recovery sqlite-recover <path>

use bench_harness::*;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Instant;

const REDIS_PORT: u16 = 6391;
const REDIS_DIR: &str = "/tmp/bench/redis-rec";
const REDIS_BIN: &str = "/tmp/redis-7.2.5/src/redis-server";

fn self_exe() -> String {
    std::env::current_exe().unwrap().to_string_lossy().into_owned()
}

// ---------- 子进程模式 ----------

fn rtidb_ingest(dir: &str, n: usize) {
    use rti_core::{Config, Sample, SyncPolicy};
    let mut cfg = Config::default();
    cfg.data_dir = Some(dir.into());
    cfg.wal_sync = SyncPolicy::Group { interval_us: 1_000 };
    let db = rti_db::Db::open(cfg).unwrap();
    let data = gen_interleaved(n / SERIES as usize);
    for (i, &(s, ts, v)) in data.iter().enumerate() {
        loop {
            match db.put(s, Sample::new(ts, v)) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
        if (i + 1) % 50_000 == 0 {
            println!("PROGRESS {}", i + 1);
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    }
    println!("DONE {n}");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}

fn rtidb_recover(dir: &str) {
    use rti_core::Config;
    use rti_query::Agg;
    let t0 = Instant::now();
    let mut cfg = Config::default();
    cfg.data_dir = Some(dir.into());
    cfg.wal_sync = rti_core::SyncPolicy::Group { interval_us: 1_000 };
    let db = rti_db::Db::open(cfg).unwrap();
    let open_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let t1 = Instant::now();
    let mut durable = 0u64;
    for s in 0..SERIES {
        let it = db.scan(s, 0, i64::MAX, None, Some(Agg::Count)).unwrap();
        durable += it.map(|x| x.value as u64).sum::<u64>();
    }
    let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let count_ms = t1.elapsed().as_secs_f64() * 1000.0;
    println!("RESULT {{\"open_ms\":{open_ms:.1},\"count_ms\":{count_ms:.1},\"total_ms\":{total_ms:.1},\"durable\":{durable}}}");
}

fn sqlite_ingest(path: &str, n: usize) {
    let c = rusqlite::Connection::open(path).unwrap();
    c.pragma_update(None, "journal_mode", "WAL").unwrap();
    c.pragma_update(None, "synchronous", "NORMAL").unwrap();
    c.execute_batch("CREATE TABLE IF NOT EXISTS points(series INTEGER, ts INTEGER, value REAL)").unwrap();
    let data = gen_interleaved(n / SERIES as usize);
    {
        let mut st = c.prepare_cached("INSERT INTO points VALUES(?1,?2,?3)").unwrap();
        for (i, &(s, ts, v)) in data.iter().enumerate() {
            st.execute((s, ts, v)).unwrap();
            if (i + 1) % 50_000 == 0 {
                println!("PROGRESS {}", i + 1);
                use std::io::Write;
                std::io::stdout().flush().unwrap();
            }
        }
    }
    println!("DONE {n}");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}

fn sqlite_recover(path: &str) {
    let t0 = Instant::now();
    let c = rusqlite::Connection::open(path).unwrap();
    let durable: u64 = c.query_row("SELECT COUNT(*) FROM points", [], |r| r.get(0)).unwrap();
    let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!("RESULT {{\"open_ms\":{total_ms:.1},\"count_ms\":0.0,\"total_ms\":{total_ms:.1},\"durable\":{durable}}}");
}

// ---------- 编排 ----------

fn spawn_ingest(args: &[&str]) -> (Child, BufReader<std::process::ChildStdout>) {
    let mut c = Command::new(self_exe())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let out = c.stdout.take().unwrap();
    (c, BufReader::new(out))
}

fn wait_done(r: &mut BufReader<std::process::ChildStdout>) {
    let mut line = String::new();
    loop {
        line.clear();
        if r.read_line(&mut line).unwrap() == 0 {
            panic!("ingest 子进程意外退出");
        }
        if line.starts_with("DONE") {
            return;
        }
    }
}

fn run_recover(args: &[&str]) -> serde_json::Value {
    let out = Command::new(self_exe()).args(args).output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().find(|l| l.starts_with("RESULT ")).expect(&format!("recover 无输出: {text}"));
    serde_json::from_str(line.trim_start_matches("RESULT ")).unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("rtidb-ingest") => rtidb_ingest(&args[2], args[3].parse().unwrap()),
        Some("rtidb-recover") => rtidb_recover(&args[2]),
        Some("sqlite-ingest") => sqlite_ingest(&args[2], args[3].parse().unwrap()),
        Some("sqlite-recover") => sqlite_recover(&args[2]),
        Some("run") => run(&args),
        _ => {
            eprintln!("usage: recovery run [--smoke] [--n N] | <subcmd>");
            std::process::exit(2);
        }
    }
}

fn run(args: &[String]) {
    let smoke = args.iter().any(|a| a == "--smoke");
    let n: usize = arg_flag(args, "--n")
        .map(|s| s.parse().unwrap())
        .unwrap_or(if smoke { 50_000 } else { 500_000 });
    let nstr = n.to_string();
    let mut results = Vec::new();

    // ---- rti-db ----
    let dir = "/tmp/bench/rtidb-rec";
    rm_rf(dir);
    let ns = nstr.as_str();
    let (mut child, mut reader) = spawn_ingest(&["rtidb-ingest", dir, ns]);
    wait_done(&mut reader);
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();
    let rec = run_recover(&["rtidb-recover", dir]);
    let durable = rec["durable"].as_u64().unwrap();
    let mut res = BenchResult::new("rti-db", "sync=group1ms", n as u64);
    res.recovery_ms = Some(rec["total_ms"].as_f64().unwrap());
    res.lost_points = Some(n as i64 - durable as i64);
    res.note(&format!(
        "DONE 后立即 SIGKILL；崩溃窗口=SPSC ring 中未出队的点（RING_CAP=65536）+ ingest 已出队未组提交的点；put 成功≠持久化（异步管线语义，如实记录）；open_ms={:.1} 含 WAL 重放，count_ms={:.1}",
        rec["open_ms"].as_f64().unwrap(),
        rec["count_ms"].as_f64().unwrap()
    ));
    results.push(res);
    rm_rf(dir);

    // ---- SQLite ----
    let path = "/tmp/bench/sqlite-rec.db";
    rm_rf(path);
    rm_rf(&format!("{path}-wal"));
    rm_rf(&format!("{path}-shm"));
    let (mut child, mut reader) = spawn_ingest(&["sqlite-ingest", path, ns]);
    wait_done(&mut reader);
    child.kill().unwrap();
    child.wait().unwrap();
    let rec = run_recover(&["sqlite-recover", path]);
    let durable = rec["durable"].as_u64().unwrap();
    let mut res = BenchResult::new("sqlite", "synchronous=NORMAL,WAL", n as u64);
    res.recovery_ms = Some(rec["total_ms"].as_f64().unwrap());
    res.lost_points = Some(n as i64 - durable as i64);
    res.note("DONE 后立即 SIGKILL；进程崩溃 OS 存活，WAL 页缓存不丢（synchronous=NORMAL 仅 checkpoint 时 fsync）");
    results.push(res);
    rm_rf(path);
    rm_rf(&format!("{path}-wal"));
    rm_rf(&format!("{path}-shm"));

    // ---- Redis（appendonly everysec）----
    rm_rf(REDIS_DIR);
    std::fs::create_dir_all(REDIS_DIR).unwrap();
    let mut server = Command::new(REDIS_BIN)
        .args(["--port", &REDIS_PORT.to_string(), "--dir", REDIS_DIR,
               "--appendonly", "yes", "--appendfsync", "everysec",
               "--save", "", "--daemonize", "no"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // 等待可连接
    let client = redis::Client::open(format!("redis://127.0.0.1:{REDIS_PORT}/")).unwrap();
    let mut con = loop {
        match client.get_connection() {
            Ok(c) => break c,
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    };
    let data = gen_interleaved(n / SERIES as usize);
    for chunk in data.chunks(10_000) {
        let mut pipe = redis::pipe();
        for &(s, ts, v) in chunk {
            pipe.cmd("ZADD").arg(format!("series:{s}")).arg(ts).arg(format!("{ts}:{v}")).ignore();
        }
        pipe.query::<()>(&mut con).unwrap();
    }
    // 全部 50 万已收到服务端应答 → 立即 SIGKILL
    server.kill().unwrap();
    server.wait().unwrap();
    drop(con);
    // 重启并计时到 PING 可用
    let t0 = Instant::now();
    let mut server2 = Command::new(REDIS_BIN)
        .args(["--port", &REDIS_PORT.to_string(), "--dir", REDIS_DIR,
               "--appendonly", "yes", "--appendfsync", "everysec", "--save", ""])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut con2 = loop {
        match client.get_connection() {
            Ok(mut c) => {
                if redis::cmd("PING").query::<String>(&mut c).is_ok() {
                    break c;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    };
    let up_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let t1 = Instant::now();
    let mut durable = 0u64;
    for s in 0..SERIES {
        durable += redis::cmd("ZCARD").arg(format!("series:{s}")).query::<u64>(&mut con2).unwrap();
    }
    let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let count_ms = t1.elapsed().as_secs_f64() * 1000.0;
    let mut res = BenchResult::new("redis", "aof-everysec", n as u64);
    res.recovery_ms = Some(total_ms);
    res.lost_points = Some(n as i64 - durable as i64);
    res.note(&format!("全部应答后 SIGKILL 服务端（everysec 崩溃窗口≈1s）；ping_up_ms={up_ms:.1} 含 AOF 重放，zcard_ms={count_ms:.1}"));
    results.push(res);
    server2.kill().unwrap();
    server2.wait().unwrap();
    rm_rf(REDIS_DIR);

    emit(&results, "recovery");
    eprintln!("[recovery] done");
}
