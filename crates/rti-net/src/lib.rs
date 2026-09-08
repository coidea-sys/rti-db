//! rti-net：TCP 行协议 server。
//!
//! 协议（每行一条命令，UTF-8 文本）：
//!
//! ```text
//! put  <series> <ts> <value>          → "OK" | "ERR <msg>"
//! scan <series> <t0> <t1> [agg]       → 若干行 "<ts> <value>"，以 "END" 结束
//! ```
//!
//! 其中 `agg ∈ {min,max,sum,avg,count}`。server 每连接一个线程，
//! `Db` 本身 `Send + Sync`，可无锁共享。

#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use rti_core::{Error, Result, Sample, SeriesId, Timestamp};
use rti_db::Db;
use rti_query::{Agg, Pred};

/// 解析后的命令。
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Command {
    /// 写入采样点。
    Put {
        /// 序列 id。
        series: SeriesId,
        /// 采样点。
        sample: Sample,
    },
    /// 区间扫描（可选聚合）。
    Scan {
        /// 序列 id。
        series: SeriesId,
        /// 起始时间（闭区间）。
        t0: Timestamp,
        /// 结束时间（闭区间）。
        t1: Timestamp,
        /// 可选聚合。
        agg: Option<Agg>,
    },
}

/// 解析一行命令；空行返回 `Ok(None)`。
pub fn parse_command(line: &str) -> Result<Option<Command>> {
    let toks: Vec<&str> = line.split_whitespace().collect();
    match toks.as_slice() {
        [] => Ok(None),
        ["put", s, ts, v] => {
            let series = s.parse().map_err(|_| Error::Protocol(format!("bad series: {s}")))?;
            let ts = ts.parse().map_err(|_| Error::Protocol(format!("bad ts: {ts}")))?;
            let value: f64 = v.parse().map_err(|_| Error::Protocol(format!("bad value: {v}")))?;
            Ok(Some(Command::Put { series, sample: Sample { ts, value } }))
        }
        ["scan", s, t0, t1] => {
            let (series, t0, t1) = parse_scan_args(s, t0, t1)?;
            Ok(Some(Command::Scan { series, t0, t1, agg: None }))
        }
        ["scan", s, t0, t1, agg] => {
            let (series, t0, t1) = parse_scan_args(s, t0, t1)?;
            Ok(Some(Command::Scan { series, t0, t1, agg: Some(parse_agg(agg)?) }))
        }
        [cmd, ..] => Err(Error::Protocol(format!("unknown command: {cmd}"))),
    }
}

fn parse_scan_args(s: &str, t0: &str, t1: &str) -> Result<(SeriesId, Timestamp, Timestamp)> {
    let series = s.parse().map_err(|_| Error::Protocol(format!("bad series: {s}")))?;
    let t0 = t0.parse().map_err(|_| Error::Protocol(format!("bad t0: {t0}")))?;
    let t1 = t1.parse().map_err(|_| Error::Protocol(format!("bad t1: {t1}")))?;
    Ok((series, t0, t1))
}

fn parse_agg(a: &str) -> Result<Agg> {
    match a {
        "min" => Ok(Agg::Min),
        "max" => Ok(Agg::Max),
        "sum" => Ok(Agg::Sum),
        "avg" => Ok(Agg::Avg),
        "count" => Ok(Agg::Count),
        _ => Err(Error::Protocol(format!("unknown agg: {a}"))),
    }
}

/// 执行一条命令并渲染响应文本（以 `\n` 结尾）。
pub fn handle(db: &Db, cmd: Command) -> String {
    match cmd {
        Command::Put { series, sample } => match db.put(series, sample) {
            Ok(()) => "OK\n".to_string(),
            Err(e) => format!("ERR {e}\n"),
        },
        Command::Scan { series, t0, t1, agg } => {
            match db.scan(series, t0, t1, None::<Pred>, agg) {
                Ok(it) => {
                    let mut out = String::new();
                    for s in it {
                        out.push_str(&format!("{} {}\n", s.ts, s.value));
                    }
                    out.push_str("END\n");
                    out
                }
                Err(e) => format!("ERR {e}\n"),
            }
        }
    }
}

/// 处理一行输入（解析 + 执行）；协议错误返回 `ERR ...` 文本。
pub fn handle_line(db: &Db, line: &str) -> String {
    match parse_command(line) {
        Ok(Some(cmd)) => handle(db, cmd),
        Ok(None) => String::new(),
        Err(e) => format!("ERR {e}\n"),
    }
}

/// 启动 TCP server（阻塞当前线程，每连接一线程）。
pub fn serve(addr: &str, db: Arc<Db>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    let _ = handle_conn(s, db);
                });
            }
            Err(e) => eprintln!("rti-net accept error: {e}"),
        }
    }
    Ok(())
}

/// 单连接循环：逐行读取命令，写回响应。
pub fn handle_conn(stream: TcpStream, db: Arc<Db>) -> std::io::Result<()> {
    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let line = line?;
        let resp = handle_line(&db, &line);
        if !resp.is_empty() {
            writer.write_all(resp.as_bytes())?;
            writer.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rti_core::{Config, SyncPolicy};

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-net-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn test_db(tag: &str) -> (Arc<Db>, std::path::PathBuf) {
        let d = tmpdir(tag);
        let db = Db::open(Config {
            data_dir: Some(d.clone()),
            memtable_max: 1 << 12,
            wal_sync: SyncPolicy::None,
            pool_bytes: 1 << 16,
            ..Config::default()
        })
        .unwrap();
        (Arc::new(db), d)
    }

    #[test]
    fn parse_put_and_scan() {
        let cmd = parse_command("put 3 1700000000 21.5").unwrap().unwrap();
        assert_eq!(cmd, Command::Put { series: 3, sample: Sample::new(1_700_000_000, 21.5) });

        let cmd = parse_command("scan 3 0 100").unwrap().unwrap();
        assert_eq!(cmd, Command::Scan { series: 3, t0: 0, t1: 100, agg: None });

        let cmd = parse_command("scan 3 0 100 avg").unwrap().unwrap();
        assert_eq!(cmd, Command::Scan { series: 3, t0: 0, t1: 100, agg: Some(Agg::Avg) });

        assert!(parse_command("").unwrap().is_none());
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(matches!(parse_command("put x 1 2"), Err(Error::Protocol(_))));
        assert!(matches!(parse_command("scan 1 0"), Err(Error::Protocol(_))));
        assert!(matches!(parse_command("scan 1 0 9 bogus"), Err(Error::Protocol(_))));
        assert!(matches!(parse_command("delete 1"), Err(Error::Protocol(_))));
    }

    #[test]
    fn handle_line_roundtrip() {
        let (db, d) = test_db("handle");
        assert_eq!(handle_line(&db, "put 1 10 1.5"), "OK\n");
        assert_eq!(handle_line(&db, "put 1 20 2.5"), "OK\n");
        db.flush().unwrap();
        let resp = handle_line(&db, "scan 1 0 100");
        assert_eq!(resp, "10 1.5\n20 2.5\nEND\n");
        let agg = handle_line(&db, "scan 1 0 100 sum");
        assert_eq!(agg, "0 4\nEND\n");
        assert!(handle_line(&db, "bogus").starts_with("ERR"));
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn tcp_loopback_end_to_end() {
        let (db, d) = test_db("tcp");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                handle_conn(stream, db).unwrap();
            })
        };
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(b"put 9 100 3.25\nput 9 200 4.25\n").unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "OK\n");
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "OK\n");

        client.write_all(b"scan 9 0 1000 max\n").unwrap();
        let mut resp = String::new();
        reader.read_line(&mut resp).unwrap();
        assert_eq!(resp, "0 4.25\n");
        resp.clear();
        reader.read_line(&mut resp).unwrap();
        assert_eq!(resp, "END\n");

        // 注意：必须先 drop reader（持有同一 socket 的克隆 fd），
        // 否则服务端永远读不到 EOF，join 会挂起。
        drop(reader);
        drop(client);
        srv.join().unwrap();
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }}
