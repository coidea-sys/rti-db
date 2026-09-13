//! 旗舰示例 2：飞行记录器——`kill -9` 也丢不了一条已确认的数据。
//!
//! 这是 rti-db 最硬核的契约：`put_durable()` 返回后，该点已扛住进程被杀。
//! 本示例**真的这么做**：
//!   1. 以子进程模式自我重启（`--child`），子进程逐点 `put_durable` 并向 stdout
//!      报告每个已确认序号；
//!   2. 父进程在确认数过半时**直接 SIGKILL 子进程**（不等它优雅退出）；
//!   3. 重新打开数据库：测量恢复耗时，并逐点校验——已确认的点**一个都不能少**；
//!   4. 对同一时间窗做两次"回放"，字节级摘要必须完全一致（确定性回放 = 审计基础）。
//!
//! 运行：`cargo run -p rti-db --release --example flight_recorder`

use rti_core::{Config, Sample, SyncPolicy};
use rti_db::Db;
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};

const BASE: i64 = 1_700_000_000_000_000_000;
const TOTAL: i64 = 20_000; // 子进程计划写入点数
const KILL_AFTER: i64 = 10_000; // 父进程在确认到第 N 点后 SIGKILL

fn config(dir: &std::path::Path) -> Config {
    Config {
        data_dir: Some(dir.to_path_buf()),
        wal_sync: SyncPolicy::Group { interval_us: 500 },
        ..Config::default()
    }
}

/// fnv-1a：对 (ts, value) 序列做字节级摘要
fn digest(it: impl Iterator<Item = Sample>) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for s in it {
        for b in s
            .ts
            .to_le_bytes()
            .into_iter()
            .chain(s.value.to_bits().to_le_bytes())
        {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}

fn child(dir: &std::path::Path) {
    let db = Db::open(config(dir)).unwrap();
    for i in 0..TOTAL {
        db.put_durable(1, Sample::new(BASE + i * 100, (i % 997) as f64 * 0.5), Duration::from_millis(50))
            .unwrap();
        if i % 100 == 0 {
            println!("ACK {i}"); // 父进程据此决定何时下杀手
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    }
    println!("DONE");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::env::temp_dir().join("flight-recorder-demo");

    if args.iter().any(|a| a == "--child") {
        child(&dir);
        return;
    }

    let _ = std::fs::remove_dir_all(&dir);
    println!("== 飞行记录器演示：SIGKILL 下的零丢失契约（rti-db）==\n");

    // ---- 1. 启动子进程写入，确认过半时 SIGKILL --------------------------------
    let exe = std::env::current_exe().unwrap();
    let mut proc = std::process::Command::new(exe)
        .arg("--child")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut acked = 0i64;
    {
        let stdout = proc.stdout.take().unwrap();
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some(n) = line.strip_prefix("ACK ") {
                acked = n.parse().unwrap();
                if acked >= KILL_AFTER {
                    proc.kill().unwrap(); // 真的 SIGKILL，不是优雅退出
                    break;
                }
            }
        }
    }
    let status = proc.wait().unwrap();
    // 子进程在打出 "ACK N" 之前已完成第 N 点的 put_durable；之后最多又确认了 99 点
    // （每 100 点报一次）。统计扫描结果后反推实际存活点数。
    println!(
        "子进程在确认第 {acked} 点后被 SIGKILL（退出状态：{status:?}）\n"
    );

    // ---- 2. 恢复 + 零丢失校验 -------------------------------------------------
    let t0 = Instant::now();
    let db = Db::open(config(&dir)).unwrap();
    let recovery = t0.elapsed();

    let survived: Vec<Sample> = db.scan(1, 0, i64::MAX, None, None).unwrap().collect();
    // 零丢失：存活的点必须是前缀 0..survived.len()，且值完全正确
    let mut ok = true;
    for (i, s) in survived.iter().enumerate() {
        let expect_ts = BASE + i as i64 * 100;
        let expect_v = (i as i64 % 997) as f64 * 0.5;
        if s.ts != expect_ts || s.value != expect_v {
            ok = false;
            println!("  ✗ 第 {i} 点损坏：ts={} value={}", s.ts, s.value);
            break;
        }
    }
    println!("崩溃恢复：{:.1} ms（WAL 检查点只重放当前 MemTable 尾巴）", recovery.as_secs_f64() * 1e3);
    println!(
        "零丢失校验：{} 个点全部在位且字节正确（最后确认 ACK {}，恢复出 {} 点）{}",
        survived.len(),
        acked,
        survived.len(),
        if ok { "✓" } else { "✗" }
    );
    assert!(ok, "durable 数据损坏——契约被破坏");
    assert!(
        survived.len() as i64 > acked,
        "已确认的点丢失——契约被破坏"
    );

    // ---- 3. 确定性回放：同一窗口两次扫描，摘要必须一致 -------------------------
    let d1 = digest(db.scan(1, 0, i64::MAX, None, None).unwrap());
    let d2 = digest(db.scan(1, 0, i64::MAX, None, None).unwrap());
    println!(
        "确定性回放：两次全窗口扫描摘要 {d1:#018x} == {d2:#018x} {}",
        if d1 == d2 { "✓" } else { "✗" }
    );
    assert_eq!(d1, d2, "回放不一致");

    println!("\n结论：");
    println!("  put_durable 契约经受了真实的 kill -9：已确认数据零丢失、逐字节正确；");
    println!("  恢复耗时毫秒级；任意决策窗口可逐字节重现——安全审计与事故复现的基础。");

    let _ = std::fs::remove_dir_all(&dir);
}
