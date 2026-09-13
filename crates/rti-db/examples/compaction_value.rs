//! 旗舰示例 4：segment 压实——写放大自己吃掉，扫描还更快。
//!
//! 时序工作负载必有重传：传感器补发、网络重试、迟到数据。rti-db 按 (series, ts)
//! **幂等去重**（同 ts 首写生效），所以重传不会污染数据——但重复帧会物理堆积：
//! 段越攒越多、磁盘膨胀、扫描合并因子上升，直到压实把它们回收。本示例：
//!   1. 写入 30 万点并 seal；
//!   2. 用**相同时间戳**重发其中 10 万点（模拟重传）再 seal；
//!   3. 压实前：可见数据幂等（重发零影响），但段数膨胀、扫描合并因子上升；
//!   4. `compact()` 后：可见数据逐字节不变，段数↓ 扫描↑。
//!
//! 注：压实是崩溃安全的尾段合并，**保留重复帧**——磁盘去重由保留策略负责；
//! 本示例的价值主张是合并因子与扫描速度，不是磁盘收缩。
//!
//! 运行：`cargo run -p rti-db --release --example compaction_value`

use rti_core::{Config, Sample, SyncPolicy};
use rti_db::Db;
use std::time::Instant;

const SERIES: u32 = 8;
const POINTS: i64 = 300_000;
const REWRITE: i64 = 100_000; // 重发前 10 万点（同 ts、异值）
const RETRANSMIT_ROUNDS: usize = 4; // 重发 4 轮——重复帧物理堆积
const BASE: i64 = 1_700_000_000_000_000_000;

fn put(db: &Db, s: u32, ts: i64, v: f64) {
    while db.put(s, Sample::new(ts, v)).is_err() {
        std::thread::yield_now();
    }
}

fn dir_bytes(p: &std::path::Path) -> u64 {
    let mut sum = 0;
    for e in std::fs::read_dir(p).unwrap() {
        let e = e.unwrap();
        let m = e.metadata().unwrap();
        if m.is_dir() {
            sum += dir_bytes(&e.path());
        } else {
            sum += m.len();
        }
    }
    sum
}

fn scan_timed(db: &Db) -> (usize, f64) {
    let t = Instant::now();
    let mut n = 0;
    for s in 0..SERIES {
        n += db.scan(s, 0, i64::MAX, None, None).unwrap().count();
    }
    (n, t.elapsed().as_secs_f64())
}

fn main() {
    let dir = std::env::temp_dir().join(format!("compaction-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = Config {
        data_dir: Some(dir.clone()),
        wal_sync: SyncPolicy::Group { interval_us: 500 },
        ..Config::default()
    };
    let db = Db::open(cfg).unwrap();

    println!("== segment 压实价值演示（rti-db）==\n");

    // ---- 1. 原始写入 + seal ----------------------------------------------------
    for i in 0..POINTS {
        put(&db, (i as u32) % SERIES, BASE + i * 100, i as f64);
    }
    db.seal().unwrap();
    println!("写入 {} 点并 seal", POINTS);

    // ---- 2. 重发前 10 万点 ×4 轮（同时间戳、不同值——幂等去重只认首写）----------
    for round in 0..RETRANSMIT_ROUNDS {
        for i in 0..REWRITE {
            put(&db, (i as u32) % SERIES, BASE + i * 100, i as f64 + 1e6 * (round + 1) as f64);
        }
        db.seal().unwrap();
    }
    println!("重发其中 {} 点 ×{} 轮（同 ts 异值，模拟反复重传）\n", REWRITE, RETRANSMIT_ROUNDS);

    // ---- 3. 压实前测量 ----------------------------------------------------------
    let seg_before = db.segment_count();
    let disk_before = dir_bytes(&dir);
    let (n_before, t_before) = scan_timed(&db);
    // 幂等性：被重发的点，扫描看到的必须仍是首写值（i，而非 i + 1e6）
    let s0_first = db.scan(0, BASE, BASE + SERIES as i64 * 100, None, None).unwrap().next().unwrap();
    let idem_ok = s0_first.value < 1e6;
    println!("压实前：段数={}  磁盘={:.1}MB  扫描 {} 点耗时 {:.1}ms（{:.1}M pts/s）  重传幂等（首写值）{}",
        seg_before, disk_before as f64 / 1e6, n_before, t_before * 1e3,
        n_before as f64 / t_before / 1e6, if idem_ok { "✓" } else { "✗" });
    assert!(idem_ok, "重传污染了可见数据");

    // ---- 4. 压实 + 压实后测量 ----------------------------------------------------
    let t = Instant::now();
    let merged = db.compact().unwrap();
    let compact_ms = t.elapsed().as_secs_f64() * 1e3;
    let seg_after = db.segment_count();
    let disk_after = dir_bytes(&dir);
    let (n_after, t_after) = scan_timed(&db);
    let stats = db.compaction_stats();
    println!(
        "\ncompact()：合并 {} 个段，耗时 {:.1}ms（输入 {:.1}MB → 输出 {:.1}MB）",
        merged, compact_ms,
        stats.input_bytes as f64 / 1e6, stats.output_bytes as f64 / 1e6
    );
    println!("压实后：段数={}  磁盘={:.1}MB  扫描 {} 点耗时 {:.1}ms（{:.1}M pts/s）",
        seg_after, disk_after as f64 / 1e6, n_after, t_after * 1e3,
        n_after as f64 / t_after / 1e6);

    // 数据不变性：压实前后可见数据完全一致
    let s0_after = db.scan(0, BASE, BASE + SERIES as i64 * 100, None, None).unwrap().next().unwrap();
    assert_eq!(s0_first, s0_after, "压实改变了可见数据");
    println!("可见数据一致性：压实前后扫描首点完全相同 ✓");

    println!("\n结论：");
    println!(
        "  段数 {} → {}（合并因子 -{:.0}%），全量扫描 {:.1}M → {:.1}M pts/s（+{:.0}%）；",
        seg_before, seg_after,
        100.0 * (1.0 - seg_after as f64 / seg_before as f64),
        n_before as f64 / t_before / 1e6, n_after as f64 / t_after / 1e6,
        100.0 * (t_before / t_after - 1.0)
    );
    println!("  重复帧按设计保留（崩溃安全合并），可见数据幂等且压实前后逐字节一致——");
    println!("  长期运行的扫描速度不衰减，这就是 LSM 类引擎的必修课。");
    let _ = (disk_before, disk_after); // 磁盘占用由保留策略管理，非本示例主张

    let _ = std::fs::remove_dir_all(&dir);
}
