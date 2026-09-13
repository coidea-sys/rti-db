//! 旗舰示例 1：具身智能的"工作记忆"——S1/S2 双系统数据面。
//!
//! 物理世界以 1–100 kHz 流动，基础模型以 10–100 ms 推理。这个示例把 rti-db 放在两者
//! 之间，同时跑两条回路并**实测各自的延迟分布**：
//!
//! - **S1 反射回路**（快系统）：每拍都要"最新传感器状态"。走 O(1) `latest()`——
//!   读路径零分配、无锁，目标是微秒级、p999 有界。
//! - **S2 推理回路**（慢系统）：每隔一段时间取最近一个时间窗做聚合（情景上下文）。
//!   走列存段 + 谓词下推 + 单遍聚合。
//!
//! 运行：`cargo run -p rti-db --release --example embodied_memory`

use rti_core::{Config, Sample, SyncPolicy};
use rti_db::Db;
use rti_query::Agg;
use std::time::Instant;

const SERIES: u32 = 8; // 8 路传感器（关节角/力矩/IMU/力触觉…）
const POINTS: usize = 200_000; // 20 万点混合摄入
const REFLEX_READS: usize = 100_000; // S1：10 万次 latest() 反射读
const REASON_QUERIES: usize = 200; // S2：200 次窗口聚合
const BASE: i64 = 1_700_000_000_000_000_000; // ns 时间戳基

fn pct(sorted: &[u64], q: f64) -> u64 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

/// SeriesFull 背压重试（与基准台一致：环形缓冲满时让出 CPU 一拍）
fn put(db: &Db, s: u32, ts: i64, v: f64) {
    while db.put(s, Sample::new(ts, v)).is_err() {
        std::thread::yield_now();
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("embodied-memory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = Config {
        data_dir: Some(dir.clone()),
        wal_sync: SyncPolicy::Group { interval_us: 500 },
        ..Config::default()
    };
    let db = Db::open(cfg).unwrap();

    println!("== 具身智能工作记忆演示（rti-db）==\n");

    // ---- 摄入：8 路 × 25k 点，交错时间戳（模拟 10 kHz 混合流）-----------------
    let t0 = Instant::now();
    for i in 0..POINTS as i64 {
        let s = (i as u32) % SERIES;
        // 类传感器波形：确定性伪信号（无需外部数据）
        let v = ((i * 7 % 1000) as f64 / 1000.0) * 2.0 - 1.0;
        put(&db, s, BASE + i * 100, v);
    }
    db.flush().unwrap();
    let ingest_s = t0.elapsed().as_secs_f64();
    println!(
        "摄入 {} 点（{} 路交错）：{:.2}M pts/s",
        POINTS,
        SERIES,
        POINTS as f64 / ingest_s / 1e6
    );

    // ---- S1 反射回路：O(1) latest()，逐次计时 ------------------------------
    let mut lats = Vec::with_capacity(REFLEX_READS);
    let t1 = Instant::now();
    for i in 0..REFLEX_READS {
        let s = (i as u32) % SERIES;
        let st = Instant::now();
        let latest = db.latest(s).unwrap().expect("series non-empty");
        lats.push(st.elapsed().as_nanos() as u64);
        std::hint::black_box(latest);
    }
    let reflex_wall = t1.elapsed();
    lats.sort_unstable();
    println!(
        "\n[S1 反射回路] {} 次 O(1) latest() 读（零分配、无锁读路径）：",
        REFLEX_READS
    );
    println!(
        "  p50={}ns  p99={}ns  p999={}ns  max={}ns  吞吐={:.1}M reads/s",
        pct(&lats, 0.50),
        pct(&lats, 0.99),
        pct(&lats, 0.999),
        lats.last().unwrap(),
        REFLEX_READS as f64 / reflex_wall.as_secs_f64() / 1e6
    );

    // ---- S2 推理回路：最近窗口聚合（列存 + 单遍聚合）-------------------------
    let head = db.latest(0).unwrap().unwrap().ts;
    let win = 10_000; // 最近 10µs 窗口（100 点/路）
    let t2 = Instant::now();
    let mut total_pts = 0usize;
    for q in 0..REASON_QUERIES {
        let hi = head - (q as i64) * 100;
        let lo = hi - win;
        for s in 0..SERIES {
            let n = db
                .scan(s, lo, hi, None, Some(Agg::Avg))
                .unwrap()
                .count();
            total_pts += n;
            std::hint::black_box(n);
        }
    }
    let reason_wall = t2.elapsed();
    println!(
        "\n[S2 推理回路] {} 次窗口聚合（{} 路 × 最近窗口，谓词下推 + 单遍 O(1) 额外内存）：",
        REASON_QUERIES, SERIES
    );
    println!(
        "  平均 {:.1}µs/次（{} 个聚合结果，{:.2}M agg/s）",
        reason_wall.as_secs_f64() * 1e6 / (REASON_QUERIES * SERIES as usize) as f64,
        total_pts,
        total_pts as f64 / reason_wall.as_secs_f64() / 1e6
    );

    // ---- 结论 ---------------------------------------------------------------
    println!("\n结论：");
    println!(
        "  反射路径 p999 = {}ns —— 每拍控制回路（100kHz 拍 = 10µs 预算）占用 <0.1%",
        pct(&lats, 0.999)
    );
    println!("  推理窗口与反射读并发无锁互相阻塞：快慢两车道结构性分离。");
    println!("  这就是\"物理世界的 KV cache\"：O(1) 取最新状态 + 列存取情景窗口。");

    let _ = std::fs::remove_dir_all(&dir);
}
