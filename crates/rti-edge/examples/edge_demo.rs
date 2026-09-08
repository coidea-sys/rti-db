//! RTI-Edge 一体化演示：RTI-L3 三层各用一档预设——
//! 安全岛（Deterministic + UDP 镜像）、认知层（3 节点内存 Raft 副本）、
//! 规划层（冷分层归档 + 透明读回），最后打印三份 health。
//!
//! 运行：`cargo run -p rti-edge --example edge_demo`

use std::net::UdpSocket;
use std::path::PathBuf;

use rti_core::{Mirror, Sample};
use rti_edge::{ColdTierConfig, EdgeConfig, EdgeHealth, EdgeNode};

fn demo_dir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("rti-edge-demo-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn print_health(layer: &str, h: &EdgeHealth) {
    println!("  [{layer}] profile={:?} mirror={:?} raft_role={:?} segments={} alloc_ok={}",
        h.profile, h.mirror_stats, h.raft_role, h.segment_count, h.alloc_ok);
}

fn main() {
    println!("== RTI-Edge 数据面演示（rti-edge）==\n");

    // ---- 安全岛：Deterministic + 镜像 --------------------------------
    println!("安全岛（safety_island）：纯内存硬实时 + UDP 镜像");
    let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut island_cfg = EdgeConfig::safety_island();
    island_cfg.mirror = Some(Mirror::new(rx.local_addr().unwrap()));
    let mut island = EdgeNode::open(island_cfg).unwrap();
    rx.set_nonblocking(true).unwrap();
    let mut pkt = [0u8; 64];
    let mut mirrored = 0u64;
    // 分批发送并即时排空接收端（UDP 接收缓冲有限）
    for chunk in 0..10i64 {
        for t in (chunk * 100)..((chunk + 1) * 100) {
            island.ingest(1, Sample::new(t, t as f64 * 0.5)).unwrap();
        }
        while rx.recv(&mut pkt).is_ok() {
            mirrored += 1;
        }
    }
    island.flush().unwrap();
    while rx.recv(&mut pkt).is_ok() {
        mirrored += 1;
    }
    let n = island.scan(1, 0, 1_000, None, None).unwrap().count();
    println!("  ingest 1000 点（scan 得 {n} 点），镜像接收 {mirrored}/1000 数据报，全程不落盘");
    print_health("安全岛", &island.health());

    // ---- 认知层：Balanced + 3 节点内存 Raft ---------------------------
    println!("\n认知层（cognition）：3 节点内存 Raft 副本（多数派确认才算 durable）");
    let mut cog_cfg = EdgeConfig::cognition();
    cog_cfg.data_dir = Some(demo_dir("cognition"));
    let mut cog = EdgeNode::open(cog_cfg).unwrap();
    for t in 0..100i64 {
        cog.ingest(2, Sample::new(t, (t * t) as f64)).unwrap();
    }
    cog.flush().unwrap();
    let n = cog.scan(2, 0, 100, None, None).unwrap().count();
    println!(
        "  ingest 100 点（scan 得 {n} 点），Raft durable 水位 = {:?}（每条 ingest 一个日志条目）",
        cog.raft_durable_index()
    );
    print_health("认知层", &cog.health());

    // ---- 规划层：Balanced + 冷分层归档 ---------------------------------
    println!("\n规划层（planning）：segment 归档冷层 + scan 透明读回");
    let mut plan_cfg = EdgeConfig::planning();
    let dir = demo_dir("planning");
    plan_cfg.data_dir = Some(dir.join("data"));
    plan_cfg.cold_tier = Some(ColdTierConfig::LocalFs { dir: dir.join("cold") });
    plan_cfg.memtable_max = 256;
    plan_cfg.tsn_align_ns = Some(1_000); // 1µs 网格对齐
    let mut plan = EdgeNode::open(plan_cfg).unwrap();
    for t in 0..1_000i64 {
        plan.ingest(3, Sample::new(t * 100, (t % 17) as f64)).unwrap(); // ts 步进 100ns → 对齐到 µs 网格
    }
    plan.flush().unwrap();
    let before = plan.scan(3, 0, 100_000, None, None).unwrap().count();
    let archived = plan.archive_older_than(100_000).unwrap();
    let after = plan.scan(3, 0, 100_000, None, None).unwrap().count();
    println!(
        "  ingest 1000 点（µs 网格对齐），seal 出 {} 个 segment；归档 {archived} 个到冷层；scan 读回 {before}→{after} 点（一致）",
        plan.health().segment_count
    );
    print_health("规划层", &plan.health());

    // 清理演示数据
    let _ = std::fs::remove_dir_all(demo_dir("cognition"));
    let _ = std::fs::remove_dir_all(dir);
    println!("\n演示完成。");
}
