//! RTI-Edge all-in-one demo: each of the three RTI-L3 tiers uses one preset —
//! safety island (Deterministic + UDP mirror), cognition (3-node in-memory Raft replica),
//! planning (cold-tier archiving + transparent read-back); finally prints three health reports.
//!
//! Run: `cargo run -p rti-edge --example edge_demo`

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
    println!("== RTI-Edge data-plane demo (rti-edge) ==\n");

    // ---- safety island: Deterministic + mirror --------------------------------
    println!("safety island (safety_island): pure in-memory hard real-time + UDP mirror");
    let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut island_cfg = EdgeConfig::safety_island();
    island_cfg.mirror = Some(Mirror::new(rx.local_addr().unwrap()));
    let mut island = EdgeNode::open(island_cfg).unwrap();
    rx.set_nonblocking(true).unwrap();
    let mut pkt = [0u8; 64];
    let mut mirrored = 0u64;
    // send in batches and drain the receiver promptly (UDP receive buffer is limited)
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
    println!("  ingest 1000 points (scan yields {n}), mirror received {mirrored}/1000 datagrams, nothing touches disk");
    print_health("safety island", &island.health());

    // ---- cognition: Balanced + 3-node in-memory Raft ---------------------------
    println!("\ncognition: 3-node in-memory Raft replica (durable only after majority acknowledgment)");
    let mut cog_cfg = EdgeConfig::cognition();
    cog_cfg.data_dir = Some(demo_dir("cognition"));
    let mut cog = EdgeNode::open(cog_cfg).unwrap();
    for t in 0..100i64 {
        cog.ingest(2, Sample::new(t, (t * t) as f64)).unwrap();
    }
    cog.flush().unwrap();
    let n = cog.scan(2, 0, 100, None, None).unwrap().count();
    println!(
        "  ingest 100 points (scan yields {n}), Raft durable watermark = {:?} (one log entry per ingest)",
        cog.raft_durable_index()
    );
    print_health("cognition", &cog.health());

    // ---- planning: Balanced + cold-tier archiving ---------------------------------
    println!("\nplanning: segment archiving to the cold tier + scan transparent read-back");
    let mut plan_cfg = EdgeConfig::planning();
    let dir = demo_dir("planning");
    plan_cfg.data_dir = Some(dir.join("data"));
    plan_cfg.cold_tier = Some(ColdTierConfig::LocalFs { dir: dir.join("cold") });
    plan_cfg.memtable_max = 256;
    plan_cfg.tsn_align_ns = Some(1_000); // 1µs grid alignment
    let mut plan = EdgeNode::open(plan_cfg).unwrap();
    for t in 0..1_000i64 {
        plan.ingest(3, Sample::new(t * 100, (t % 17) as f64)).unwrap(); // ts steps by 100ns -> aligned down to the µs grid
    }
    plan.flush().unwrap();
    let before = plan.scan(3, 0, 100_000, None, None).unwrap().count();
    let archived = plan.archive_older_than(100_000).unwrap();
    let after = plan.scan(3, 0, 100_000, None, None).unwrap().count();
    println!(
        "  ingest 1000 points (µs grid aligned), sealed {} segments; archived {archived} to the cold tier; scan read back {before}->{after} points (identical)",
        plan.health().segment_count
    );
    print_health("planning", &plan.health());

    // clean up demo data
    let _ = std::fs::remove_dir_all(demo_dir("cognition"));
    let _ = std::fs::remove_dir_all(dir);
    println!("\nDemo finished.");
}
