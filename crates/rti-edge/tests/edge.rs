//! rti-edge integration tests: open → ingest → scan → health round-trips for the three presets,
//! plus the safety_island preset's \"never touches disk\" verification (reusing the v0.3 machinery).

use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rti_core::{Mirror, Profile, Sample};
use rti_edge::{ColdTierConfig, EdgeConfig, EdgeNode};
use rti_raft::Role;

/// Unique temp directory (process id + atomic counter + label), cleaned up on Drop.
struct TmpDir(PathBuf);

static SEQ: AtomicU64 = AtomicU64::new(0);

impl TmpDir {
    fn new(label: &str) -> Self {
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let p = std::env::temp_dir().join(format!("rti-edge-test-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }

    /// A **nonexistent** sub-path (used to verify the Deterministic profile creates no directories).
    fn absent(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn samples(node: &mut EdgeNode, series: u32, tags: std::ops::Range<i64>) -> usize {
    let mut n = 0;
    for t in tags {
        node.ingest(series, Sample::new(t, t as f64)).unwrap();
        n += 1;
    }
    n
}

/// Scenario 1: safety-island preset — Deterministic + mirror; never touches the file system.
#[test]
fn safety_island_roundtrip_and_no_disk_io() {
    let tmp = TmpDir::new("safety");
    let ghost = tmp.absent("must-not-be-created");

    // mirror receiver (loopback)
    let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
    let rx_addr = rx.local_addr().unwrap();

    let mut cfg = EdgeConfig::safety_island();
    assert_eq!(cfg.profile, Profile::Deterministic);
    // deliberately provide a data_dir: the Deterministic profile must still never touch it
    cfg.data_dir = Some(ghost.clone());
    cfg.mirror = Some(Mirror::new(rx_addr));

    let mut node = EdgeNode::open(cfg).unwrap();
    const N: usize = 2_000;
    // send in batches and drain the receiver promptly (the UDP receive buffer is limited; reading only at the end would overflow)
    rx.set_nonblocking(true).unwrap();
    let mut received = 0u64;
    let mut pkt = [0u8; 64];
    for chunk in 0..(N / 200) {
        samples(&mut node, 7, (chunk * 200) as i64..((chunk + 1) * 200) as i64);
        while rx.recv(&mut pkt).is_ok() {
            received += 1;
        }
    }
    node.flush().unwrap();

    // scan round-trip
    let out: Vec<Sample> = node.scan(7, 0, N as i64, None, None).unwrap().collect();
    assert_eq!(out.len(), N);
    assert_eq!(out[123], Sample::new(123, 123.0));

    // health: Deterministic, mirror counters, no raft, no segments
    let h = node.health();
    assert_eq!(h.profile, Profile::Deterministic);
    let ms = h.mirror_stats.expect("the safety-island preset must have mirror statistics");
    assert_eq!(ms.sent, N as u64);
    assert_eq!(ms.failed, 0, "the loopback receiver is online; send failures are not allowed");
    assert_eq!(h.raft_role, None);
    assert_eq!(h.segment_count, 0, "pure in-memory operation must not produce segments");
    assert!(h.alloc_ok);

    // no persistence: the data directory was never created
    assert!(!ghost.exists(), "the Deterministic profile never touches the file system");

    // the mirror receiver did receive >99% of datagrams (drain the remainder one last time)
    while rx.recv(&mut pkt).is_ok() {
        received += 1;
    }
    assert!(received * 100 > N as u64 * 99, "mirror receive rate must be >99% (got {received}/{N})");
}

/// Scenario 1b (feature alloc-count): zero allocations on the safety-island steady-state hot path.
#[cfg(feature = "alloc-count")]
#[test]
fn safety_island_steady_state_zero_alloc() {
    let tmp = TmpDir::new("safety-alloc");
    let mut cfg = EdgeConfig::safety_island();
    cfg.data_dir = Some(tmp.absent("ghost"));
    let mut node = EdgeNode::open(cfg).unwrap();

    // warmup (lazy initialization on the path may allocate)
    samples(&mut node, 1, 0..1_000);
    node.flush().unwrap();
    node.reset_alloc_baseline();

    // steady state: 1000 more ingests; the allocation count must not grow
    samples(&mut node, 1, 1_000..2_000);
    node.flush().unwrap();
    assert!(node.health().alloc_ok, "the Deterministic steady-state hot path must allocate zero");
}

/// Scenario 2: cognition preset — Balanced + 3-node in-memory Raft; ingest is majority-durable.
#[test]
fn cognition_roundtrip_with_raft_replication() {
    let tmp = TmpDir::new("cognition");
    let mut cfg = EdgeConfig::cognition();
    cfg.data_dir = Some(tmp.0.join("data"));

    let mut node = EdgeNode::open(cfg).unwrap();
    // open completes leader election
    assert_eq!(node.health().raft_role, Some(Role::Leader));
    assert_eq!(node.raft_durable_index(), Some(0));

    const N: usize = 100;
    samples(&mut node, 3, 0..N as i64);
    node.flush().unwrap();

    // each ingest = one log entry, all majority-durable
    assert_eq!(node.raft_durable_index(), Some(N as u64));

    let out: Vec<Sample> = node.scan(3, 0, N as i64, None, None).unwrap().collect();
    assert_eq!(out.len(), N);

    let h = node.health();
    assert_eq!(h.profile, Profile::Balanced);
    assert_eq!(h.raft_role, Some(Role::Leader));
    assert_eq!(h.mirror_stats, None);
}

/// Scenario 3: planning preset — Balanced + cold tiering; scan reads back transparently and identically after archiving.
#[test]
fn planning_roundtrip_with_cold_tier_archive() {
    let tmp = TmpDir::new("planning");
    let mut cfg = EdgeConfig::planning();
    cfg.data_dir = Some(tmp.0.join("data"));
    cfg.cold_tier = Some(ColdTierConfig::LocalFs { dir: tmp.0.join("cold") });
    cfg.memtable_max = 256; // small table to speed up sealing

    let mut node = EdgeNode::open(cfg).unwrap();
    const N: i64 = 1_000;
    samples(&mut node, 5, 0..N);
    node.flush().unwrap();
    assert!(node.health().segment_count >= 2, "a small memtable must seal out multiple segments");

    // baseline before archiving
    let before: Vec<Sample> = node.scan(5, 0, N, None, None).unwrap().collect();
    assert_eq!(before.len(), N as usize);

    // archive all time ranges -> local files deleted, cold tier readable back
    let archived = node.archive_older_than(N + 1).unwrap();
    assert!(archived >= 1);
    assert_eq!(node.archived_segment_count(), archived);

    // transparent read-back: results point-for-point identical to before archiving
    let after: Vec<Sample> = node.scan(5, 0, N, None, None).unwrap().collect();
    assert_eq!(after, before);

    let h = node.health();
    assert_eq!(h.profile, Profile::Balanced);
    assert_eq!(h.raft_role, None);
    assert!(h.segment_count >= archived);
}

/// TSN grid alignment: ingest timestamps are first floored to the grid.
#[test]
fn tsn_align_applies_on_ingest() {
    let tmp = TmpDir::new("tsn");
    let mut cfg = EdgeConfig::planning();
    cfg.data_dir = Some(tmp.0.join("data"));
    cfg.cold_tier = None;
    cfg.tsn_align_ns = Some(1_000);

    let mut node = EdgeNode::open(cfg).unwrap();
    node.ingest(9, Sample::new(1_500, 1.0)).unwrap();
    node.ingest(9, Sample::new(2_500, 2.0)).unwrap();
    node.ingest(9, Sample::new(3_000, 3.0)).unwrap();
    let out: Vec<Sample> = node.scan(9, 0, 10_000, None, None).unwrap().collect();
    let ts: Vec<i64> = out.iter().map(|s| s.ts).collect();
    assert_eq!(ts, vec![1_000, 2_000, 3_000], "timestamps must be floored to the grid");
    assert_eq!(out[0].value, 1.0);

    // an illegal grid is rejected at open
    let mut bad = EdgeConfig::planning();
    bad.tsn_align_ns = Some(0);
    assert!(EdgeNode::open(bad).is_err());
}
