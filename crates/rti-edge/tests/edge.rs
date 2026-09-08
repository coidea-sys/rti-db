//! rti-edge 集成测试：三档预设 open → ingest → scan → health 往返，
//! 以及 safety_island 档的「全程不落盘」验证（复用 v0.3 机制）。

use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rti_core::{Mirror, Profile, Sample};
use rti_edge::{ColdTierConfig, EdgeConfig, EdgeNode};
use rti_raft::Role;

/// 唯一临时目录（进程 id + 原子序号 + 标签），Drop 时清理。
struct TmpDir(PathBuf);

static SEQ: AtomicU64 = AtomicU64::new(0);

impl TmpDir {
    fn new(label: &str) -> Self {
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let p = std::env::temp_dir().join(format!("rti-edge-test-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }

    /// 一个**不存在**的子路径（用于验证 Deterministic 档不建目录）。
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

/// 场景 1：安全岛档——Deterministic + 镜像；全程不触碰文件系统。
#[test]
fn safety_island_roundtrip_and_no_disk_io() {
    let tmp = TmpDir::new("safety");
    let ghost = tmp.absent("must-not-be-created");

    // 镜像接收端（loopback）
    let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
    let rx_addr = rx.local_addr().unwrap();

    let mut cfg = EdgeConfig::safety_island();
    assert_eq!(cfg.profile, Profile::Deterministic);
    // 故意给一个 data_dir：Deterministic 档也必须绝不触碰
    cfg.data_dir = Some(ghost.clone());
    cfg.mirror = Some(Mirror::new(rx_addr));

    let mut node = EdgeNode::open(cfg).unwrap();
    const N: usize = 2_000;
    // 分批发送并即时排空接收端（UDP 接收缓冲有限，攒到最后读会溢出）
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

    // scan 往返
    let out: Vec<Sample> = node.scan(7, 0, N as i64, None, None).unwrap().collect();
    assert_eq!(out.len(), N);
    assert_eq!(out[123], Sample::new(123, 123.0));

    // health：Deterministic、镜像计数、无 raft、无 segment
    let h = node.health();
    assert_eq!(h.profile, Profile::Deterministic);
    let ms = h.mirror_stats.expect("安全岛档必须有镜像统计");
    assert_eq!(ms.sent, N as u64);
    assert_eq!(ms.failed, 0, "loopback 接收端在线，不允许发送失败");
    assert_eq!(h.raft_role, None);
    assert_eq!(h.segment_count, 0, "纯内存运行不得产生 segment");
    assert!(h.alloc_ok);

    // 不落盘：数据目录从未被创建
    assert!(!ghost.exists(), "Deterministic 档绝不触碰文件系统");

    // 镜像接收端确实收到 >99% 数据报（残余部分最后排空一次）
    while rx.recv(&mut pkt).is_ok() {
        received += 1;
    }
    assert!(received * 100 > N as u64 * 99, "镜像接收率必须 >99%（实收 {received}/{N}）");
}

/// 场景 1b（feature alloc-count）：安全岛档稳态热路径零分配。
#[cfg(feature = "alloc-count")]
#[test]
fn safety_island_steady_state_zero_alloc() {
    let tmp = TmpDir::new("safety-alloc");
    let mut cfg = EdgeConfig::safety_island();
    cfg.data_dir = Some(tmp.absent("ghost"));
    let mut node = EdgeNode::open(cfg).unwrap();

    // 预热（路径上的惰性初始化允许分配）
    samples(&mut node, 1, 0..1_000);
    node.flush().unwrap();
    node.reset_alloc_baseline();

    // 稳态：再来 1000 次 ingest，分配计数必须零增长
    samples(&mut node, 1, 1_000..2_000);
    node.flush().unwrap();
    assert!(node.health().alloc_ok, "Deterministic 稳态热路径必须零分配");
}

/// 场景 2：认知层档——Balanced + 3 节点内存 Raft；ingest 经多数派 durable。
#[test]
fn cognition_roundtrip_with_raft_replication() {
    let tmp = TmpDir::new("cognition");
    let mut cfg = EdgeConfig::cognition();
    cfg.data_dir = Some(tmp.0.join("data"));

    let mut node = EdgeNode::open(cfg).unwrap();
    // open 即完成选主
    assert_eq!(node.health().raft_role, Some(Role::Leader));
    assert_eq!(node.raft_durable_index(), Some(0));

    const N: usize = 100;
    samples(&mut node, 3, 0..N as i64);
    node.flush().unwrap();

    // 每条 ingest = 一个日志条目，全部多数派 durable
    assert_eq!(node.raft_durable_index(), Some(N as u64));

    let out: Vec<Sample> = node.scan(3, 0, N as i64, None, None).unwrap().collect();
    assert_eq!(out.len(), N);

    let h = node.health();
    assert_eq!(h.profile, Profile::Balanced);
    assert_eq!(h.raft_role, Some(Role::Leader));
    assert_eq!(h.mirror_stats, None);
}

/// 场景 3：规划层档——Balanced + 冷分层；归档后 scan 透明读回一致。
#[test]
fn planning_roundtrip_with_cold_tier_archive() {
    let tmp = TmpDir::new("planning");
    let mut cfg = EdgeConfig::planning();
    cfg.data_dir = Some(tmp.0.join("data"));
    cfg.cold_tier = Some(ColdTierConfig::LocalFs { dir: tmp.0.join("cold") });
    cfg.memtable_max = 256; // 小表加速 seal

    let mut node = EdgeNode::open(cfg).unwrap();
    const N: i64 = 1_000;
    samples(&mut node, 5, 0..N);
    node.flush().unwrap();
    assert!(node.health().segment_count >= 2, "小 memtable 必须 seal 出多个 segment");

    // 归档前基线
    let before: Vec<Sample> = node.scan(5, 0, N, None, None).unwrap().collect();
    assert_eq!(before.len(), N as usize);

    // 全部时间段归档 → 本地删除、冷层可读回
    let archived = node.archive_older_than(N + 1).unwrap();
    assert!(archived >= 1);
    assert_eq!(node.archived_segment_count(), archived);

    // 透明读回：结果与归档前逐点一致
    let after: Vec<Sample> = node.scan(5, 0, N, None, None).unwrap().collect();
    assert_eq!(after, before);

    let h = node.health();
    assert_eq!(h.profile, Profile::Balanced);
    assert_eq!(h.raft_role, None);
    assert!(h.segment_count >= archived);
}

/// TSN 网格对齐：ingest 时间戳先向下取整到网格。
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
    assert_eq!(ts, vec![1_000, 2_000, 3_000], "时间戳必须向下取整到网格");
    assert_eq!(out[0].value, 1.0);

    // 非法网格在 open 时即报错
    let mut bad = EdgeConfig::planning();
    bad.tsn_align_ns = Some(0);
    assert!(EdgeNode::open(bad).is_err());
}
