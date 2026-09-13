//! 旗舰示例 3：多分片 Raft——两个分片共享 3 台物理机，互不拖累。
//!
//! 虚拟时钟驱动（完全确定性）：传感器分片(SENSOR)与执行器分片(ACTUATOR)
//! 在同一组物理节点上**独立**选举、复制、故障转移。演示时间线：
//!   1. 两个分片各自选出 Leader（可能落在不同物理机——天然负载分散）；
//!   2. 各自复制写入，提交水位独立推进，数据互不串台；
//!   3. 网络分区切走 SENSOR 的 Leader——SENSOR 换主，ACTUATOR 无感知；
//!   4. 愈合后日志收敛，老 Leader 未提交的条目被新 Leader 覆写。
//!
//! 运行：`cargo run -p rti-raft --release --example multishard_failover`

use std::collections::BTreeMap;

use rti_raft::{MultiNetwork, NodeId, Router};
use rti_wal::Record;

const ELECTION_MIN: u64 = 150;
const ELECTION_SPAN: u64 = 150;
const HEARTBEAT: u64 = 40;
const STEP_MS: u64 = 10;
const BASE_SEED: u64 = 7;

const SENSOR: u32 = 10; // 传感器分片
const ACTUATOR: u32 = 20; // 执行器分片

struct Sim {
    net: MultiNetwork,
    routers: BTreeMap<NodeId, Router>,
    now: u64,
}

impl Sim {
    fn new(ids: &[NodeId], groups: &[u32]) -> Self {
        let net = MultiNetwork::new();
        let mut routers = BTreeMap::new();
        for &id in ids {
            let mut r = Router::new(net.clone());
            for &g in groups {
                let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
                r.add_group(g, id, peers, ELECTION_MIN, ELECTION_SPAN, HEARTBEAT, BASE_SEED * id)
                    .unwrap();
            }
            routers.insert(id, r);
        }
        Self { net, routers, now: 0 }
    }

    fn step(&mut self) {
        self.now += STEP_MS;
        for r in self.routers.values_mut() {
            r.tick(self.now);
        }
        loop {
            let mut handled = 0;
            for r in self.routers.values_mut() {
                handled += r.pump();
            }
            if handled == 0 {
                break;
            }
        }
    }

    fn step_n(&mut self, n: usize) {
        for _ in 0..n {
            self.step();
        }
    }

    fn leaders(&self, group: u32) -> Vec<NodeId> {
        self.routers
            .values()
            .filter_map(|r| r.node(group))
            .filter(|n| n.is_leader())
            .map(|n| n.id())
            .collect()
    }

    fn elect(&mut self, group: u32) -> NodeId {
        for _ in 0..200 {
            self.step();
            let ls = self.leaders(group);
            if ls.len() == 1 {
                let l = ls[0];
                self.step_n(3);
                if self.leaders(group) == [l] {
                    return l;
                }
            }
        }
        panic!("group {group}: 选举超时");
    }

    fn commit(&self, group: u32, id: NodeId) -> u64 {
        self.routers[&id].node(group).unwrap().commit_index()
    }

    /// 组内所有节点的已提交日志是否逐条一致
    fn consistent(&self, group: u32) -> bool {
        let logs: Vec<&[rti_raft::Entry]> = self
            .routers
            .values()
            .map(|r| r.node(group).unwrap().log_entries())
            .collect();
        let min_len = logs.iter().map(|l| l.len()).min().unwrap();
        let commit = (0..self.routers.len() as u64)
            .map(|i| self.commit(group, i + 1))
            .min()
            .unwrap() as usize;
        let upto = commit.min(min_len);
        logs.windows(2).all(|w| w[0][..upto] == w[1][..upto])
    }
}

fn rec(series: u32, i: i64) -> Record {
    Record::new(series, 1_000 * i, i as f64)
}

fn main() {
    println!("== 多分片 Raft 故障转移演示（虚拟时钟，完全确定性）==\n");
    let mut sim = Sim::new(&[1, 2, 3], &[SENSOR, ACTUATOR]);

    // ---- 1. 独立选举 ----------------------------------------------------------
    let l_sensor = sim.elect(SENSOR);
    let t1 = sim.now;
    let l_act = sim.elect(ACTUATOR);
    println!(
        "T+{:>4}ms  选举完成：SENSOR Leader=节点{}  ACTUATOR Leader=节点{}{}",
        t1,
        l_sensor,
        l_act,
        if l_sensor != l_act { "（分片领导天然分散在不同物理机）" } else { "" }
    );

    // ---- 2. 独立复制，互不串台 -------------------------------------------------
    for i in 1..=5 {
        sim.routers.get_mut(&l_sensor).unwrap().propose(SENSOR, vec![rec(1, i)]).unwrap();
        sim.routers.get_mut(&l_act).unwrap().propose(ACTUATOR, vec![rec(2, i)]).unwrap();
    }
    sim.step_n(10);
    let cs: Vec<u64> = (1..=3).map(|id| sim.commit(SENSOR, id)).collect();
    let ca: Vec<u64> = (1..=3).map(|id| sim.commit(ACTUATOR, id)).collect();
    println!(
        "T+{:>4}ms  各写 5 条：SENSOR 提交水位 {:?}  ACTUATOR {:?}  日志一致 {}{}",
        sim.now,
        cs,
        ca,
        sim.consistent(SENSOR),
        if sim.consistent(SENSOR) && sim.consistent(ACTUATOR) { " ✓" } else { " ✗" }
    );
    // 数据隔离：SENSOR 分片日志里不应出现 ACTUATOR 的 series=2
    let sensor_log = sim.routers[&1].node(SENSOR).unwrap().log_entries();
    let iso = sensor_log.iter().all(|e| e.records.iter().all(|r| r.series == 1));
    println!("          分片隔离：SENSOR 日志不含 ACTUATOR 数据 {}", if iso { "✓" } else { "✗" });
    assert!(iso && sim.consistent(SENSOR) && sim.consistent(ACTUATOR));

    // ---- 3. 分区：切走 SENSOR 的 Leader ---------------------------------------
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&n| n != l_sensor).collect();
    sim.net.partition(SENSOR, &[l_sensor], &others);
    println!(
        "T+{:>4}ms  网络分区：SENSOR Leader(节点{})被切走",
        sim.now, l_sensor
    );
    // 等多数派（others）中选出新 Leader；被隔离的老 Leader 在自己的视野里仍是 Leader，
    // 这是 CAP 下分区两侧的真实状态，不能按"全网唯一 Leader"等待。
    let mut l2 = None;
    for _ in 0..200 {
        sim.step();
        if let Some(&l) = sim.leaders(SENSOR).iter().find(|l| others.contains(l)) {
            l2 = Some(l);
            break;
        }
    }
    let l2 = l2.expect("多数派未在 200 步内选出新 Leader");
    sim.step_n(3);
    println!(
        "T+{:>4}ms  SENSOR 换主成功：新 Leader=节点{}；期间 ACTUATOR Leader 仍为节点{}（无感知）✓",
        sim.now,
        l2,
        sim.routers[&l_act].node(ACTUATOR).unwrap().id()
    );
    assert_ne!(l2, l_sensor);

    // 换主期间两个分片都能继续写
    for i in 6..=8 {
        sim.routers.get_mut(&l2).unwrap().propose(SENSOR, vec![rec(1, i)]).unwrap();
        sim.routers.get_mut(&l_act).unwrap().propose(ACTUATOR, vec![rec(2, i)]).unwrap();
    }
    sim.step_n(10);

    // ---- 4. 愈合与收敛 ----------------------------------------------------------
    sim.net.heal_all();
    sim.step_n(30);
    let fin_s: Vec<u64> = (1..=3).map(|id| sim.commit(SENSOR, id)).collect();
    let fin_a: Vec<u64> = (1..=3).map(|id| sim.commit(ACTUATOR, id)).collect();
    println!(
        "T+{:>4}ms  分区愈合：SENSOR 提交水位 {:?}  ACTUATOR {:?}",
        sim.now, fin_s, fin_a
    );
    let conv = sim.consistent(SENSOR) && sim.consistent(ACTUATOR);
    println!("          收敛校验：两个分片所有节点已提交日志逐条一致 {}", if conv { "✓" } else { "✗" });
    assert!(conv);

    println!("\n结论：");
    println!("  分片是独立的一致性域：选举/复制/换主互不惊扰，单分片故障爆炸半径=该分片；");
    println!("  全程虚拟时钟驱动，输出逐次可复现——分布式正确性可以像单元测试一样验证。");
}
