//! Raft 集群集成测试：MemoryTransport + 虚拟时钟，完全确定性。
//!
//! 覆盖 SPEC-evolution Wave 3 点名的四个场景：
//! 3 节点选主收敛 / Leader 宕机重选 / 日志一致性（不丢已提交条目）/
//! 网络分区少数派不可提交。

use rti_raft::{MemoryNetwork, MemoryTransport, Msg, Node, NodeId, ReplicatedWal, Role, Transport};
use rti_wal::Record;

const ELECTION_MIN: u64 = 150;
const ELECTION_SPAN: u64 = 150;
const HEARTBEAT: u64 = 40;
const STEP_MS: u64 = 10;

/// 测试集群驱动：每步推进逻辑时钟并泵空全部消息。
struct Cluster {
    nodes: Vec<Node<MemoryTransport>>,
    net: MemoryNetwork,
    now: u64,
}

impl Cluster {
    /// ids 与 seeds 一一对应；固定种子 ⇒ 选举超时序列确定。
    fn new(ids: &[NodeId], seeds: &[u64]) -> Self {
        let net = MemoryNetwork::new();
        let nodes = ids
            .iter()
            .zip(seeds)
            .map(|(&id, &seed)| {
                let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
                Node::new(
                    id,
                    peers,
                    net.transport(id),
                    ELECTION_MIN,
                    ELECTION_SPAN,
                    HEARTBEAT,
                    seed,
                )
            })
            .collect();
        Self { nodes, net, now: 0 }
    }

    fn three() -> Self {
        Self::new(&[1, 2, 3], &[11, 22, 33])
    }

    /// 推进一个逻辑步：全部节点 tick，然后泵消息直到队列清空。
    fn step(&mut self) {
        self.now += STEP_MS;
        for n in &mut self.nodes {
            n.tick(self.now);
        }
        loop {
            let mut progress = false;
            for n in &mut self.nodes {
                while let Some((from, msg)) = n.transport_mut().recv() {
                    n.handle(from, msg);
                    progress = true;
                }
            }
            if !progress {
                break;
            }
        }
    }

    fn step_n(&mut self, n: usize) {
        for _ in 0..n {
            self.step();
        }
    }

    /// 当前唯一 Leader 的 nodes 下标（无或多于一个返回 None）。
    fn leader(&self) -> Option<usize> {
        let ls: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_leader())
            .map(|(i, _)| i)
            .collect();
        if ls.len() == 1 {
            Some(ls[0])
        } else {
            None
        }
    }

    /// 步进直到出现唯一 Leader（最多 max_steps），返回其下标。
    fn elect(&mut self, max_steps: usize) -> usize {
        for _ in 0..max_steps {
            self.step();
            if let Some(l) = self.leader() {
                // 再稳几步，确认不翻转
                let term = self.nodes[l].term();
                self.step_n(3);
                if self.leader() == Some(l) {
                    return l;
                }
                let _ = term;
            }
        }
        panic!("no leader elected within {max_steps} steps");
    }
}

fn rec(tag: u64) -> Vec<Record> {
    vec![Record::new(tag as u32, tag as i64, tag as f64)]
}

fn committed_records(n: &mut Node<MemoryTransport>) -> Vec<Record> {
    n.take_committed()
        .into_iter()
        .flat_map(|e| e.records)
        .collect()
}

/// 场景 1：3 节点选主收敛——恰好一个 Leader，其余 Follower，任期一致。
#[test]
fn three_node_election_converges() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let term = c.nodes[l].term();
    assert!(term >= 1);
    for (i, n) in c.nodes.iter().enumerate() {
        if i == l {
            assert_eq!(n.role(), Role::Leader);
            assert_eq!(n.leader_id(), Some(n.id()));
        } else {
            assert_eq!(n.role(), Role::Follower, "node {} must follow", n.id());
            assert_eq!(n.leader_id(), Some(c.nodes[l].id()), "node {} must know leader", n.id());
        }
        assert_eq!(n.term(), term, "任期必须收敛一致");
    }
}

/// 场景 2：Leader 宕机后其余节点重选出新 Leader（任期递增）。
#[test]
fn leader_crash_triggers_reelection() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let old_id = c.nodes[l].id();
    let old_term = c.nodes[l].term();
    // 宕机：直接移除节点（其传输端点随之消失）
    c.nodes.remove(l);
    let l2 = c.elect(500);
    let new_id = c.nodes[l2].id();
    assert_ne!(new_id, old_id, "必须另选新 Leader");
    assert!(c.nodes[l2].term() > old_term, "新任期必须递增");
    // 存活的另一个节点跟随新 Leader
    let other = c.nodes.iter().find(|n| n.id() != new_id).unwrap();
    assert_eq!(other.leader_id(), Some(new_id));
}

/// 场景 3：日志一致性——多数派提交后 Leader 宕机，新 Leader 不丢已提交条目。
#[test]
fn committed_entries_survive_leader_crash() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    c.nodes[l].propose(rec(100)).unwrap();
    c.step_n(5);
    c.nodes[l].propose(rec(200)).unwrap();
    c.step_n(5);
    // 全体节点 commit 水位应到 2
    for n in &c.nodes {
        assert_eq!(n.commit_index(), 2, "node {} 必须已提交两条", n.id());
    }
    // 杀掉 Leader，重选
    c.nodes.remove(l);
    let l2 = c.elect(500);
    // 新 Leader 日志必须包含全部已提交条目
    let log = c.nodes[l2].log_entries();
    assert!(log.len() >= 2, "新 Leader 必须携带已提交日志");
    assert_eq!(log[0].records, rec(100));
    assert_eq!(log[1].records, rec(200));
    // 新 Leader 继续提议，已提交序列线性延伸
    c.nodes[l2].propose(rec(300)).unwrap();
    c.step_n(5);
    let log = c.nodes[l2].log_entries();
    assert_eq!(log.len(), 3);
    assert_eq!(log[2].records, rec(300));
    assert_eq!(c.nodes[l2].commit_index(), 3);
    // 存活跟随者取走全部已提交记录，顺序与内容一致
    let other_idx = if l2 == 0 { 1 } else { 0 };
    let got = committed_records(&mut c.nodes[other_idx]);
    let want: Vec<Record> = [rec(100), rec(200), rec(300)].concat();
    assert_eq!(got, want, "已提交记录序列不允许丢失/乱序");
}

/// 场景 4：网络分区时少数派（含旧 Leader）不可提交；
/// 愈合后少数派条目被多数派覆盖，已提交历史不受影响。
#[test]
fn partitioned_minority_cannot_commit() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    c.nodes[l].propose(rec(1)).unwrap();
    c.step_n(5);
    assert_eq!(c.nodes[l].commit_index(), 1);

    // 分区：旧 Leader 单独一侧（少数派），另两个节点一侧
    let majority_ids: Vec<NodeId> = c
        .nodes
        .iter()
        .map(|n| n.id())
        .filter(|&id| id != leader_id)
        .collect();
    c.net.partition(&[leader_id], &majority_ids);

    // 旧 Leader 在分区中提议——永远凑不齐多数派
    c.nodes[l].propose(rec(2)).unwrap();
    c.step_n(30);
    assert_eq!(
        c.nodes[l].commit_index(),
        1,
        "少数派侧不得推进 commit（丢包即不可用，不可用即安全）"
    );

    // 多数派侧选出新 Leader（旧 Leader 分区中仍自以为是 Leader，
    // 故 elect() 的唯一 Leader 判定不适用，直接在多数派侧找）
    let old_term = c.nodes[l].term();
    let mut l2 = None;
    for _ in 0..500 {
        c.step();
        for (i, n) in c.nodes.iter().enumerate() {
            if n.is_leader() && majority_ids.contains(&n.id()) && n.term() > old_term {
                l2 = Some(i);
            }
        }
        if l2.is_some() {
            break;
        }
    }
    let l2 = l2.expect("多数派侧必须选出新 Leader");
    c.nodes[l2].propose(rec(3)).unwrap();
    c.step_n(5);
    assert_eq!(c.nodes[l2].commit_index(), 2);

    // 愈合：旧 Leader 退位，少数派条目被覆盖，历史保留
    c.net.heal();
    c.step_n(30);
    let old = c.nodes.iter_mut().find(|n| n.id() == leader_id).unwrap();
    assert_eq!(old.role(), Role::Follower, "旧 Leader 必须退位");
    assert_eq!(old.commit_index(), 2, "愈合后必须追平多数派历史");
    let log = old.log_entries();
    assert_eq!(log.len(), 2, "少数派未提交条目必须被截断覆盖");
    assert_eq!(log[0].records, rec(1), "已提交历史不允许改变");
    assert_eq!(log[1].records, rec(3), "愈合后必须复制多数派的新条目");
    let got = committed_records(old);
    assert_eq!(got, [rec(1), rec(3)].concat(), "少数派条目永不出现在已提交流中");
}

/// ReplicatedWal 集成：多数派确认前不可见 durable，确认后按序取回。
#[test]
fn replicated_wal_durable_only_after_majority() {
    let net = MemoryNetwork::new();
    let ids = [1u64, 2, 3];
    let mut wals: Vec<ReplicatedWal<MemoryTransport>> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            let node = Node::new(
                id,
                peers,
                net.transport(id),
                ELECTION_MIN,
                ELECTION_SPAN,
                HEARTBEAT,
                (i as u64 + 1) * 111,
            );
            ReplicatedWal::new(node)
        })
        .collect();

    let mut now = 0u64;
    let pump = |wals: &mut [ReplicatedWal<MemoryTransport>], now: &mut u64, steps: usize| {
        for _ in 0..steps {
            *now += STEP_MS;
            for w in wals.iter_mut() {
                w.tick(*now);
            }
            loop {
                let mut progress = false;
                for w in wals.iter_mut() {
                    while let Some((from, msg)) = w.node_mut().transport_mut().recv() {
                        w.handle(from, msg);
                        progress = true;
                    }
                }
                if !progress {
                    break;
                }
            }
        }
    };

    // 选出 Leader
    let mut leader = None;
    for _ in 0..500 {
        pump(&mut wals, &mut now, 1);
        let ls: Vec<usize> = wals
            .iter()
            .enumerate()
            .filter(|(_, w)| w.is_leader())
            .map(|(i, _)| i)
            .collect();
        if ls.len() == 1 {
            leader = Some(ls[0]);
            break;
        }
    }
    let l = leader.expect("election must converge");

    // 非 Leader 提议被拒绝
    let follower = (l + 1) % 3;
    assert!(wals[follower].append_batch(rec(9)).is_err());

    // 提议但消息未泵：未达多数派，durable 水位不动
    let idx = wals[l].append_batch(rec(42)).unwrap();
    assert_eq!(idx, 1);
    // （消息已发送但跟随者尚未处理——把泵停了语义等价于复制未完成）
    assert!(wals[l].take_durable().is_empty(), "多数派确认前不得交付");

    // 泵消息完成复制：durable 推进，按序交付
    pump(&mut wals, &mut now, 5);
    assert_eq!(wals[l].durable_index(), 1);
    assert_eq!(wals[l].take_durable(), rec(42), "确认后按原批次交付");
    // 再无可交付记录
    assert!(wals[l].take_durable().is_empty());
}

/// TCP 传输 loopback 双向收发。
#[test]
fn tcp_transport_loopback_roundtrip() {
    use rti_raft::{TcpTransport, Transport};
    use std::net::SocketAddr;

    let lo: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    let mut t1 = TcpTransport::new(1, SocketAddr::new(lo, 0), &[]).unwrap();
    let mut t2 = TcpTransport::new(2, SocketAddr::new(lo, 0), &[]).unwrap();
    let a1 = t1.local_addr().unwrap();
    let a2 = t2.local_addr().unwrap();
    t1.add_peer(2, a2);
    t2.add_peer(1, a1);
    assert_eq!(t1.id(), 1);

    let m1 = Msg::RequestVote { term: 5, candidate: 1, last_log_index: 7, last_log_term: 4 };
    let m2 = Msg::AppendEntries {
        term: 5,
        leader: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![rti_raft::Entry { term: 5, records: rec(77) }],
        leader_commit: 0,
    };
    t1.send(2, m1.clone());
    t2.send(1, m2.clone());

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut got1 = None;
    let mut got2 = None;
    while std::time::Instant::now() < deadline && (got1.is_none() || got2.is_none()) {
        if got1.is_none() {
            got1 = t1.recv();
        }
        if got2.is_none() {
            got2 = t2.recv();
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(got1, Some((2, m2)), "t1 必须收到 t2 的消息且发送者 id 正确");
    assert_eq!(got2, Some((1, m1)));
}
