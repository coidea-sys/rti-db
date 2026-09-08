//! Wave 5（v0.5）集成测试：快照 / 日志压缩 / 单步成员变更 / PreVote。
//!
//! 与 cluster.rs 相同的确定性框架：MemoryTransport + 虚拟时钟，
//! 固定种子 ⇒ 全部时序可复现。

use rti_raft::{
    MemoryNetwork, MemoryTransport, Msg, Node, NodeId, ReplicatedWal, Role, Transport, CONF_SERIES,
};
use rti_wal::Record;

const ELECTION_MIN: u64 = 150;
const ELECTION_SPAN: u64 = 150;
const HEARTBEAT: u64 = 40;
const STEP_MS: u64 = 10;

/// 确定性测试集群（支持运行中加节点/移除节点模拟宕机）。
struct Cluster {
    nodes: Vec<Node<MemoryTransport>>,
    net: MemoryNetwork,
    now: u64,
}

impl Cluster {
    fn new(ids: &[NodeId], seeds: &[u64]) -> Self {
        let net = MemoryNetwork::new();
        let nodes = ids
            .iter()
            .zip(seeds)
            .map(|(&id, &seed)| {
                let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
                Node::new(id, peers, net.transport(id), ELECTION_MIN, ELECTION_SPAN, HEARTBEAT, seed)
            })
            .collect();
        Self { nodes, net, now: 0 }
    }

    fn three() -> Self {
        Self::new(&[1, 2, 3], &[11, 22, 33])
    }

    /// 运行中补注册一个节点（初始配置 = 参数给定 peers）。
    fn join(&mut self, id: NodeId, peers: &[NodeId], seed: u64) {
        let t = self.net.transport(id);
        self.nodes.push(Node::new(
            id,
            peers.to_vec(),
            t,
            ELECTION_MIN,
            ELECTION_SPAN,
            HEARTBEAT,
            seed,
        ));
    }

    fn idx(&self, id: NodeId) -> usize {
        self.nodes.iter().position(|n| n.id() == id).expect("node exists")
    }

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

    fn elect(&mut self, max_steps: usize) -> usize {
        for _ in 0..max_steps {
            self.step();
            if let Some(l) = self.leader() {
                self.step_n(3);
                if self.leader() == Some(l) {
                    return l;
                }
            }
        }
        panic!("no leader elected within {max_steps} steps");
    }
}

fn rec(tag: u64) -> Vec<Record> {
    vec![Record::new(tag as u32, tag as i64, tag as f64)]
}

// ---------------------------------------------------------------- 快照

/// 点名场景 1：快照安装后落后 Follower 追平。
///
/// 分区一个 Follower → Leader 继续提交 6 条并压缩全部前缀 →
/// 愈合后 Follower 经 InstallSnapshot 跳到快照点，随后正常追赶新条目。
#[test]
fn snapshot_install_catches_up_lagging_follower() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    let lag_id = [1u64, 2, 3].into_iter().find(|&id| id != leader_id).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != lag_id).collect();

    // 分区落后者，Leader 与另一节点（仍是多数派）继续提交
    c.net.partition(&[lag_id], &rest);
    for tag in 1..=6u64 {
        let l = c.idx(leader_id);
        c.nodes[l].propose(rec(tag)).unwrap();
        c.step_n(3);
    }
    let l = c.idx(leader_id);
    assert_eq!(c.nodes[l].commit_index(), 6, "多数派在线，6 条必须提交");

    // 压缩全部已提交前缀
    let snap = c.nodes[l].take_snapshot(6, b"snap-state-v1".to_vec()).unwrap();
    assert_eq!(snap.last_included_index, 6);
    assert_eq!(c.nodes[l].compacted_index(), 6);
    assert!(c.nodes[l].log_entries().is_empty(), "前缀必须被丢弃");

    // 愈合：落后者经 InstallSnapshot 追平
    c.net.heal();
    c.step_n(30);
    let f = c.idx(lag_id);
    assert_eq!(c.nodes[f].compacted_index(), 6, "Follower 必须安装快照");
    assert_eq!(c.nodes[f].commit_index(), 6);
    assert_eq!(
        c.nodes[f].snapshot().map(|s| s.state.as_slice()),
        Some(b"snap-state-v1".as_slice()),
        "快照状态字节必须不透明送达"
    );

    // 快照后继续复制新条目：Follower 正常追平并交付
    let l = c.idx(leader_id);
    c.nodes[l].propose(rec(7)).unwrap();
    c.step_n(10);
    let f = c.idx(lag_id);
    assert_eq!(c.nodes[f].commit_index(), 7);
    let got: Vec<Record> = c.nodes[f]
        .take_committed()
        .into_iter()
        .flat_map(|e| e.records)
        .collect();
    assert_eq!(got, rec(7), "快照点前的条目不重复交付，快照后的按序交付");
}

/// 点名场景 2：压缩后旧索引请求被拒并触发快照。
///
/// 直接观测线路上发给落后 Follower 的第一条消息是 InstallSnapshot
/// 而非 AppendEntries（所需前缀已压缩，AppendEntries 无从对齐）；
/// 附带校验 take_snapshot 的边界拒绝。
#[test]
fn compacted_prefix_triggers_install_snapshot() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    let lag_id = [1u64, 2, 3].into_iter().find(|&id| id != leader_id).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != lag_id).collect();

    // 边界：超过 commit / 不超过已压缩点都必须拒绝
    let l = c.idx(leader_id);
    c.nodes[l].propose(rec(1)).unwrap();
    c.step_n(3);
    let l = c.idx(leader_id);
    assert_eq!(c.nodes[l].commit_index(), 1);
    assert!(c.nodes[l].take_snapshot(2, vec![]).is_err(), "不得压缩未提交条目");
    let snap = c.nodes[l].take_snapshot(1, b"v1".to_vec()).unwrap();
    assert!(c.nodes[l].take_snapshot(1, vec![]).is_err(), "不得重复压缩同一点");
    assert!(c.nodes[l].take_snapshot(0, vec![]).is_err());

    // 分区落后者再提交两条并压缩 → 其 next_index(=2) 落在已压缩区
    c.net.partition(&[lag_id], &rest);
    let l = c.idx(leader_id);
    c.nodes[l].propose(rec(2)).unwrap();
    c.nodes[l].propose(rec(3)).unwrap();
    c.step_n(5);
    let l = c.idx(leader_id);
    assert_eq!(c.nodes[l].commit_index(), 3);
    c.nodes[l].take_snapshot(3, b"v3".to_vec()).unwrap();
    assert_eq!(c.nodes[l].compacted_index(), 3);

    // 愈合并让 Leader 到点发心跳：落后者的第一条消息必须是 InstallSnapshot
    c.net.heal();
    c.now += HEARTBEAT;
    let l = c.idx(leader_id);
    c.nodes[l].tick(c.now);
    let f = c.idx(lag_id);
    let (from, msg) = c.nodes[f]
        .transport_mut()
        .recv()
        .expect("愈合后 Leader 必须立即联系落后者");
    assert_eq!(from, leader_id);
    match msg {
        Msg::InstallSnapshot { term, leader, snapshot } => {
            assert_eq!(leader, leader_id);
            assert_eq!(term, c.nodes[l].term());
            assert_eq!(snapshot.last_included_index, 3);
            assert_eq!(snapshot.state, b"v3".to_vec());
            // 应用之，随后泵消息收敛
            c.nodes[f].handle(from, Msg::InstallSnapshot { term, leader, snapshot });
        }
        other => panic!("压缩后旧索引请求必须触发 InstallSnapshot，实得 {other:?}"),
    }
    c.step_n(20);
    let f = c.idx(lag_id);
    assert_eq!(c.nodes[f].compacted_index(), 3);
    assert_eq!(c.nodes[f].commit_index(), 3, "快照装好后 commit 必须追平");

    // 陈旧快照的本地安装是幂等空操作
    let f = c.idx(lag_id);
    c.nodes[f].install_snapshot(snap.clone()).unwrap();
    assert_eq!(c.nodes[f].compacted_index(), 3, "旧快照不得回退压缩点");
}

// ---------------------------------------------------------------- 成员变更

/// 点名场景 3：add_peer 后 4 节点选主与提交。
///
/// 新节点在加入配置前的预投票被在位成员拒绝（防扰乱）；变更提交后
/// 全配置 {1,2,3,4}；杀掉旧 Leader 后 4 节点（需 3 票）重选并提交。
#[test]
fn add_peer_then_four_node_election_and_commit() {
    let mut c = Cluster::three();
    // 新节点自带"我属于 4 节点集群"的视图加入（追赶中的常见形态）
    c.join(4, &[1, 2, 3], 44);
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();

    // 未加入配置前：节点 4 的预投票被在位成员拒绝（无法成为
    // Candidate/Leader）；它至多经应答*跟随*集群任期，绝不能超出
    let f4 = c.idx(4);
    let cluster_term = c.nodes[l].term();
    assert!(c.nodes[f4].term() <= cluster_term, "非配置成员不得把任期抬过集群");
    assert_ne!(c.nodes[f4].role(), Role::Candidate);
    assert!(!c.nodes[f4].is_leader());

    // 一次只变一个：pending 期间的第二次变更被拒
    let l = c.idx(leader_id);
    let conf_idx = c.nodes[l].add_peer(4).unwrap();
    assert!(c.nodes[l].add_peer(5).is_err(), "前一变更未提交时必须拒绝新变更");
    c.step_n(10);

    // 变更经多数派提交后在全员生效
    let l = c.idx(leader_id);
    assert!(c.nodes[l].commit_index() >= conf_idx, "变更条目必须已提交");
    for n in &c.nodes {
        assert_eq!(n.membership(), vec![1, 2, 3, 4], "node {} 配置必须收敛", n.id());
    }
    // 变更条目确实经过日志（哨兵对用户接口透明，协议层可见）
    let committed: Vec<_> = c.nodes[l].take_committed();
    assert!(
        committed.iter().flat_map(|e| e.records.iter()).any(|r| r.series == CONF_SERIES),
        "配置必须以日志条目形式复制提交"
    );

    // 新节点追平日志（含变更条目本身）
    let f4 = c.idx(4);
    assert_eq!(c.nodes[f4].commit_index(), c.nodes[l].commit_index());

    // 杀掉旧 Leader：4 节点配置（多数派 = 3）重选
    let dead = c.idx(leader_id);
    c.nodes.remove(dead);
    let l2 = c.elect(1000);
    let new_id = c.nodes[l2].id();
    assert_ne!(new_id, leader_id);
    assert_eq!(c.nodes[l2].membership(), vec![1, 2, 3, 4]);

    // 新 Leader 提交：3 存活节点恰为 4 节点配置的多数派
    c.nodes[l2].propose(rec(100)).unwrap();
    c.step_n(10);
    for n in &c.nodes {
        assert_eq!(n.commit_index(), c.nodes[l2].commit_index());
        assert!(n.commit_index() > conf_idx, "node {} 必须提交新条目", n.id());
    }
}

/// 点名场景 4：remove_peer 后旧节点不再参与多数派。
///
/// Leader 移除自己：变更提交后退位降级为非投票成员；剩余 {2,3}
/// 两人配置自行完成选主与提交，旧节点全程静默（不选举、不投票、
/// 任期不再变化）。
#[test]
fn remove_peer_excludes_old_node_from_quorum() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let old_id = c.nodes[l].id();
    let old_term = c.nodes[l].term();

    c.nodes[l].remove_peer(old_id).unwrap();
    // 移除不存在的成员必须报错（且前一变更 pending，99 本就不在配置）
    assert!(c.nodes[l].remove_peer(99).is_err());
    c.step_n(10);

    // 变更条目已在旧 Leader 处提交：它退位并降级为非投票成员
    let want: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != old_id).collect();
    let old = c.idx(old_id);
    assert_eq!(c.nodes[old].membership(), want, "旧 Leader 本地配置必须先生效");
    assert_eq!(c.nodes[old].role(), Role::Follower, "被移除的 Leader 必须退位");
    assert!(!c.nodes[old].is_voter(), "被移除者必须降级为非投票成员");

    // 剩余节点先按旧配置 {1,2,3}（多数派 2，旧节点不投票）重选；
    // 新 Leader 复制新任期条目后间接提交变更，配置收敛为 {2,3}
    let mut l2 = None;
    for _ in 0..1000 {
        c.step();
        l2 = c.leader();
        if l2.is_some() {
            break;
        }
    }
    let l2 = l2.expect("剩余两人必须选出 Leader");
    assert!(want.contains(&c.nodes[l2].id()), "Leader 必须来自新配置");
    assert!(c.nodes[l2].term() > old_term);

    // 新 Leader 提交新条目（旧节点完全不参与多数派计数）
    c.nodes[l2].propose(rec(50)).unwrap();
    c.step_n(20);
    let new_commit = c.nodes[l2].commit_index();
    for n in &c.nodes {
        assert_eq!(n.membership(), want, "node {} 配置必须收敛", n.id());
    }
    let other = c.nodes.iter().find(|n| want.contains(&n.id()) && n.id() != c.nodes[l2].id()).unwrap();
    assert_eq!(other.commit_index(), new_commit);

    // 旧节点隔离运行：永不发起选举（非投票成员），任期冻结
    c.net.partition(&[old_id], &want);
    let frozen_term = c.nodes[c.idx(old_id)].term();
    c.step_n(100);
    let old = c.idx(old_id);
    assert_eq!(c.nodes[old].role(), Role::Follower);
    assert_eq!(c.nodes[old].term(), frozen_term, "非投票成员不得预投票抬任期");
}

// ---------------------------------------------------------------- PreVote

/// 点名场景 5：PreVote 下分区节点无法抬高任期。
///
/// 分区一个 Follower：它反复预投票（角色停在 PreCandidate）但
/// 任期原地不动；集群侧 Leader 与任期均不受扰；愈合后它直接
/// 以原任期回归 Follower——对比无 PreVote 时分区节点的
/// RequestVote(term+k) 会迫使全集群抬任期。
#[test]
fn prevote_partitioned_node_cannot_inflate_term() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    let term = c.nodes[l].term();
    let iso_id = [1u64, 2, 3].into_iter().find(|&id| id != leader_id).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != iso_id).collect();

    c.net.partition(&[iso_id], &rest);
    c.step_n(100); // 1000ms：远超选举超时，足够多轮预投票

    let iso = c.idx(iso_id);
    assert_eq!(c.nodes[iso].role(), Role::PreCandidate, "分区节点应停在预投票阶段");
    assert_eq!(c.nodes[iso].term(), term, "预投票不得抬高自身任期");
    let l = c.idx(leader_id);
    assert!(c.nodes[l].is_leader(), "在位 Leader 不得被分区节点扰乱");
    assert_eq!(c.nodes[l].term(), term, "集群任期不得被抬");

    // 愈合：分区节点直接回归，全集群任期零扰动
    c.net.heal();
    c.step_n(20);
    for n in &c.nodes {
        assert_eq!(n.term(), term, "node {} 任期必须原样收敛", n.id());
    }
    let iso = c.idx(iso_id);
    assert_eq!(c.nodes[iso].role(), Role::Follower);
    assert_eq!(c.nodes[iso].leader_id(), Some(leader_id));
}

// ---------------------------------------------------------------- ReplicatedWal 集成

/// 成员变更哨兵对 ReplicatedWal 的用户数据流透明。
#[test]
fn replicated_wal_filters_membership_sentinel() {
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

    let mut leader = None;
    for _ in 0..500 {
        pump(&mut wals, &mut now, 1);
        let ls: Vec<usize> = wals.iter().enumerate().filter(|(_, w)| w.is_leader()).map(|(i, _)| i).collect();
        if ls.len() == 1 {
            leader = Some(ls[0]);
            break;
        }
    }
    let l = leader.expect("election must converge");

    // 变更条目提交后：durable 流为空（哨兵被过滤），但 durable 水位推进
    let before = wals[l].durable_index();
    wals[l].node_mut().add_peer(99).unwrap();
    pump(&mut wals, &mut now, 10);
    assert!(wals[l].durable_index() > before, "变更条目必须推进 durable 水位");
    assert!(wals[l].take_durable().is_empty(), "哨兵记录不得出现在用户数据流");

    // 用户数据照常按序交付（新配置 {1,2,3,99} 下 3 真实节点仍够多数派）
    wals[l].append_batch(rec(7)).unwrap();
    pump(&mut wals, &mut now, 10);
    assert_eq!(wals[l].take_durable(), rec(7));
}
