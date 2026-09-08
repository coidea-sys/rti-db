//! Raft cluster integration tests: MemoryTransport + virtual clock, fully deterministic.
//!
//! Covers the four scenarios named by SPEC-evolution Wave 3:
//! 3-node election convergence / Leader crash and re-election / log consistency
//! (no committed entry is lost) / a minority under network partition cannot commit.

use rti_raft::{MemoryNetwork, MemoryTransport, Msg, Node, NodeId, ReplicatedWal, Role, Transport};
use rti_wal::Record;

const ELECTION_MIN: u64 = 150;
const ELECTION_SPAN: u64 = 150;
const HEARTBEAT: u64 = 40;
const STEP_MS: u64 = 10;

/// Test cluster driver: each step advances the logical clock and pumps all messages dry.
struct Cluster {
    nodes: Vec<Node<MemoryTransport>>,
    net: MemoryNetwork,
    now: u64,
}

impl Cluster {
    /// ids correspond to seeds one-to-one; fixed seeds => deterministic election-timeout sequences.
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

    /// Advance one logical step: all nodes tick, then pump messages until the queues are empty.
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

    /// Index in `nodes` of the single current Leader (`None` if zero or more than one).
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

    /// Step until a single Leader emerges (at most max_steps), returning its index.
    fn elect(&mut self, max_steps: usize) -> usize {
        for _ in 0..max_steps {
            self.step();
            if let Some(l) = self.leader() {
                // hold steady for a few more steps to confirm it does not flip
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

/// Scenario 1: 3-node election convergence — exactly one Leader, the rest Followers, terms agree.
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
        assert_eq!(n.term(), term, "terms must converge");
    }
}

/// Scenario 2: after the Leader crashes, the remaining nodes elect a new Leader (term increases).
#[test]
fn leader_crash_triggers_reelection() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let old_id = c.nodes[l].id();
    let old_term = c.nodes[l].term();
    // crash: simply remove the node (its transport endpoint disappears with it)
    c.nodes.remove(l);
    let l2 = c.elect(500);
    let new_id = c.nodes[l2].id();
    assert_ne!(new_id, old_id, "a new Leader must be elected");
    assert!(c.nodes[l2].term() > old_term, "the new term must increase");
    // the other surviving node follows the new Leader
    let other = c.nodes.iter().find(|n| n.id() != new_id).unwrap();
    assert_eq!(other.leader_id(), Some(new_id));
}

/// Scenario 3: log consistency — after a majority commit, the Leader crashes; the new Leader must not lose committed entries.
#[test]
fn committed_entries_survive_leader_crash() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    c.nodes[l].propose(rec(100)).unwrap();
    c.step_n(5);
    c.nodes[l].propose(rec(200)).unwrap();
    c.step_n(5);
    // every node's commit watermark should reach 2
    for n in &c.nodes {
        assert_eq!(n.commit_index(), 2, "node {} must have committed both entries", n.id());
    }
    // kill the Leader and re-elect
    c.nodes.remove(l);
    let l2 = c.elect(500);
    // the new Leader's log must contain all committed entries
    let log = c.nodes[l2].log_entries();
    assert!(log.len() >= 2, "the new Leader must carry the committed log");
    assert_eq!(log[0].records, rec(100));
    assert_eq!(log[1].records, rec(200));
    // the new Leader keeps proposing; the committed sequence extends linearly
    c.nodes[l2].propose(rec(300)).unwrap();
    c.step_n(5);
    let log = c.nodes[l2].log_entries();
    assert_eq!(log.len(), 3);
    assert_eq!(log[2].records, rec(300));
    assert_eq!(c.nodes[l2].commit_index(), 3);
    // a surviving follower takes all committed records, in order and with identical content
    let other_idx = if l2 == 0 { 1 } else { 0 };
    let got = committed_records(&mut c.nodes[other_idx]);
    let want: Vec<Record> = [rec(100), rec(200), rec(300)].concat();
    assert_eq!(got, want, "the committed record sequence must not be lost or reordered");
}

/// Scenario 4: under a network partition the minority (including the old Leader) cannot commit;
/// after healing, minority entries are overwritten by the majority; committed history is unaffected.
#[test]
fn partitioned_minority_cannot_commit() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    c.nodes[l].propose(rec(1)).unwrap();
    c.step_n(5);
    assert_eq!(c.nodes[l].commit_index(), 1);

    // partition: the old Leader alone on one side (minority), the other two nodes on the other side
    let majority_ids: Vec<NodeId> = c
        .nodes
        .iter()
        .map(|n| n.id())
        .filter(|&id| id != leader_id)
        .collect();
    c.net.partition(&[leader_id], &majority_ids);

    // the old Leader proposes inside the partition — it can never assemble a majority
    c.nodes[l].propose(rec(2)).unwrap();
    c.step_n(30);
    assert_eq!(
        c.nodes[l].commit_index(),
        1,
        "the minority side must not advance commit (packet loss means unavailability, unavailability means safety)"
    );

    // the majority side elects a new Leader (the old Leader still believes itself Leader inside the
    // partition, so elect()'s single-Leader check does not apply — search directly on the majority side)
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
    let l2 = l2.expect("the majority side must elect a new Leader");
    c.nodes[l2].propose(rec(3)).unwrap();
    c.step_n(5);
    assert_eq!(c.nodes[l2].commit_index(), 2);

    // heal: the old Leader steps down, minority entries are overwritten, history is preserved
    c.net.heal();
    c.step_n(30);
    let old = c.nodes.iter_mut().find(|n| n.id() == leader_id).unwrap();
    assert_eq!(old.role(), Role::Follower, "the old Leader must step down");
    assert_eq!(old.commit_index(), 2, "after healing it must catch up with the majority history");
    let log = old.log_entries();
    assert_eq!(log.len(), 2, "uncommitted minority entries must be truncated and overwritten");
    assert_eq!(log[0].records, rec(1), "committed history must not change");
    assert_eq!(log[1].records, rec(3), "after healing it must replicate the majority's new entries");
    let got = committed_records(old);
    assert_eq!(got, [rec(1), rec(3)].concat(), "minority entries must never appear in the committed stream");
}

/// ReplicatedWal integration: not visible as durable before majority acknowledgment; taken back in order afterwards.
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

    // elect a Leader
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

    // proposals to a non-Leader are rejected
    let follower = (l + 1) % 3;
    assert!(wals[follower].append_batch(rec(9)).is_err());

    // proposed but messages not pumped: no majority, so the durable watermark does not move
    let idx = wals[l].append_batch(rec(42)).unwrap();
    assert_eq!(idx, 1);
    // (messages sent but not yet processed by followers — stopping the pump is semantically equivalent to incomplete replication)
    assert!(wals[l].take_durable().is_empty(), "must not deliver before majority acknowledgment");

    // pump messages to complete replication: durable advances, delivered in order
    pump(&mut wals, &mut now, 5);
    assert_eq!(wals[l].durable_index(), 1);
    assert_eq!(wals[l].take_durable(), rec(42), "delivered in the original batches after acknowledgment");
    // no more records to deliver
    assert!(wals[l].take_durable().is_empty());
}

/// TCP transport loopback bidirectional send/receive.
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
    assert_eq!(got1, Some((2, m2)), "t1 must receive t2's message with the correct sender id");
    assert_eq!(got2, Some((1, m1)));
}
