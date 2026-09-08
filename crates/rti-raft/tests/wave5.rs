//! Wave 5 (v0.5) integration tests: snapshots / log compaction / single-step membership changes / PreVote.
//!
//! Same deterministic framework as cluster.rs: MemoryTransport + virtual clock,
//! fixed seeds => all timing is reproducible.

use rti_raft::{
    MemoryNetwork, MemoryTransport, Msg, Node, NodeId, ReplicatedWal, Role, Transport, CONF_SERIES,
};
use rti_wal::Record;

const ELECTION_MIN: u64 = 150;
const ELECTION_SPAN: u64 = 150;
const HEARTBEAT: u64 = 40;
const STEP_MS: u64 = 10;

/// Deterministic test cluster (supports adding/removing nodes at runtime to simulate crashes).
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

    /// Register an additional node at runtime (initial configuration = the given peers).
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

// ---------------------------------------------------------------- snapshots

/// Named scenario 1: a lagging Follower catches up after snapshot installation.
///
/// Partition one Follower → the Leader commits 6 more entries and compacts the whole prefix →
/// after healing, the Follower jumps to the snapshot point via InstallSnapshot, then catches up with new entries normally.
#[test]
fn snapshot_install_catches_up_lagging_follower() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    let lag_id = [1u64, 2, 3].into_iter().find(|&id| id != leader_id).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != lag_id).collect();

    // partition the laggard; the Leader and the other node (still a majority) keep committing
    c.net.partition(&[lag_id], &rest);
    for tag in 1..=6u64 {
        let l = c.idx(leader_id);
        c.nodes[l].propose(rec(tag)).unwrap();
        c.step_n(3);
    }
    let l = c.idx(leader_id);
    assert_eq!(c.nodes[l].commit_index(), 6, "with the majority online, all 6 entries must commit");

    // compact the entire committed prefix
    let snap = c.nodes[l].take_snapshot(6, b"snap-state-v1".to_vec()).unwrap();
    assert_eq!(snap.last_included_index, 6);
    assert_eq!(c.nodes[l].compacted_index(), 6);
    assert!(c.nodes[l].log_entries().is_empty(), "the prefix must be discarded");

    // heal: the laggard catches up via InstallSnapshot
    c.net.heal();
    c.step_n(30);
    let f = c.idx(lag_id);
    assert_eq!(c.nodes[f].compacted_index(), 6, "the Follower must install the snapshot");
    assert_eq!(c.nodes[f].commit_index(), 6);
    assert_eq!(
        c.nodes[f].snapshot().map(|s| s.state.as_slice()),
        Some(b"snap-state-v1".as_slice()),
        "snapshot state bytes must arrive opaquely"
    );

    // keep replicating new entries after the snapshot: the Follower catches up and delivers normally
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
    assert_eq!(got, rec(7), "entries before the snapshot point are not redelivered; entries after it are delivered in order");
}

/// Named scenario 2: after compaction, old-index requests are rejected and trigger a snapshot.
///
/// Directly observe that the first message sent to the lagging Follower on the wire is an
/// InstallSnapshot rather than AppendEntries (the needed prefix is compacted, so AppendEntries
/// cannot align); also verifies take_snapshot's boundary rejections.
#[test]
fn compacted_prefix_triggers_install_snapshot() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    let lag_id = [1u64, 2, 3].into_iter().find(|&id| id != leader_id).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != lag_id).collect();

    // boundaries: beyond commit / not beyond the compacted point must both be rejected
    let l = c.idx(leader_id);
    c.nodes[l].propose(rec(1)).unwrap();
    c.step_n(3);
    let l = c.idx(leader_id);
    assert_eq!(c.nodes[l].commit_index(), 1);
    assert!(c.nodes[l].take_snapshot(2, vec![]).is_err(), "must not compact uncommitted entries");
    let snap = c.nodes[l].take_snapshot(1, b"v1".to_vec()).unwrap();
    assert!(c.nodes[l].take_snapshot(1, vec![]).is_err(), "must not compact the same point twice");
    assert!(c.nodes[l].take_snapshot(0, vec![]).is_err());

    // partition the laggard, commit two more entries and compact -> its next_index(=2) falls in the compacted region
    c.net.partition(&[lag_id], &rest);
    let l = c.idx(leader_id);
    c.nodes[l].propose(rec(2)).unwrap();
    c.nodes[l].propose(rec(3)).unwrap();
    c.step_n(5);
    let l = c.idx(leader_id);
    assert_eq!(c.nodes[l].commit_index(), 3);
    c.nodes[l].take_snapshot(3, b"v3".to_vec()).unwrap();
    assert_eq!(c.nodes[l].compacted_index(), 3);

    // heal and let the Leader's heartbeat come due: the laggard's first message must be an InstallSnapshot
    c.net.heal();
    c.now += HEARTBEAT;
    let l = c.idx(leader_id);
    c.nodes[l].tick(c.now);
    let f = c.idx(lag_id);
    let (from, msg) = c.nodes[f]
        .transport_mut()
        .recv()
        .expect("after healing the Leader must contact the laggard immediately");
    assert_eq!(from, leader_id);
    match msg {
        Msg::InstallSnapshot { term, leader, snapshot } => {
            assert_eq!(leader, leader_id);
            assert_eq!(term, c.nodes[l].term());
            assert_eq!(snapshot.last_included_index, 3);
            assert_eq!(snapshot.state, b"v3".to_vec());
            // apply it, then pump messages to converge
            c.nodes[f].handle(from, Msg::InstallSnapshot { term, leader, snapshot });
        }
        other => panic!("an old-index request after compaction must trigger InstallSnapshot, got {other:?}"),
    }
    c.step_n(20);
    let f = c.idx(lag_id);
    assert_eq!(c.nodes[f].compacted_index(), 3);
    assert_eq!(c.nodes[f].commit_index(), 3, "commit must catch up once the snapshot is installed");

    // locally installing a stale snapshot is an idempotent no-op
    let f = c.idx(lag_id);
    c.nodes[f].install_snapshot(snap.clone()).unwrap();
    assert_eq!(c.nodes[f].compacted_index(), 3, "an old snapshot must not move the compaction point backwards");
}

// ---------------------------------------------------------------- membership changes

/// Named scenario 3: 4-node election and commit after add_peer.
///
/// The new node's pre-votes are rejected by incumbent members before it joins the configuration
/// (anti-disruption); after the change commits, the configuration is {1,2,3,4}; killing the old Leader re-elects among 4 nodes (3 votes needed) and commits.
#[test]
fn add_peer_then_four_node_election_and_commit() {
    let mut c = Cluster::three();
    // the new node joins with its own 'I belong to a 4-node cluster' view (a common shape while catching up)
    c.join(4, &[1, 2, 3], 44);
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();

    // before joining the configuration: node 4's pre-votes are rejected by incumbents (it cannot become
    // Candidate/Leader); at most it *follows* the cluster term via responses — it must never exceed it
    let f4 = c.idx(4);
    let cluster_term = c.nodes[l].term();
    assert!(c.nodes[f4].term() <= cluster_term, "a non-configuration member must not raise the term above the cluster's");
    assert_ne!(c.nodes[f4].role(), Role::Candidate);
    assert!(!c.nodes[f4].is_leader());

    // one at a time: a second change during a pending one is rejected
    let l = c.idx(leader_id);
    let conf_idx = c.nodes[l].add_peer(4).unwrap();
    assert!(c.nodes[l].add_peer(5).is_err(), "a new change must be rejected while the previous one is uncommitted");
    c.step_n(10);

    // the change takes effect on everyone once majority-committed
    let l = c.idx(leader_id);
    assert!(c.nodes[l].commit_index() >= conf_idx, "the change entry must be committed");
    for n in &c.nodes {
        assert_eq!(n.membership(), vec![1, 2, 3, 4], "node {} configuration must converge", n.id());
    }
    // the change entry does travel through the log (the sentinel is transparent to the user interface, visible at the protocol layer)
    let committed: Vec<_> = c.nodes[l].take_committed();
    assert!(
        committed.iter().flat_map(|e| e.records.iter()).any(|r| r.series == CONF_SERIES),
        "the configuration must be replicated and committed as a log entry"
    );

    // the new node catches up on the log (including the change entry itself)
    let f4 = c.idx(4);
    assert_eq!(c.nodes[f4].commit_index(), c.nodes[l].commit_index());

    // kill the old Leader: re-election under the 4-node configuration (majority = 3)
    let dead = c.idx(leader_id);
    c.nodes.remove(dead);
    let l2 = c.elect(1000);
    let new_id = c.nodes[l2].id();
    assert_ne!(new_id, leader_id);
    assert_eq!(c.nodes[l2].membership(), vec![1, 2, 3, 4]);

    // the new Leader commits: the 3 surviving nodes are exactly a majority of the 4-node configuration
    c.nodes[l2].propose(rec(100)).unwrap();
    c.step_n(10);
    for n in &c.nodes {
        assert_eq!(n.commit_index(), c.nodes[l2].commit_index());
        assert!(n.commit_index() > conf_idx, "node {} must commit the new entries", n.id());
    }
}

/// Named scenario 4: after remove_peer the old node no longer participates in majorities.
///
/// The Leader removes itself: it steps down and is demoted to non-voting member once the change commits;
/// the remaining {2,3} two-node configuration elects and commits on its own, while the old node stays
/// silent throughout (no elections, no votes, term never changes).
#[test]
fn remove_peer_excludes_old_node_from_quorum() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let old_id = c.nodes[l].id();
    let old_term = c.nodes[l].term();

    c.nodes[l].remove_peer(old_id).unwrap();
    // removing a nonexistent member must error (and the previous change is pending; 99 is not in the configuration anyway)
    assert!(c.nodes[l].remove_peer(99).is_err());
    c.step_n(10);

    // the change entry is committed at the old Leader: it steps down and is demoted to non-voting member
    let want: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != old_id).collect();
    let old = c.idx(old_id);
    assert_eq!(c.nodes[old].membership(), want, "the old Leader's local configuration must take effect first");
    assert_eq!(c.nodes[old].role(), Role::Follower, "the removed Leader must step down");
    assert!(!c.nodes[old].is_voter(), "the removed node must be demoted to non-voting member");

    // the remaining nodes first re-elect under the old configuration {1,2,3} (majority 2, the old node does not vote);
    // after the new Leader replicates a new-term entry, the change commits indirectly and the configuration converges to {2,3}
    let mut l2 = None;
    for _ in 0..1000 {
        c.step();
        l2 = c.leader();
        if l2.is_some() {
            break;
        }
    }
    let l2 = l2.expect("the remaining two must elect a Leader");
    assert!(want.contains(&c.nodes[l2].id()), "the Leader must come from the new configuration");
    assert!(c.nodes[l2].term() > old_term);

    // the new Leader commits new entries (the old node plays no part in majority counting at all)
    c.nodes[l2].propose(rec(50)).unwrap();
    c.step_n(20);
    let new_commit = c.nodes[l2].commit_index();
    for n in &c.nodes {
        assert_eq!(n.membership(), want, "node {} configuration must converge", n.id());
    }
    let other = c.nodes.iter().find(|n| want.contains(&n.id()) && n.id() != c.nodes[l2].id()).unwrap();
    assert_eq!(other.commit_index(), new_commit);

    // the old node runs in isolation: never starts an election (non-voting member), term frozen
    c.net.partition(&[old_id], &want);
    let frozen_term = c.nodes[c.idx(old_id)].term();
    c.step_n(100);
    let old = c.idx(old_id);
    assert_eq!(c.nodes[old].role(), Role::Follower);
    assert_eq!(c.nodes[old].term(), frozen_term, "a non-voting member must not raise its term via pre-votes");
}

// ---------------------------------------------------------------- PreVote

/// Named scenario 5: under PreVote a partitioned node cannot inflate the term.
///
/// Partition one Follower: it keeps pre-voting (role stuck at PreCandidate) but its term never
/// moves; the cluster-side Leader and term are undisturbed; after healing it rejoins directly as a
/// Follower with its original term — in contrast, without PreVote a partitioned node's
/// RequestVote(term+k) would force the whole cluster to raise its term.
#[test]
fn prevote_partitioned_node_cannot_inflate_term() {
    let mut c = Cluster::three();
    let l = c.elect(500);
    let leader_id = c.nodes[l].id();
    let term = c.nodes[l].term();
    let iso_id = [1u64, 2, 3].into_iter().find(|&id| id != leader_id).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != iso_id).collect();

    c.net.partition(&[iso_id], &rest);
    c.step_n(100); // 1000ms: far beyond the election timeout, enough for many pre-vote rounds

    let iso = c.idx(iso_id);
    assert_eq!(c.nodes[iso].role(), Role::PreCandidate, "the partitioned node should stay in the pre-vote stage");
    assert_eq!(c.nodes[iso].term(), term, "pre-votes must not raise one's own term");
    let l = c.idx(leader_id);
    assert!(c.nodes[l].is_leader(), "the incumbent Leader must not be disrupted by the partitioned node");
    assert_eq!(c.nodes[l].term(), term, "the cluster term must not be raised");

    // heal: the partitioned node rejoins directly; zero term disturbance cluster-wide
    c.net.heal();
    c.step_n(20);
    for n in &c.nodes {
        assert_eq!(n.term(), term, "node {} term must converge unchanged", n.id());
    }
    let iso = c.idx(iso_id);
    assert_eq!(c.nodes[iso].role(), Role::Follower);
    assert_eq!(c.nodes[iso].leader_id(), Some(leader_id));
}

// ---------------------------------------------------------------- ReplicatedWal integration

/// Membership-change sentinels are transparent to ReplicatedWal's user data stream.
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

    // after the change entry commits: the durable stream is empty (the sentinel is filtered), but the durable watermark advances
    let before = wals[l].durable_index();
    wals[l].node_mut().add_peer(99).unwrap();
    pump(&mut wals, &mut now, 10);
    assert!(wals[l].durable_index() > before, "the change entry must advance the durable watermark");
    assert!(wals[l].take_durable().is_empty(), "sentinel records must not appear in the user data stream");

    // user data is still delivered in order (under the new configuration {1,2,3,99}, the 3 real nodes still form a majority)
    wals[l].append_batch(rec(7)).unwrap();
    pump(&mut wals, &mut now, 10);
    assert_eq!(wals[l].take_durable(), rec(7));
}
