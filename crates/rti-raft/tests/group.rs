//! v0.8 multi-shard Raft integration tests: MultiNetwork + Router + virtual clock, fully deterministic.
//!
//! Covers the SPEC v0.8 §4 acceptance gates for multi-shard Raft:
//! groups elect independently / one partitioned or crashed group does not block another /
//! writes to group A never appear in group B / durable watermarks advance independently /
//! equal NodeIds in different groups cannot cross-deliver / per-group partition & heal /
//! snapshot & membership changes are per-group.

use std::collections::BTreeMap;

use rti_core::Error;
use rti_raft::{group_seed, GroupId, Msg, MultiNetwork, NodeId, Role, Router, Transport};
use rti_wal::Record;

const ELECTION_MIN: u64 = 150;
const ELECTION_SPAN: u64 = 150;
const HEARTBEAT: u64 = 40;
const STEP_MS: u64 = 10;
const BASE_SEED: u64 = 7;

/// Two shards used by most tests.
const A: GroupId = 10;
const B: GroupId = 20;

/// Deterministic multi-group driver: one Router per physical node, all sharing one MultiNetwork.
struct Sim {
    net: MultiNetwork,
    routers: BTreeMap<NodeId, Router>,
    now: u64,
}

impl Sim {
    /// Every physical node hosts every group with identical membership.
    ///
    /// Each physical node gets its own base seed (as in the single-group Cluster):
    /// `group_seed` then staggers election timeouts per (node, group).
    fn new(ids: &[NodeId], groups: &[GroupId]) -> Self {
        let net = MultiNetwork::new();
        let mut routers = BTreeMap::new();
        for &id in ids {
            let mut r = Router::new(net.clone());
            for &g in groups {
                let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
                r.add_group(
                    g,
                    id,
                    peers,
                    ELECTION_MIN,
                    ELECTION_SPAN,
                    HEARTBEAT,
                    BASE_SEED.wrapping_mul(id),
                )
                .unwrap();
            }
            routers.insert(id, r);
        }
        Self {
            net,
            routers,
            now: 0,
        }
    }

    /// Advance one logical step: all routers tick, then all pump until every queue is empty.
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

    /// Ids of physical nodes whose `group` node is currently Leader.
    fn leaders(&self, group: GroupId) -> Vec<NodeId> {
        self.routers
            .values()
            .filter_map(|r| r.node(group))
            .filter(|n| n.is_leader())
            .map(|n| n.id())
            .collect()
    }

    /// Step until `group` has exactly one stable Leader (at most max_steps), returning its node id.
    fn elect(&mut self, group: GroupId, max_steps: usize) -> NodeId {
        for _ in 0..max_steps {
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
        panic!("group {group}: no leader elected within {max_steps} steps");
    }

    /// Propose on the router hosting `group`'s node `via` (must be the Leader to succeed).
    fn propose(&mut self, via: NodeId, group: GroupId, records: Vec<Record>) -> u64 {
        self.routers
            .get_mut(&via)
            .unwrap()
            .propose(group, records)
            .unwrap()
    }
}

fn rec(tag: u64) -> Vec<Record> {
    vec![Record::new(tag as u32, tag as i64, tag as f64)]
}

// ---------------------------------------------------------------- group_seed

/// Same input => same output (determinism); distinct groups never share a derived seed.
#[test]
fn group_seed_deterministic_and_distinct() {
    assert_eq!(group_seed(BASE_SEED, A), group_seed(BASE_SEED, A));
    let seeds: Vec<u64> = (0..64).map(|g| group_seed(BASE_SEED, g)).collect();
    let mut dedup = seeds.clone();
    dedup.sort_unstable();
    dedup.dedup();
    assert_eq!(
        seeds.len(),
        dedup.len(),
        "per-group seeds must be distinct so groups do not campaign in lockstep"
    );
}

// ---------------------------------------------------------------- elections

/// Multiple groups on the same physical nodes each elect exactly one Leader, independently.
#[test]
fn groups_elect_independently() {
    let mut sim = Sim::new(&[1, 2, 3], &[A, B]);
    let la = sim.elect(A, 500);
    let lb = sim.elect(B, 500);
    for (&id, r) in &sim.routers {
        for &g in &[A, B] {
            let n = r.node(g).unwrap();
            assert!(n.term() >= 1);
            if n.is_leader() {
                assert_eq!(n.leader_id(), Some(id));
            } else {
                assert_eq!(n.role(), Role::Follower, "node {id} group {g} must follow");
                assert_eq!(n.leader_id(), Some(if g == A { la } else { lb }));
            }
        }
    }
}

/// Router bookkeeping: duplicate group registration fails; unknown groups yield NotFound / None.
#[test]
fn router_group_bookkeeping() {
    let net = MultiNetwork::new();
    let mut r = Router::new(net);
    assert_eq!(r.groups(), Vec::<GroupId>::new());
    r.add_group(
        A,
        1,
        vec![2, 3],
        ELECTION_MIN,
        ELECTION_SPAN,
        HEARTBEAT,
        BASE_SEED,
    )
    .unwrap();
    assert_eq!(r.groups(), vec![A]);
    assert!(matches!(
        r.add_group(
            A,
            1,
            vec![2, 3],
            ELECTION_MIN,
            ELECTION_SPAN,
            HEARTBEAT,
            BASE_SEED
        ),
        Err(Error::Corrupt(_))
    ));

    assert!(r.node(B).is_none());
    assert!(r.node_mut(B).is_none());
    assert!(matches!(r.propose(B, rec(1)), Err(Error::NotFound)));
    assert!(matches!(r.take_durable(B), Err(Error::NotFound)));
    assert!(matches!(r.durable_index(B), Err(Error::NotFound)));

    // a non-Leader keeps the existing retryable not-leader error behavior
    let err = r.propose(A, rec(1)).unwrap_err();
    assert!(
        matches!(err, Error::Corrupt(_)),
        "not-leader must stay retryable, got {err:?}"
    );
}

// ---------------------------------------------------------------- replication isolation

/// Writes proposed to group A never appear in group B; durable watermarks advance independently.
#[test]
fn writes_and_durable_watermarks_are_per_group() {
    let mut sim = Sim::new(&[1, 2, 3], &[A, B]);
    let la = sim.elect(A, 500);
    let lb = sim.elect(B, 500);

    sim.propose(la, A, rec(1));
    sim.step_n(5);
    for r in sim.routers.values_mut() {
        assert_eq!(
            r.durable_index(A).unwrap(),
            1,
            "group A must commit its entry"
        );
        assert_eq!(
            r.durable_index(B).unwrap(),
            0,
            "group B must stay untouched"
        );
        assert_eq!(
            r.node(B).unwrap().log_len(),
            0,
            "group A writes must never appear in group B's log"
        );
        assert_eq!(r.take_durable(B).unwrap(), Vec::<Record>::new());
        assert_eq!(
            r.take_durable(A).unwrap(),
            rec(1),
            "A's records arrive exactly once, on A"
        );
    }

    // B advances on its own without moving A
    sim.propose(lb, B, rec(101));
    sim.propose(lb, B, rec(102));
    sim.step_n(5);
    for r in sim.routers.values_mut() {
        assert_eq!(
            r.durable_index(A).unwrap(),
            1,
            "A's watermark must not move with B's writes"
        );
        assert_eq!(r.durable_index(B).unwrap(), 2);
        let got = r.take_durable(B).unwrap();
        assert_eq!(got, [rec(101), rec(102)].concat());
    }
}

/// Equal NodeIds in different groups cannot cross-deliver; per-group partition/heal is independent.
///
/// Directly exercises the MultiNetwork queue keying `(GroupId, NodeId)` without any Router.
#[test]
fn same_node_id_groups_do_not_cross_deliver() {
    let net = MultiNetwork::new();
    let mut a1 = net.endpoint(A, 1);
    let mut b1 = net.endpoint(B, 1);
    let mut a2 = net.endpoint(A, 2);
    let mut b2 = net.endpoint(B, 2);
    assert_eq!(a1.group(), A);
    assert_eq!(a1.id(), 1);

    let ma = Msg::VoteResponse {
        term: 1,
        granted: true,
    };
    let mb = Msg::VoteResponse {
        term: 9,
        granted: false,
    };
    a1.send(2, ma.clone());
    b1.send(2, mb.clone());
    // each endpoint receives exactly its own group's message, then runs dry
    assert_eq!(a2.recv(), Some((1, ma.clone())));
    assert_eq!(a2.recv(), None);
    assert_eq!(b2.recv(), Some((1, mb.clone())));
    assert_eq!(b2.recv(), None);

    // a partition on group A drops only group A's traffic
    net.partition(A, &[1], &[2]);
    a1.send(2, ma.clone());
    b1.send(2, mb.clone());
    assert_eq!(a2.recv(), None, "group A is partitioned");
    assert_eq!(
        b2.recv(),
        Some((1, mb.clone())),
        "group B between the same ids is unaffected"
    );
    assert_eq!(b2.recv(), None);

    // healing group A restores it without touching anything else
    net.heal(A);
    a1.send(2, ma.clone());
    assert_eq!(a2.recv(), Some((1, ma)));
    assert_eq!(a2.recv(), None);
}

/// One group partitioned (Leader isolated, majority re-elects) does not block the other group;
/// after per-group heal the partitioned group reconverges and minority entries are overwritten.
#[test]
fn partition_of_one_group_does_not_block_other() {
    let mut sim = Sim::new(&[1, 2, 3], &[A, B]);
    let la = sim.elect(A, 500);
    let lb = sim.elect(B, 500);
    sim.propose(la, A, rec(1));
    sim.propose(lb, B, rec(101));
    sim.step_n(5);
    let old_term_a = sim.routers[&la].node(A).unwrap().term();

    // partition group A only: its Leader alone on the minority side
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != la).collect();
    sim.net.partition(A, &[la], &others);

    // the partitioned group's minority Leader cannot commit
    sim.propose(la, A, rec(2));
    // meanwhile group B between the same physical nodes keeps committing
    sim.propose(lb, B, rec(102));
    sim.step_n(60); // 600ms: far beyond the election timeout
    assert_eq!(
        sim.routers[&la].node(A).unwrap().commit_index(),
        1,
        "group A's minority side must not advance commit"
    );
    for r in sim.routers.values_mut() {
        assert_eq!(
            r.durable_index(B).unwrap(),
            2,
            "group B is unaffected by group A's partition"
        );
    }
    // group A's majority side elects a new Leader under a higher term
    let new_la = others
        .iter()
        .copied()
        .find(|&id| {
            let n = sim.routers[&id].node(A).unwrap();
            n.is_leader() && n.term() > old_term_a
        })
        .expect("group A's majority side must elect a new Leader while partitioned");
    sim.propose(new_la, A, rec(3));
    sim.step_n(5);
    assert_eq!(sim.routers[&new_la].node(A).unwrap().commit_index(), 2);

    // heal group A only: the old Leader steps down, its uncommitted entry is overwritten
    sim.net.heal(A);
    sim.step_n(30);
    let old = sim.routers[&la].node(A).unwrap();
    assert_eq!(
        old.role(),
        Role::Follower,
        "group A's old Leader must step down after heal"
    );
    assert_eq!(
        old.commit_index(),
        2,
        "group A must reconverge on the majority history"
    );
    let log = old.log_entries();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].records, rec(1));
    assert_eq!(
        log[1].records,
        rec(3),
        "minority entry rec(2) must be overwritten"
    );
    // and group B simply keeps working throughout
    sim.propose(lb, B, rec(103));
    sim.step_n(5);
    for r in sim.routers.values_mut() {
        assert_eq!(r.durable_index(B).unwrap(), 3);
    }
}

/// Simulated crash of an entire group (all its members cut off from each other) leaves
/// the other group fully available.
#[test]
fn crashed_group_does_not_block_other_group() {
    let mut sim = Sim::new(&[1, 2, 3], &[A, B]);
    let lb = sim.elect(B, 500);
    sim.elect(A, 500);

    // "crash" group A: every member is cut off from every other member
    sim.net.partition(A, &[1], &[2, 3]);
    sim.net.partition(A, &[2], &[3]);
    sim.step_n(30);

    // group B elects (if needed) and commits without interruption
    let lb = if sim.routers[&lb].node(B).unwrap().is_leader() {
        lb
    } else {
        sim.elect(B, 500)
    };
    sim.propose(lb, B, rec(201));
    sim.step_n(5);
    for r in sim.routers.values_mut() {
        assert_eq!(r.durable_index(B).unwrap(), 1);
        assert_eq!(r.take_durable(B).unwrap(), rec(201));
    }
    // group A made no progress anywhere
    for r in sim.routers.values_mut() {
        assert_eq!(r.durable_index(A).unwrap(), 0);
    }
}

// ---------------------------------------------------------------- snapshot & membership isolation

/// Snapshot + InstallSnapshot in group A do not disturb group B's log, watermarks, or traffic.
#[test]
fn snapshot_is_per_group() {
    let mut sim = Sim::new(&[1, 2, 3], &[A, B]);
    let la = sim.elect(A, 500);
    let lb = sim.elect(B, 500);
    let lag_id = [1, 2, 3].into_iter().find(|&id| id != la).unwrap();
    let rest: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != lag_id).collect();

    // group A: partition a laggard, commit 3 entries, compact the whole prefix
    sim.net.partition(A, &[lag_id], &rest);
    for tag in 1..=3u64 {
        sim.propose(la, A, rec(tag));
        sim.step_n(3);
    }
    let snap = sim
        .routers
        .get_mut(&la)
        .unwrap()
        .node_mut(A)
        .unwrap()
        .take_snapshot(3, b"snap-A".to_vec())
        .unwrap();
    assert_eq!(snap.last_included_index, 3);

    // group B is not partitioned and keeps committing while A compacts
    sim.propose(lb, B, rec(101));
    sim.step_n(5);
    for r in sim.routers.values_mut() {
        let b = r.node(B).unwrap();
        assert_eq!(b.commit_index(), 1);
        assert_eq!(
            b.compacted_index(),
            0,
            "group A's snapshot must not compact group B"
        );
        assert_eq!(b.log_len(), 1);
    }

    // heal group A: the laggard catches up via InstallSnapshot; group B never notices
    sim.net.heal(A);
    sim.step_n(30);
    let lag = sim.routers[&lag_id].node(A).unwrap();
    assert_eq!(
        lag.compacted_index(),
        3,
        "group A's laggard must install the snapshot"
    );
    assert_eq!(lag.commit_index(), 3);
    assert_eq!(
        lag.snapshot().map(|s| s.state.as_slice()),
        Some(b"snap-A".as_slice())
    );
    let lag_b = sim.routers[&lag_id].node(B).unwrap();
    assert_eq!(lag_b.compacted_index(), 0);
    assert_eq!(lag_b.commit_index(), 1);
}

/// A one-step membership change in group A leaves group B's membership and progress untouched.
#[test]
fn membership_change_is_per_group() {
    let mut sim = Sim::new(&[1, 2, 3], &[A, B]);
    // physical node 4 exists but hosts group A only (as a non-voting member to be added)
    let mut r4 = Router::new(sim.net.clone());
    r4.add_group(
        A,
        4,
        vec![1, 2, 3],
        ELECTION_MIN,
        ELECTION_SPAN,
        HEARTBEAT,
        BASE_SEED.wrapping_mul(4),
    )
    .unwrap();
    sim.routers.insert(4, r4);

    let la = sim.elect(A, 500);
    let lb = sim.elect(B, 500);
    sim.routers
        .get_mut(&la)
        .unwrap()
        .node_mut(A)
        .unwrap()
        .add_peer(4)
        .unwrap();
    sim.step_n(10);

    // group A's configuration converges to {1,2,3,4} everywhere in group A
    for (&id, r) in &sim.routers {
        let a = r.node(A).unwrap();
        assert_eq!(
            a.membership(),
            vec![1, 2, 3, 4],
            "group A node {id} must converge to the new configuration"
        );
        if id != 4 {
            let b = r.node(B).unwrap();
            assert_eq!(
                b.membership(),
                vec![1, 2, 3],
                "group B's membership must be unchanged"
            );
        }
    }
    // the membership sentinel never leaks into the user data stream
    for r in sim.routers.values_mut() {
        assert_eq!(r.take_durable(A).unwrap(), Vec::<Record>::new());
    }

    // group A commits under the 4-node configuration; group B is unaffected and still commits
    sim.propose(la, A, rec(1));
    sim.propose(lb, B, rec(101));
    sim.step_n(10);
    for r in sim.routers.values_mut() {
        assert_eq!(
            r.durable_index(A).unwrap(),
            2,
            "conf entry + user entry committed in A"
        );
        if r.node(B).is_some() {
            assert_eq!(r.durable_index(B).unwrap(), 1);
            assert_eq!(r.take_durable(B).unwrap(), rec(101));
        }
    }
}

// ---------------------------------------------------------------- determinism / coexistence

/// Two full runs with identical seeds produce identical outcomes (virtual-clock determinism),
/// and the single-group path stays available alongside groups (group 0 == default shard).
#[test]
fn deterministic_rerun_and_default_group_zero() {
    fn run() -> Vec<(NodeId, u64, u64)> {
        let mut sim = Sim::new(&[1, 2, 3], &[0, A]);
        let l0 = sim.elect(0, 500);
        let la = sim.elect(A, 500);
        sim.propose(l0, 0, rec(5));
        sim.propose(la, A, rec(6));
        sim.step_n(10);
        let mut out: Vec<(NodeId, u64, u64)> = sim
            .routers
            .values()
            .map(|r| {
                let n0 = r.node(0).unwrap();
                (n0.id(), n0.term(), n0.commit_index())
            })
            .collect();
        out.sort();
        out
    }
    let r1 = run();
    let r2 = run();
    assert_eq!(
        r1, r2,
        "same seeds and virtual clock must reproduce identical runs"
    );
    for (_, term, commit) in r1 {
        assert!(term >= 1);
        assert_eq!(commit, 1);
    }
}
