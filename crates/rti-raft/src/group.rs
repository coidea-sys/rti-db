//! Multi-shard Raft (v0.8): several independent Raft groups hosted by one physical node.
//!
//! Endpoint-adapter design (SPEC v0.8 §2): [`Node`] stays group-unaware — each group
//! gets a [`GroupEndpoint`] implementing the existing [`Transport`] trait, while
//! [`MultiNetwork`] keys delivery by `(GroupId, NodeId)`, so equal `NodeId`s in
//! different groups never cross-deliver. [`Router`] owns this physical node's
//! `Node<GroupEndpoint>` for every group and drives ticks/pumps across them.
//!
//! The message codec, wire tags, and the single-group transports
//! (`MemoryNetwork`/`MemoryTransport`/`TcpTransport`) are unchanged; the group tag
//! exists only as in-memory routing metadata inside [`MultiNetwork`].
//!
//! Non-goals (v0.8): shard split/merge, cross-shard transactions, Raft log
//! persistence, multi-group TCP envelopes.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use rti_core::{Error, Result};
use rti_wal::Record;

use crate::node::{Msg, Node, NodeId, Transport, CONF_SERIES};

/// Raft group (shard) identifier. The default single-shard deployment is group `0`.
pub type GroupId = u32;

/// One protocol message in transit, tagged with its group.
///
/// Purely in-memory routing metadata inside [`MultiNetwork`]; the `Msg` codec/tags
/// are unchanged (multi-group TCP envelopes are a later-wave concern).
#[derive(Clone, Debug, PartialEq)]
pub struct GroupMsg {
    /// Group the message belongs to.
    pub group: GroupId,
    /// Sender node id (within the group).
    pub from: NodeId,
    /// The group-unaware protocol message.
    pub inner: Msg,
}

/// Derive a deterministic per-group election seed from a physical node's base seed.
///
/// Same input gives the same output (test determinism); distinct groups get
/// well-mixed seeds so groups sharing a physical node do not campaign in lockstep.
pub fn group_seed(base_seed: u64, group: GroupId) -> u64 {
    // SplitMix64 over (base_seed, group), finalizer identical in style to Node::election_timeout.
    let mut z = base_seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((group as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F))
        .wrapping_add(0x1656_67B1_9E37_79F9);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ------------------------------------------------------------- multi-group in-memory network

#[derive(Default)]
struct SharedMulti {
    /// Delivery queues keyed by `(group, node)` — the same NodeId in different
    /// groups never shares a queue, so groups cannot cross-deliver.
    queues: BTreeMap<(GroupId, NodeId), VecDeque<GroupMsg>>,
    /// Blocked directed edges `(group, from, to)`; `partition` adds both directions.
    blocked: Vec<(GroupId, NodeId, NodeId)>,
}

/// Deterministic multi-group in-memory network (single-threaded; test/simulation).
///
/// Endpoints are created per `(group, node)` via [`MultiNetwork::endpoint`];
/// [`MultiNetwork::partition`] / [`MultiNetwork::heal`] operate on one group at a
/// time so tests can fault a single shard while others keep running.
/// Handles obtained via `Clone` share the same state with all endpoints.
#[derive(Clone, Default)]
pub struct MultiNetwork {
    shared: Rc<RefCell<SharedMulti>>,
}

impl MultiNetwork {
    /// Create an empty multi-group network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `(group, id)` and return its per-group endpoint.
    ///
    /// Re-registering an existing `(group, id)` returns an equivalent endpoint
    /// without disturbing queued messages.
    pub fn endpoint(&self, group: GroupId, id: NodeId) -> GroupEndpoint {
        self.shared
            .borrow_mut()
            .queues
            .entry((group, id))
            .or_default();
        GroupEndpoint {
            group,
            id,
            shared: Rc::clone(&self.shared),
        }
    }

    /// Partition one group: block bidirectional traffic between sets `a` and `b`
    /// **within `group` only** (other groups' traffic between the same ids is unaffected).
    pub fn partition(&self, group: GroupId, a: &[NodeId], b: &[NodeId]) {
        let mut net = self.shared.borrow_mut();
        for &x in a {
            for &y in b {
                net.blocked.push((group, x, y));
                net.blocked.push((group, y, x));
            }
        }
    }

    /// Restore all traffic of one group (other groups' partitions are unaffected).
    pub fn heal(&self, group: GroupId) {
        self.shared
            .borrow_mut()
            .blocked
            .retain(|&(g, _, _)| g != group);
    }

    /// Restore all traffic of all groups.
    pub fn heal_all(&self) {
        self.shared.borrow_mut().blocked.clear();
    }
}

/// Per-group transport endpoint: implements the existing [`Transport`] trait, so
/// `Node<GroupEndpoint>` runs the single-group protocol state machine byte-for-byte.
pub struct GroupEndpoint {
    group: GroupId,
    id: NodeId,
    shared: Rc<RefCell<SharedMulti>>,
}

impl GroupEndpoint {
    /// The group this endpoint belongs to.
    pub fn group(&self) -> GroupId {
        self.group
    }

    /// This endpoint's node id (within the group).
    pub fn id(&self) -> NodeId {
        self.id
    }
}

impl Transport for GroupEndpoint {
    fn send(&mut self, to: NodeId, msg: Msg) {
        let mut net = self.shared.borrow_mut();
        if net.blocked.contains(&(self.group, self.id, to)) {
            return; // packets dropped by this group's partition
        }
        net.queues
            .entry((self.group, to))
            .or_default()
            .push_back(GroupMsg {
                group: self.group,
                from: self.id,
                inner: msg,
            });
    }

    fn recv(&mut self) -> Option<(NodeId, Msg)> {
        let gm = self
            .shared
            .borrow_mut()
            .queues
            .get_mut(&(self.group, self.id))?
            .pop_front()?;
        debug_assert_eq!(gm.group, self.group, "queues are keyed by (group, node)");
        Some((gm.from, gm.inner))
    }
}

// ------------------------------------------------------------- router

/// Local router: owns this physical node's protocol [`Node`] for each group and
/// drives all of them over one shared [`MultiNetwork`].
///
/// Groups are fully independent: elections, logs, commit watermarks, snapshots and
/// memberships advance per group; `propose`/`take_durable` for an unknown group
/// return [`Error::NotFound`], and a non-Leader proposal keeps the single-group
/// retryable error behavior.
pub struct Router {
    net: MultiNetwork,
    nodes: BTreeMap<GroupId, Node<GroupEndpoint>>,
}

impl Router {
    /// Create a router with no groups yet (use [`Router::add_group`]).
    pub fn new(net: MultiNetwork) -> Self {
        Self {
            net,
            nodes: BTreeMap::new(),
        }
    }

    /// Add a group hosted on this physical node.
    ///
    /// The node seed is derived as `group_seed(base_seed, group)` so co-located
    /// groups do not campaign in lockstep; the group's endpoint is registered on the
    /// router's network under `(group, id)`. Adding the same group twice is an error.
    #[allow(clippy::too_many_arguments)]
    pub fn add_group(
        &mut self,
        group: GroupId,
        id: NodeId,
        peers: Vec<NodeId>,
        election_min: u64,
        election_span: u64,
        heartbeat: u64,
        base_seed: u64,
    ) -> Result<()> {
        if self.nodes.contains_key(&group) {
            return Err(Error::Corrupt(format!(
                "raft: group {group} already exists on this node"
            )));
        }
        let endpoint = self.net.endpoint(group, id);
        let node = Node::new(
            id,
            peers,
            endpoint,
            election_min,
            election_span,
            heartbeat,
            group_seed(base_seed, group),
        );
        self.nodes.insert(group, node);
        Ok(())
    }

    /// Groups currently hosted by this router (ascending).
    pub fn groups(&self) -> Vec<GroupId> {
        self.nodes.keys().copied().collect()
    }

    /// Drive the logical clock of every group (heartbeats/election timeouts per group).
    pub fn tick(&mut self, now: u64) {
        for n in self.nodes.values_mut() {
            n.tick(now);
        }
    }

    /// Pump all groups' inboxes until dry (responses generated while handling are
    /// processed in the same call). Returns the number of messages handled.
    pub fn pump(&mut self) -> usize {
        let mut handled = 0;
        loop {
            let mut progress = false;
            for n in self.nodes.values_mut() {
                while let Some((from, msg)) = n.transport_mut().recv() {
                    n.handle(from, msg);
                    handled += 1;
                    progress = true;
                }
            }
            if !progress {
                return handled;
            }
        }
    }

    /// The protocol node of `group` (`None` for unknown groups).
    pub fn node(&self, group: GroupId) -> Option<&Node<GroupEndpoint>> {
        self.nodes.get(&group)
    }

    /// Mutable access to the protocol node of `group` (driving/tests; `None` for unknown groups).
    pub fn node_mut(&mut self, group: GroupId) -> Option<&mut Node<GroupEndpoint>> {
        self.nodes.get_mut(&group)
    }

    /// Propose a batch of WAL records to `group`'s Leader.
    ///
    /// Unknown group: [`Error::NotFound`]. Non-Leader: the existing retryable
    /// not-leader error (the caller should forward to the group's Leader or wait for
    /// an election). Returns the entry index; durability is not guaranteed on return.
    pub fn propose(&mut self, group: GroupId, records: Vec<Record>) -> Result<u64> {
        self.nodes
            .get_mut(&group)
            .ok_or(Error::NotFound)?
            .propose(records)
    }

    /// Take newly confirmed-durable records of `group` (batches flattened in log
    /// order; membership-change sentinel records are filtered, as in `ReplicatedWal`).
    ///
    /// Unknown group: [`Error::NotFound`].
    pub fn take_durable(&mut self, group: GroupId) -> Result<Vec<Record>> {
        let node = self.nodes.get_mut(&group).ok_or(Error::NotFound)?;
        let entries = node.take_committed();
        let n: usize = entries.iter().map(|e| e.records.len()).sum();
        let mut out = Vec::with_capacity(n);
        for e in entries {
            out.extend(e.records.into_iter().filter(|r| r.series != CONF_SERIES));
        }
        Ok(out)
    }

    /// Maximum log index of `group` confirmed durable (majority commit watermark).
    ///
    /// Unknown group: [`Error::NotFound`].
    pub fn durable_index(&self, group: GroupId) -> Result<u64> {
        Ok(self
            .nodes
            .get(&group)
            .ok_or(Error::NotFound)?
            .commit_index())
    }
}
