//! rti-raft: a single-shard, simplified Raft replica for rti-db (v0.5).
//!
//! Scope (honest disclosure): single shard, in-memory log; covers the four Raft essentials —
//! leader election (deterministically seeded 'random' timeouts + PreVote), heartbeats,
//! log replication (AppendEntries with conflict truncation), majority commit; v0.5 adds
//! snapshots (InstallSnapshot RPC + log compaction) and single-step membership changes
//! (`add_peer`/`remove_peer`, effective once committed, one at a time).
//!
//! - [`Node`]: the protocol state machine — **time is injected by the caller as a logical
//!   clock** (`tick(now)`); tests use a virtual clock for full determinism;
//! - [`Transport`]: transport abstraction (`send`/`recv`, packet loss allowed — Raft tolerates
//!   it naturally), with [`MemoryTransport`] (test/simulation, supports network partitions) and
//!   [`TcpTransport`] (loopback / real deployment);
//! - [`ReplicatedWal`]: integration point with rti-wal — a log entry is a batch of WAL
//!   [`Record`]s, **durable only after majority acknowledgment (commit)**.

#![forbid(unsafe_code)]

mod codec;
mod node;
mod transport;

pub use codec::{decode_msg, encode_msg};
pub use node::{ConfChange, Entry, Msg, Node, NodeId, Role, Snapshot, Transport, CONF_SERIES};
pub use transport::{MemoryNetwork, MemoryTransport, TcpTransport};

use rti_core::{Error, Result};
use rti_wal::Record;

/// Replicated WAL: a thin wrapper over [`Node`] mapping Raft commit semantics to WAL durability semantics.
///
/// - [`ReplicatedWal::append_batch`] proposes a batch of [`Record`]s to the Leader
///   (one Raft log entry = one WAL Record batch);
/// - commit advances only after the entry is replicated to a majority; [`ReplicatedWal::take_durable`]
///   returns only Records **confirmed durable** — records before that may be lost under a majority
///   crash, and callers must not treat them as crash-safe.
pub struct ReplicatedWal<T: Transport> {
    node: Node<T>,
}

impl<T: Transport> ReplicatedWal<T> {
    /// Wrap a Raft node.
    pub fn new(node: Node<T>) -> Self {
        Self { node }
    }

    /// Propose a batch of records (Leader only; a non-Leader returns `Err(Error::SeriesFull)`
    /// as a backpressure signal — the caller should forward to the Leader or wait for an election).
    ///
    /// Returns the entry index; **durability is not guaranteed on return** — use
    /// [`ReplicatedWal::durable_index`] / [`ReplicatedWal::take_durable`]
    /// to track majority-acknowledgment progress.
    pub fn append_batch(&mut self, records: Vec<Record>) -> Result<u64> {
        self.node.propose(records)
    }

    /// Drive the protocol clock (heartbeats/election timeouts); time semantics are caller-defined (milliseconds recommended).
    pub fn tick(&mut self, now: u64) {
        self.node.tick(now);
    }

    /// Handle one received protocol message.
    pub fn handle(&mut self, from: NodeId, msg: Msg) {
        self.node.handle(from, msg);
    }

    /// Maximum log index confirmed durable (majority commit watermark).
    pub fn durable_index(&self) -> u64 {
        self.node.commit_index()
    }

    /// Take newly confirmed-durable records (batches flattened in log order).
    ///
    /// Membership-change entries (sentinel series = [`CONF_SERIES`]) are transparent to user data
    /// and filtered here, never appearing in the returned stream.
    pub fn take_durable(&mut self) -> Vec<Record> {
        let entries = self.node.take_committed();
        let n: usize = entries.iter().map(|e| e.records.len()).sum();
        let mut out = Vec::with_capacity(n);
        for e in entries {
            out.extend(e.records.into_iter().filter(|r| r.series != CONF_SERIES));
        }
        out
    }

    /// Whether this node is the Leader.
    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    /// Current term.
    pub fn term(&self) -> u64 {
        self.node.term()
    }

    /// Underlying node reference (diagnostics/tests).
    pub fn node(&self) -> &Node<T> {
        &self.node
    }

    /// Underlying node mutable reference (driving/tests).
    pub fn node_mut(&mut self) -> &mut Node<T> {
        &mut self.node
    }
}

/// Error constructor for non-Leader proposals (backpressure semantics, for callers to forward/retry).
pub(crate) fn not_leader_err() -> Error {
    Error::Corrupt("raft: not leader (redirect or wait for election)".into())
}
