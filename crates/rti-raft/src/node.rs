//! Raft protocol state machine: leader election (PreVote) / heartbeats / log replication /
//! majority commit / snapshots & log compaction / single-step membership changes.
//!
//! Design highlights:
//! - **Logical clock injection**: [`Node::tick`] takes `now` from the caller (milliseconds recommended),
//!   so tests can use a virtual clock for full determinism;
//! - election-timeout 'randomization' uses a deterministic hash `hash(seed, term)` — same seed and
//!   term give the same result (reproducible in tests), while different nodes/terms stagger (liveness);
//! - **PreVote**: after a timeout, a node first runs a pre-vote with `term+1` without raising its term;
//!   it enters a real election only after winning a majority of pre-votes. Partitioned nodes therefore
//!   cannot inflate the cluster term (a receiver grants a pre-vote only when 'its own Leader lease has
//!   expired' (`>= election_min`) and the candidate is in the configuration with a non-lagging log);
//! - log indices are externally 1-based and **absolute**: after snapshot compaction, `log[0]` corresponds
//!   to absolute index `snap_index + 1`, and `prev_log_index == snap_index` means the snapshot point is the prefix;
//! - **snapshots**: `take_snapshot` discards the committed prefix and keeps the state bytes; when the Leader
//!   finds a follower's `next_index <= snap_index` (the needed prefix is compacted away), it sends an
//!   `InstallSnapshot` RPC instead of AppendEntries;
//! - **single-step membership changes**: only one node changes at a time; the new configuration is encoded
//!   as a log entry (a sentinel [`Record`] with series = `u32::MAX`, transparent to user data) and takes
//!   effect once **committed** by a majority; a removed node is demoted to non-voting member (it no longer
//!   starts elections and is excluded from voting and majority counting);
//! - the commit rule follows the Raft paper: the Leader only advances **current-term** entries by counting;
//!   older-term entries are committed indirectly as a consequence.

use std::collections::BTreeMap;

use rti_core::{Error, Result};
use rti_wal::Record;

/// Cluster node identifier.
pub type NodeId = u64;

/// Sentinel series id for membership-change log entries (reserved; user data must not use it).
///
/// Simplifying trade-off (honest disclosure): a single-server change encodes the new configuration as a
/// sentinel [`Record`] inside an ordinary log entry, reusing the existing replication/codec paths with zero
/// wire-format changes; the cost is reserving `u32::MAX` from the user series namespace.
pub const CONF_SERIES: u32 = u32::MAX;

/// Node role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Follower.
    Follower,
    /// Pre-vote candidate (term not yet raised).
    PreCandidate,
    /// Candidate (real election, term already +1).
    Candidate,
    /// Leader.
    Leader,
}

/// One replicated log entry = one WAL Record batch + the term at write time.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// Term when the entry was appended by the Leader.
    pub term: u64,
    /// WAL records of this batch (membership-change entries contain a [`CONF_SERIES`] sentinel record).
    pub records: Vec<Record>,
}

/// Snapshot: a replacement for the compacted log prefix + upper-layer state-machine bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// Maximum log index covered by the snapshot (inclusive).
    pub last_included_index: u64,
    /// Term of the entry at that index.
    pub last_included_term: u64,
    /// Upper-layer state-machine bytes (semantics caller-defined; forwarded opaquely by the protocol layer).
    pub state: Vec<u8>,
}

/// Single-step membership-change operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfChange {
    /// Add a voting member.
    AddPeer(NodeId),
    /// Remove a voting member (the removed one is demoted to a non-voting member).
    RemovePeer(NodeId),
}

/// Encode a membership change as a sentinel record (`value` bit pattern: 1=Add, 2=Remove).
pub(crate) fn encode_conf(cc: ConfChange) -> Record {
    let (id, tag) = match cc {
        ConfChange::AddPeer(id) => (id, 1u64),
        ConfChange::RemovePeer(id) => (id, 2u64),
    };
    Record::new(CONF_SERIES, id as i64, f64::from_bits(tag))
}

/// Decode a membership change from a record; non-sentinel records return `None`.
pub(crate) fn decode_conf(r: &Record) -> Option<ConfChange> {
    if r.series != CONF_SERIES {
        return None;
    }
    let id = r.sample.ts as u64;
    match r.sample.value.to_bits() {
        1 => Some(ConfChange::AddPeer(id)),
        2 => Some(ConfChange::RemovePeer(id)),
        _ => None,
    }
}

/// Protocol message.
#[derive(Clone, Debug, PartialEq)]
pub enum Msg {
    /// Pre-vote request (`term` is the term the candidate *will* use = its current term + 1;
    /// the receiver does not raise its own term because of it).
    PreVote {
        /// Candidate's prospective term.
        term: u64,
        /// Candidate id.
        candidate: NodeId,
        /// Candidate's last log index.
        last_log_index: u64,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },
    /// Pre-vote response.
    PreVoteResponse {
        /// Voter's term.
        term: u64,
        /// Whether the vote is granted.
        granted: bool,
    },
    /// Election request.
    RequestVote {
        /// Candidate's term.
        term: u64,
        /// Candidate id.
        candidate: NodeId,
        /// Candidate's last log index.
        last_log_index: u64,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },
    /// Election response.
    VoteResponse {
        /// Voter's term.
        term: u64,
        /// Whether the vote is granted.
        granted: bool,
    },
    /// Log replication / heartbeat.
    AppendEntries {
        /// Leader's term.
        term: u64,
        /// Leader id.
        leader: NodeId,
        /// Index of the entry just before the new ones (0 = log head; `snap_index` = snapshot point).
        prev_log_index: u64,
        /// Term of the previous entry.
        prev_log_term: u64,
        /// New entries (empty = heartbeat).
        entries: Vec<Entry>,
        /// Leader's commit watermark.
        leader_commit: u64,
    },
    /// Replication response (also used as the InstallSnapshot response, with `match_index` =
    /// the snapshot's `last_included_index`).
    AppendResponse {
        /// Responder's term.
        term: u64,
        /// Whether replication succeeded.
        success: bool,
        /// Maximum index confirmed replicated (for the Leader to advance commit).
        match_index: u64,
    },
    /// Snapshot installation: replaces AppendEntries when a follower lags beyond the Leader's log start.
    InstallSnapshot {
        /// Leader's term.
        term: u64,
        /// Leader id.
        leader: NodeId,
        /// The snapshot itself.
        snapshot: Snapshot,
    },
}

/// Transport abstraction: messages may be lost/delayed/reordered (Raft tolerates all),
/// so `send` returns no error — a failure is just a dropped packet, retried by the protocol layer.
pub trait Transport {
    /// Send one message to `to` (best-effort).
    fn send(&mut self, to: NodeId, msg: Msg);
    /// Non-blocking receive of one message.
    fn recv(&mut self) -> Option<(NodeId, Msg)>;
}

/// Single-shard Raft node.
pub struct Node<T: Transport> {
    id: NodeId,
    /// Other voting members (excluding self).
    peers: Vec<NodeId>,
    /// Whether self is a voting member (false after being removed by remove_peer:
    /// does not start elections, does not vote, is not counted in majorities, but still follows the log).
    voter: bool,
    transport: T,
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<Entry>,
    /// Snapshot compaction point: absolute index of `log[i]` = `snap_index + 1 + i`.
    snap_index: u64,
    /// Term of the entry at the snapshot point.
    snap_term: u64,
    /// Most recent snapshot (kept because the Leader needs the state bytes for InstallSnapshot).
    snapshot: Option<Snapshot>,
    commit_index: u64,
    /// Watermark already delivered upward (take_committed).
    applied_up_to: u64,
    /// candidate: votes received (including self-vote).
    votes_received: usize,
    /// pre-candidate: pre-votes received (including self-vote).
    pre_votes: usize,
    /// leader: whether there is an uncommitted current-term membership change (one at a time).
    pending_conf: bool,
    /// leader: replication progress of each follower.
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,
    leader_id: Option<NodeId>,
    now: u64,
    /// Last 'election timer reset' (received a legitimate heartbeat/vote, or started an election).
    last_progress: u64,
    last_heartbeat_sent: u64,
    election_min: u64,
    election_span: u64,
    heartbeat_interval: u64,
    /// Pre-campaign round number (mixed into the timeout hash, see [`Node::election_timeout`]).
    campaign: u64,
    seed: u64,
}

impl<T: Transport> Node<T> {
    /// Create a node (initially a Follower, term 0, voting member).
    ///
    /// - `peers`: the other node ids (excluding self); cluster = peers ∪ {id};
    /// - `election_timeout ∈ [min, min+span)` derived from `hash(seed, term)`;
    /// - `heartbeat_interval` must be significantly smaller than `min` (recommended <= min/3).
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        transport: T,
        election_min: u64,
        election_span: u64,
        heartbeat_interval: u64,
        seed: u64,
    ) -> Self {
        Self {
            id,
            peers,
            voter: true,
            transport,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            snap_index: 0,
            snap_term: 0,
            snapshot: None,
            commit_index: 0,
            applied_up_to: 0,
            votes_received: 0,
            pre_votes: 0,
            pending_conf: false,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            leader_id: None,
            now: 0,
            last_progress: 0,
            last_heartbeat_sent: 0,
            election_min: election_min.max(1),
            election_span: election_span.max(1),
            heartbeat_interval,
            campaign: 0,
            seed,
        }
    }

    // ------------------------------------------------------------ observation

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Current role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Whether this node is the Leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Current term.
    pub fn term(&self) -> u64 {
        self.current_term
    }

    /// Known Leader (`None` when unknown).
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Majority commit watermark.
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Maximum log index (absolute; including the snapshot-point prefix).
    pub fn log_len(&self) -> u64 {
        self.last_log_index()
    }

    /// Log contents (uncompacted suffix; for tests/diagnostics).
    pub fn log_entries(&self) -> &[Entry] {
        &self.log
    }

    /// Snapshot compaction point: entries with absolute index <= this value have been discarded (0 = no snapshot).
    pub fn compacted_index(&self) -> u64 {
        self.snap_index
    }

    /// Most recent snapshot (if any).
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Currently effective configuration (full set of voting members, including self if in the configuration; ascending).
    pub fn membership(&self) -> Vec<NodeId> {
        let mut m = self.peers.clone();
        if self.voter {
            m.push(self.id);
        }
        m.sort_unstable();
        m
    }

    /// Whether self is a voting member.
    pub fn is_voter(&self) -> bool {
        self.voter
    }

    /// Transport-layer reference (drives the message pump).
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    // ------------------------------------------------------------ clock

    /// Election timeout for this campaign round (deterministically 'random').
    ///
    /// The hash mixes `(seed, current_term, campaign)`: pre-votes do not raise the term,
    /// so if the timeout depended only on the term, nodes timing out at the same moment would
    /// retry in lockstep every round and reject each other's pre-votes — a livelock; the campaign
    /// round staggers each retry's backoff, and results stay constant for the same event sequence (determinism unaffected).
    fn election_timeout(&self) -> u64 {
        // SplitMix64 finalizer.
        let mut z = self
            .seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(self.current_term.wrapping_mul(0xC2B2_AE3D_27D4_EB4F))
            .wrapping_add(self.campaign.wrapping_mul(0x1656_67B1_9E37_79F9));
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        self.election_min + z % self.election_span
    }

    /// Advance the logical clock: the Leader sends heartbeats when due; voting-member
    /// Followers/PreCandidates/Candidates start (pre-)elections when due.
    pub fn tick(&mut self, now: u64) {
        self.now = now;
        match self.role {
            Role::Leader => {
                if now.saturating_sub(self.last_heartbeat_sent) >= self.heartbeat_interval {
                    self.broadcast_append();
                    self.last_heartbeat_sent = now;
                }
            }
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                if !self.voter {
                    return; // non-voting members never start an election
                }
                if now.saturating_sub(self.last_progress) >= self.election_timeout() {
                    self.start_pre_vote();
                }
            }
        }
    }

    // ------------------------------------------------------------ upper-layer interface

    /// Propose a batch of WAL records (Leader only). Returns the entry index (1-based, absolute).
    pub fn propose(&mut self, records: Vec<Record>) -> Result<u64> {
        if !self.is_leader() {
            return Err(crate::not_leader_err());
        }
        self.log.push(Entry { term: self.current_term, records });
        let idx = self.last_log_index();
        self.match_index.insert(self.id, idx);
        self.broadcast_append();
        Ok(idx)
    }

    /// Propose a single-step membership change (Leader only, and only with no previous uncommitted change).
    ///
    /// The new configuration is encoded as a log entry and takes effect locally only after **majority
    /// commit**; until then, majorities are still computed per the old configuration — the safety basis of one-at-a-time changes.
    pub fn change_membership(&mut self, cc: ConfChange) -> Result<u64> {
        if !self.is_leader() {
            return Err(crate::not_leader_err());
        }
        if self.pending_conf {
            return Err(Error::Corrupt("raft: another membership change is still uncommitted".into()));
        }
        match cc {
            ConfChange::AddPeer(id) => {
                if id == self.id || self.peers.contains(&id) {
                    return Err(Error::Corrupt(format!("raft: peer {id} already in membership")));
                }
            }
            ConfChange::RemovePeer(id) => {
                if id != self.id && !self.peers.contains(&id) {
                    return Err(Error::Corrupt(format!("raft: peer {id} not in membership")));
                }
            }
        }
        self.pending_conf = true;
        self.propose(vec![encode_conf(cc)])
    }

    /// Add a voting member (convenience wrapper for [`ConfChange::AddPeer`]).
    pub fn add_peer(&mut self, id: NodeId) -> Result<u64> {
        self.change_membership(ConfChange::AddPeer(id))
    }

    /// Remove a voting member (convenience wrapper for [`ConfChange::RemovePeer`]).
    /// Removing self is allowed (the Leader steps down once the change is committed).
    pub fn remove_peer(&mut self, id: NodeId) -> Result<u64> {
        self.change_membership(ConfChange::RemovePeer(id))
    }

    /// Take committed entries in `(applied, commit_index]` (majority-confirmed durable).
    ///
    /// Membership-change entries are also returned as-is (containing a [`CONF_SERIES`] sentinel record);
    /// filtering is up to the upper layer (e.g. `ReplicatedWal`).
    pub fn take_committed(&mut self) -> Vec<Entry> {
        let hi = self.commit_index.min(self.last_log_index());
        let lo = self.applied_up_to.max(self.snap_index);
        if hi <= lo {
            return Vec::new();
        }
        let base = self.snap_index;
        let out: Vec<Entry> = self.log[(lo - base) as usize..(hi - base) as usize].to_vec();
        self.applied_up_to = hi;
        out
    }

    // ------------------------------------------------------------ snapshots

    /// Take a snapshot and compact the log: discard the committed prefix up to and including `compact_upto`.
    ///
    /// - requires `snap_index < compact_upto <= commit_index` (only committed, not-yet-compacted entries
    ///   may be compacted), otherwise returns `Err`;
    /// - `state` holds upper-layer state-machine bytes, saved/forwarded opaquely by the protocol layer;
    /// - the returned [`Snapshot`] is also retained inside the node so the Leader can send
    ///   `InstallSnapshot` to lagging followers.
    pub fn take_snapshot(&mut self, compact_upto: u64, state: Vec<u8>) -> Result<Snapshot> {
        if compact_upto <= self.snap_index || compact_upto > self.commit_index {
            return Err(Error::Corrupt(format!(
                "raft: bad snapshot point {compact_upto} (compacted={}, commit={})",
                self.snap_index, self.commit_index
            )));
        }
        let term = self.term_at(compact_upto).expect("compact_upto is within the log range");
        let snap = Snapshot { last_included_index: compact_upto, last_included_term: term, state };
        // discard the prefix
        let drop = (compact_upto - self.snap_index) as usize;
        self.log.drain(..drop);
        self.snap_index = compact_upto;
        self.snap_term = term;
        self.snapshot = Some(snap.clone());
        Ok(snap)
    }

    /// Install a snapshot locally (e.g. restart recovery; equivalent to receiving an InstallSnapshot
    /// but without term/role arbitration).
    ///
    /// No-op when the snapshot point is <= the current compaction point. After installation the log keeps
    /// only the suffix after the snapshot point with continuous terms (the whole log is discarded on a term
    /// mismatch at the snapshot point); `commit_index` / `applied` watermarks are raised to at least the
    /// snapshot point — skipped entries are considered included in the snapshot state and are no longer delivered via `take_committed`.
    pub fn install_snapshot(&mut self, snap: Snapshot) -> Result<()> {
        if snap.last_included_index <= self.snap_index {
            return Ok(()); // stale snapshot
        }
        self.apply_snapshot(snap);
        Ok(())
    }

    /// Common part of snapshot installation.
    fn apply_snapshot(&mut self, snap: Snapshot) {
        let idx = snap.last_included_index;
        let keep_from = if self.term_at(idx) == Some(snap.last_included_term) {
            // the snapshot point is exactly in the local log with a matching term: keep the suffix after it
            (idx - self.snap_index) as usize
        } else {
            // the snapshot point is beyond the local log or terms conflict: discard the whole log
            self.log.len()
        };
        self.log.drain(..keep_from.min(self.log.len()));
        self.snap_index = idx;
        self.snap_term = snap.last_included_term;
        self.commit_index = self.commit_index.max(idx);
        self.applied_up_to = self.applied_up_to.max(idx);
        self.snapshot = Some(snap);
    }

    // ------------------------------------------------------------ message handling

    /// Handle one received protocol message.
    pub fn handle(&mut self, from: NodeId, msg: Msg) {
        match msg {
            Msg::PreVote { term, candidate, last_log_index, last_log_term } => {
                self.handle_pre_vote(candidate, term, last_log_index, last_log_term);
            }
            Msg::PreVoteResponse { term, granted } => {
                self.handle_pre_vote_response(from, term, granted);
            }
            Msg::RequestVote { term, candidate, last_log_index, last_log_term } => {
                self.handle_request_vote(candidate, term, last_log_index, last_log_term);
            }
            Msg::VoteResponse { term, granted } => {
                self.handle_vote_response(from, term, granted);
            }
            Msg::AppendEntries { term, leader, prev_log_index, prev_log_term, entries, leader_commit } => {
                self.handle_append_entries(leader, term, prev_log_index, prev_log_term, entries, leader_commit);
            }
            Msg::AppendResponse { term, success, match_index } => {
                self.handle_append_response(from, term, success, match_index);
            }
            Msg::InstallSnapshot { term, leader, snapshot } => {
                self.handle_install_snapshot(leader, term, snapshot);
            }
        }
    }

    /// Seeing a higher term: step back to Follower and update the term.
    fn step_down_if_stale(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.role = Role::Follower;
            self.leader_id = None;
            self.pending_conf = false;
            self.last_progress = self.now;
        }
    }

    /// Log 'freshness' comparison: whether the candidate's (last_log_term, last_log_index)
    /// is not behind the local one.
    fn log_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        let my_last_term = self.last_log_term();
        last_log_term > my_last_term
            || (last_log_term == my_last_term && last_log_index >= self.last_log_index())
    }

    /// Pre-vote receiver logic: no term raise, no role change. Grants the vote only when
    /// 'the candidate is a voting member + prospective term is higher + log not behind + own
    /// Leader lease has expired (>= election_min)'.
    fn handle_pre_vote(&mut self, candidate: NodeId, term: u64, last_log_index: u64, last_log_term: u64) {
        let lease_expired = self.now.saturating_sub(self.last_progress) >= self.election_min;
        let granted = self.voter
            && self.peers.contains(&candidate)
            && term > self.current_term
            && self.log_up_to_date(last_log_index, last_log_term)
            && lease_expired;
        self.transport.send(candidate, Msg::PreVoteResponse { term: self.current_term, granted });
    }

    fn handle_pre_vote_response(&mut self, from: NodeId, term: u64, granted: bool) {
        self.step_down_if_stale(term);
        if self.role != Role::PreCandidate || !granted || !self.peers.contains(&from) {
            return;
        }
        self.pre_votes += 1;
        if self.pre_votes >= self.majority() {
            self.start_election();
        }
    }

    fn handle_request_vote(&mut self, candidate: NodeId, term: u64, last_log_index: u64, last_log_term: u64) {
        self.step_down_if_stale(term);
        let granted = term >= self.current_term
            && self.voter
            && self.peers.contains(&candidate)
            && self.log_up_to_date(last_log_index, last_log_term)
            && (self.voted_for.is_none() || self.voted_for == Some(candidate));
        if granted {
            self.voted_for = Some(candidate);
            self.last_progress = self.now;
        }
        self.transport.send(candidate, Msg::VoteResponse { term: self.current_term, granted });
    }

    fn handle_vote_response(&mut self, from: NodeId, term: u64, granted: bool) {
        self.step_down_if_stale(term);
        if self.role != Role::Candidate || term != self.current_term || !granted || !self.peers.contains(&from) {
            return;
        }
        self.votes_received += 1;
        if self.votes_received >= self.majority() {
            self.become_leader();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        leader: NodeId,
        term: u64,
        mut prev_log_index: u64,
        mut prev_log_term: u64,
        mut entries: Vec<Entry>,
        leader_commit: u64,
    ) {
        self.step_down_if_stale(term);
        if term < self.current_term {
            self.transport.send(leader, Msg::AppendResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            });
            return;
        }
        self.role = Role::Follower;
        self.leader_id = Some(leader);
        self.last_progress = self.now;

        // the prefix has been compacted by the local snapshot: skip covered entries and align at the snapshot point.
        if prev_log_index < self.snap_index {
            let skip = (self.snap_index - prev_log_index) as usize;
            if entries.len() <= skip {
                // all entries are covered by the snapshot: only commit needs advancing
                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(self.last_log_index());
                }
                self.transport.send(leader, Msg::AppendResponse {
                    term: self.current_term,
                    success: true,
                    match_index: self.snap_index,
                });
                return;
            }
            entries.drain(..skip);
            prev_log_index = self.snap_index;
            prev_log_term = self.snap_term;
        }

        // prev consistency check
        let prev_ok = if prev_log_index == 0 {
            true
        } else {
            self.term_at(prev_log_index) == Some(prev_log_term)
        };
        if !prev_ok {
            self.transport.send(leader, Msg::AppendResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
            return;
        }
        // conflict truncation + append (aligned entry by entry; entries before the snapshot point are skipped)
        let mut idx = prev_log_index;
        for e in entries {
            idx += 1;
            if idx <= self.snap_index {
                continue; // already covered by the snapshot
            }
            let slot = (idx - self.snap_index - 1) as usize;
            if let Some(existing) = self.log.get(slot) {
                if existing.term != e.term {
                    self.log.truncate(slot);
                    self.log.push(e);
                }
            } else {
                self.log.push(e);
            }
        }
        // advance the local commit (not beyond the local last index) and apply newly committed configurations
        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(self.last_log_index());
            self.apply_committed_confs(self.commit_index + 1, new_commit);
            self.commit_index = new_commit;
        }
        self.transport.send(leader, Msg::AppendResponse {
            term: self.current_term,
            success: true,
            match_index: idx,
        });
    }

    /// Install a snapshot from the Leader (follower lagging beyond the Leader's log start).
    fn handle_install_snapshot(&mut self, leader: NodeId, term: u64, snapshot: Snapshot) {
        self.step_down_if_stale(term);
        if term < self.current_term {
            self.transport.send(leader, Msg::AppendResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            });
            return;
        }
        self.role = Role::Follower;
        self.leader_id = Some(leader);
        self.last_progress = self.now;
        let idx = snapshot.last_included_index;
        if idx > self.snap_index {
            self.apply_snapshot(snapshot);
        }
        self.transport.send(leader, Msg::AppendResponse {
            term: self.current_term,
            success: true,
            match_index: idx,
        });
    }

    fn handle_append_response(&mut self, from: NodeId, term: u64, success: bool, match_index: u64) {
        self.step_down_if_stale(term);
        if self.role != Role::Leader || term != self.current_term || !self.peers.contains(&from) {
            return;
        }
        if success {
            let m = match_index.min(self.last_log_index());
            self.match_index.insert(from, m);
            self.next_index.insert(from, m + 1);
            self.advance_commit();
        } else {
            // step back one slot and resend immediately (once stepped below the snapshot point,
            // send_append automatically switches to InstallSnapshot)
            let next = self.next_index.get(&from).copied().unwrap_or(self.last_log_index() + 1);
            let next = next.saturating_sub(1).max(1);
            self.next_index.insert(from, next);
            self.send_append(from);
        }
    }

    // ------------------------------------------------------------ internals

    /// Term of the entry at absolute index `idx`; `idx == 0` (empty log head) returns
    /// `Some(0)`; an already-compacted prefix (`idx < snap_index`) returns `None`.
    fn term_at(&self, idx: u64) -> Option<u64> {
        if idx == 0 {
            return Some(0);
        }
        if idx == self.snap_index {
            return Some(self.snap_term);
        }
        if idx < self.snap_index {
            return None; // compacted away; unknowable
        }
        self.log.get((idx - self.snap_index - 1) as usize).map(|e| e.term)
    }

    fn last_log_index(&self) -> u64 {
        self.snap_index + self.log.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.snap_term)
    }

    /// Majority size of the current configuration.
    fn majority(&self) -> usize {
        let cluster = self.peers.len() + usize::from(self.voter);
        cluster / 2 + 1
    }

    /// First step after an election timeout: pre-vote (does not raise the term).
    fn start_pre_vote(&mut self) {
        self.campaign += 1; // a new backoff round (see election_timeout)
        self.role = Role::PreCandidate;
        self.pre_votes = 1; // self-vote
        self.votes_received = 0;
        self.leader_id = None;
        self.last_progress = self.now;
        let msg = Msg::PreVote {
            term: self.current_term + 1,
            candidate: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        let peers = self.peers.clone();
        for p in peers {
            self.transport.send(p, msg.clone());
        }
        if self.majority() == 1 {
            self.start_election();
        }
    }

    /// Enter the real election (term +1) after the pre-vote wins a majority.
    fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received = 1;
        self.leader_id = None;
        self.last_progress = self.now;
        let msg = Msg::RequestVote {
            term: self.current_term,
            candidate: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        let peers = self.peers.clone();
        for p in peers {
            self.transport.send(p, msg.clone());
        }
        if self.majority() == 1 {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.pending_conf = false;
        let next = self.last_log_index() + 1;
        for &p in &self.peers {
            self.next_index.insert(p, next);
            self.match_index.insert(p, 0);
        }
        self.match_index.insert(self.id, self.last_log_index());
        self.last_heartbeat_sent = self.now;
        self.broadcast_append();
    }

    /// Send AppendEntries to a follower per its next_index; when the needed prefix has been
    /// compacted (`next <= snap_index`), send InstallSnapshot instead.
    fn send_append(&mut self, to: NodeId) {
        let next = self.next_index.get(&to).copied().unwrap_or(self.last_log_index() + 1);
        if next <= self.snap_index {
            // the follower lags beyond the log start: use InstallSnapshot instead of AppendEntries.
            // snapshot is retained since take_snapshot/install_snapshot, so it must be Some.
            if let Some(snap) = self.snapshot.clone() {
                self.transport.send(to, Msg::InstallSnapshot {
                    term: self.current_term,
                    leader: self.id,
                    snapshot: snap,
                });
            }
            return;
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index).expect("prev is after the snapshot point, so it must be queryable");
        let first = (next - self.snap_index - 1) as usize;
        let entries: Vec<Entry> = self.log[first.min(self.log.len())..].to_vec();
        self.transport.send(to, Msg::AppendEntries {
            term: self.current_term,
            leader: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        });
    }

    fn broadcast_append(&mut self) {
        let peers = self.peers.clone();
        for p in peers {
            self.send_append(p);
        }
    }

    /// The Leader advances commit by majority match_index (only current-term entries can be advanced
    /// directly; older-term entries commit indirectly; only voting members in the current configuration are counted).
    /// Membership changes are applied immediately once their entries commit (single-step protocol: the
    /// change entry itself is still counted under the old configuration).
    fn advance_commit(&mut self) {
        let mut newly = self.commit_index;
        for n in (self.commit_index + 1)..=self.last_log_index() {
            if self.term_at(n) != Some(self.current_term) {
                continue;
            }
            let mut replicated = usize::from(self.match_index.get(&self.id).copied().unwrap_or(0) >= n);
            replicated += self
                .peers
                .iter()
                .filter(|p| self.match_index.get(p).copied().unwrap_or(0) >= n)
                .count();
            if replicated >= self.majority() {
                newly = n;
            }
        }
        if newly > self.commit_index {
            self.apply_committed_confs(self.commit_index + 1, newly);
            self.commit_index = newly;
        }
    }

    /// Apply membership changes from newly committed entries in `(from, to]` (both ends inclusive,
    /// absolute indices). Scans entry by entry; user data outside sentinel records is unaffected.
    fn apply_committed_confs(&mut self, from: u64, to: u64) {
        let mut ccs = Vec::new();
        for idx in from..=to {
            if idx <= self.snap_index {
                continue;
            }
            if let Some(e) = self.log.get((idx - self.snap_index - 1) as usize) {
                ccs.extend(e.records.iter().filter_map(decode_conf));
            }
        }
        for cc in ccs {
            self.apply_conf(cc);
        }
    }

    /// Apply one committed single-step membership change.
    fn apply_conf(&mut self, cc: ConfChange) {
        self.pending_conf = false;
        match cc {
            ConfChange::AddPeer(id) => {
                if id == self.id {
                    self.voter = true;
                } else if !self.peers.contains(&id) {
                    self.peers.push(id);
                    if self.role == Role::Leader {
                        self.next_index.insert(id, self.last_log_index() + 1);
                        self.match_index.insert(id, 0);
                    }
                }
            }
            ConfChange::RemovePeer(id) => {
                if id == self.id {
                    // removed from the configuration: step down and demote to non-voting member
                    self.voter = false;
                    self.role = Role::Follower;
                    self.leader_id = None;
                    self.voted_for = None;
                } else {
                    self.peers.retain(|&p| p != id);
                    self.next_index.remove(&id);
                    self.match_index.remove(&id);
                }
            }
        }
    }
}
