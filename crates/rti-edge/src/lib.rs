//! rti-edge: wraps rti-db as the data-plane component of the RTI-Edge platform (v0.5, Wave 5).
//!
//! Three presets matching the RTI-L3 three-tier architecture ([`EdgeConfig`] constructors):
//!
//! | RTI-L3 tier | preset | composition |
//! |---|---|---|
//! | safety island (functional safety / hard real-time) | [`EdgeConfig::safety_island`] | Deterministic (pure in-memory, LRU, zero-allocation hot path) + UDP mirror |
//! | cognition (perception/fusion) | [`EdgeConfig::cognition`] | Balanced + 3-node in-memory Raft replica |
//! | planning (long-horizon analytics) | [`EdgeConfig::planning`] | Balanced + cold-tier archiving |
//!
//! Unified interface: [`EdgeNode::open`] → [`EdgeNode::ingest`] (optionally grid-aligned
//! via [`TsAligner`]) → [`EdgeNode::scan`] → [`EdgeNode::health`].
//!
//! Scope (honest disclosure):
//! - the Raft replica is a **single-process in-memory cluster simulation** ([`MemoryNetwork`] +
//!   [`ReplicatedWal`], driven by a virtual clock, deterministic): meant for demonstrating/
//!   validating data-plane integration semantics; cross-process deployment requires swapping the
//!   transport for `TcpTransport` and having the deployer drive the clock — this crate's [`RaftConfig`] does not cover that form yet;
//! - an [`EdgeNode`] with Raft attached is **not `Send`** because `MemoryTransport` contains
//!   an `Rc` (without Raft it behaves like a plain `Db`);
//! - `alloc_ok` only reports a real reading under the `alloc-count` feature (test-only global
//!   counting allocator); otherwise it is always `true` (see [`EdgeHealth`]).

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use rti_core::{Config, Error, Mirror, Profile, Result, Sample, SeriesId, SyncPolicy, Timestamp, TsAligner};
use rti_db::{Db, MirrorStats};
use rti_query::{Agg, Pred};
use rti_raft::{MemoryNetwork, MemoryTransport, Node, NodeId, ReplicatedWal, Role, Transport};
use rti_store::{ColdTier, LocalFsColdTier};
use rti_wal::Record;

// ------------------------------------------------------------ configuration

/// Raft replica configuration (in-memory cluster simulation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftConfig {
    /// Cluster node ids (single-process simulation: the `EdgeNode` creates all nodes internally).
    pub node_ids: Vec<NodeId>,
    /// Lower bound of the election timeout (logical milliseconds).
    pub election_min: u64,
    /// Randomized span of the election timeout.
    pub election_span: u64,
    /// Heartbeat interval (logical milliseconds).
    pub heartbeat: u64,
}

impl Default for RaftConfig {
    /// 3 nodes + the same deterministic timing parameters as the rti-raft tests.
    fn default() -> Self {
        Self { node_ids: vec![1, 2, 3], election_min: 150, election_span: 150, heartbeat: 40 }
    }
}

/// Cold-tier configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColdTierConfig {
    /// Local-directory cold tier (planning preset default; semantically equivalent to a single-bucket object store).
    LocalFs {
        /// Cold-tier root directory (created if missing).
        dir: PathBuf,
    },
}

impl ColdTierConfig {
    /// Instantiate the cold-tier handle.
    fn build(&self) -> Result<Arc<dyn ColdTier>> {
        match self {
            ColdTierConfig::LocalFs { dir } => Ok(Arc::new(LocalFsColdTier::new(dir)?)),
        }
    }
}

/// Edge node configuration.
#[derive(Clone, Debug)]
pub struct EdgeConfig {
    /// Runtime profile (Balanced / Deterministic).
    pub profile: Profile,
    /// Data directory (may be `None` under the Deterministic profile, which never touches the file system).
    pub data_dir: Option<PathBuf>,
    /// Maximum number of samples in the MemTable (sealed into a segment when reached).
    pub memtable_max: usize,
    /// Optional UDP mirror (best-effort, outside the hot-path latency budget).
    pub mirror: Option<Mirror>,
    /// Optional Raft replica (in-memory cluster simulation).
    pub raft: Option<RaftConfig>,
    /// Optional cold tiering.
    pub cold_tier: Option<ColdTierConfig>,
    /// Optional TSN grid alignment (nanoseconds); `ingest` timestamps are first floored to the grid.
    pub tsn_align_ns: Option<u64>,
}

impl EdgeConfig {
    /// Safety-island preset: Deterministic (pure in-memory, LRU eviction, zero-allocation hot path) + mirror.
    ///
    /// The mirror defaults to `127.0.0.1:7800` (UDP; no error if the peer does not exist);
    /// override the `mirror` field when deploying/testing.
    pub fn safety_island() -> Self {
        Self {
            profile: Profile::Deterministic,
            data_dir: None,
            memtable_max: 1 << 16,
            mirror: Some(Mirror::new("127.0.0.1:7800".parse().expect("literal address is valid"))),
            raft: None,
            cold_tier: None,
            tsn_align_ns: None,
        }
    }

    /// Cognition preset: Balanced + 3-node in-memory Raft replica.
    pub fn cognition() -> Self {
        Self {
            profile: Profile::Balanced,
            data_dir: Some(PathBuf::from("rti-edge-cognition")),
            memtable_max: 1 << 16,
            mirror: None,
            raft: Some(RaftConfig::default()),
            cold_tier: None,
            tsn_align_ns: None,
        }
    }

    /// Planning preset: Balanced + local-directory cold tiering.
    pub fn planning() -> Self {
        Self {
            profile: Profile::Balanced,
            data_dir: Some(PathBuf::from("rti-edge-planning")),
            memtable_max: 1 << 16,
            mirror: None,
            raft: None,
            cold_tier: Some(ColdTierConfig::LocalFs { dir: PathBuf::from("rti-edge-planning-cold") }),
            tsn_align_ns: None,
        }
    }
}

// ------------------------------------------------------------ health

/// Unified health view (shared by all three presets).
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeHealth {
    /// Runtime profile.
    pub profile: Profile,
    /// Mirror statistics (`None` when no mirror is configured).
    pub mirror_stats: Option<MirrorStats>,
    /// Raft role: `Some(Role::Leader)` for the cluster's current Leader node;
    /// `None` when Raft is disabled; during an election, the role of the tracked node.
    pub raft_role: Option<Role>,
    /// Current segment count (including archived-and-registered ones).
    pub segment_count: usize,
    /// Allocation health: under the `alloc-count` feature, 'no hot-path allocation-count
    /// growth since the last [`EdgeNode::reset_alloc_baseline`]'; always `true` without
    /// that feature (no counters to read — disclosed honestly).
    pub alloc_ok: bool,
}

// ------------------------------------------------------------ Raft simulation

/// Single-process in-memory Raft cluster (virtual clock, deterministic).
struct RaftSim {
    wals: Vec<ReplicatedWal<MemoryTransport>>,
    /// Index of the current Leader in `wals`.
    leader: usize,
    now: u64,
}

impl RaftSim {
    /// Pump one logical step: everyone ticks + pump messages until the queues are empty.
    fn step(&mut self) {
        self.now += 10;
        for w in &mut self.wals {
            w.tick(self.now);
        }
        loop {
            let mut progress = false;
            for w in &mut self.wals {
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

    fn unique_leader(&self) -> Option<usize> {
        let ls: Vec<usize> = self
            .wals
            .iter()
            .enumerate()
            .filter(|(_, w)| w.is_leader())
            .map(|(i, _)| i)
            .collect();
        if ls.len() == 1 {
            Some(ls[0])
        } else {
            None
        }
    }

    /// Elect a leader (bounded virtual-clock stepping; always converges on the in-memory network).
    fn elect(&mut self) -> Result<()> {
        for _ in 0..500 {
            self.step();
            if let Some(l) = self.unique_leader() {
                self.leader = l;
                return Ok(());
            }
        }
        Err(Error::Corrupt("rti-edge: raft election did not converge".into()))
    }

    /// Propose a batch of records and pump until a majority acknowledges them as durable.
    fn replicate(&mut self, records: Vec<Record>) -> Result<()> {
        let before = self.wals[self.leader].durable_index();
        // the Leader may have rotated: relocate it once on failure.
        if self.wals[self.leader].append_batch(records).is_err() {
            self.elect()?;
            return Err(Error::Corrupt("rti-edge: raft leader changed; retry ingest".into()));
        }
        for _ in 0..50 {
            self.step();
            if self.wals[self.leader].durable_index() > before {
                return Ok(());
            }
        }
        Err(Error::Corrupt("rti-edge: raft replication did not reach majority".into()))
    }
}

// ------------------------------------------------------------ node

/// RTI-Edge data-plane node: a unified wrapper over `Db` + optional mirror/Raft/cold tier/TSN alignment.
pub struct EdgeNode {
    db: Db,
    aligner: Option<TsAligner>,
    raft: Option<RaftSim>,
    profile: Profile,
    has_mirror: bool,
    #[cfg(feature = "alloc-count")]
    alloc_baseline: std::cell::Cell<u64>,
}

impl EdgeNode {
    /// Open a node from its config: build the `Db` → attach the cold tier → start the Raft cluster and elect a leader.
    pub fn open(config: EdgeConfig) -> Result<Self> {
        let aligner = match config.tsn_align_ns {
            None => None,
            Some(0) => return Err(Error::Corrupt("rti-edge: tsn_align_ns must be > 0".into())),
            Some(ns) => Some(
                TsAligner::new(ns.min(i64::MAX as u64) as i64)
                    .ok_or_else(|| Error::Corrupt("rti-edge: bad tsn_align_ns".into()))?,
            ),
        };

        let db = Db::open(Config {
            data_dir: config.data_dir.clone(),
            memtable_max: config.memtable_max,
            wal_sync: SyncPolicy::default(),
            pool_bytes: 1 << 20,
            profile: config.profile,
            mirror: config.mirror,
        })?;

        if let Some(ct) = &config.cold_tier {
            db.set_cold_tier(ct.build()?);
        }

        let raft = match &config.raft {
            None => None,
            Some(rc) => {
                if rc.node_ids.is_empty() {
                    return Err(Error::Corrupt("rti-edge: raft node_ids must not be empty".into()));
                }
                let net = MemoryNetwork::new();
                let wals: Vec<_> = rc
                    .node_ids
                    .iter()
                    .map(|&id| {
                        let peers: Vec<NodeId> =
                            rc.node_ids.iter().copied().filter(|&p| p != id).collect();
                        let node = Node::new(
                            id,
                            peers,
                            net.transport(id),
                            rc.election_min,
                            rc.election_span,
                            rc.heartbeat,
                            id.wrapping_mul(111),
                        );
                        ReplicatedWal::new(node)
                    })
                    .collect();
                let mut sim = RaftSim { wals, leader: 0, now: 0 };
                sim.elect()?;
                Some(sim)
            }
        };

        let node = Self {
            db,
            aligner,
            raft,
            profile: config.profile,
            has_mirror: config.mirror.is_some(),
            #[cfg(feature = "alloc-count")]
            alloc_baseline: std::cell::Cell::new(rti_db::alloc_count()),
        };
        Ok(node)
    }

    /// Write one sample: optional TSN grid alignment → local `Db::put` →
    /// optional Raft replication to majority durable.
    pub fn ingest(&mut self, series: SeriesId, mut sample: Sample) -> Result<()> {
        if let Some(a) = &self.aligner {
            sample.ts = a.align(sample.ts);
        }
        self.db.put(series, sample)?;
        if let Some(r) = &mut self.raft {
            r.replicate(vec![Record::new(series, sample.ts, sample.value)])?;
        }
        Ok(())
    }

    /// Scan (pass-through to `Db::scan`, read-your-writes).
    pub fn scan(
        &self,
        series: SeriesId,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<Pred>,
        agg: Option<Agg>,
    ) -> Result<Box<dyn Iterator<Item = Sample> + '_>> {
        self.db.scan(series, t0, t1, pred, agg)
    }

    /// Block until all enqueued records are persisted and visible (pass-through to `Db::flush`).
    pub fn flush(&self) -> Result<()> {
        self.db.flush()
    }

    /// Cold-tier archiving (pass-through to `Db::archive_older_than`; planning preset).
    pub fn archive_older_than(&self, ts: Timestamp) -> Result<usize> {
        self.db.archive_older_than(ts)
    }

    /// Archived segment count (pass-through to `Db::archived_segment_count`).
    pub fn archived_segment_count(&self) -> usize {
        self.db.archived_segment_count()
    }

    /// Maximum log index acknowledged durable by the Raft majority (`None` when Raft is disabled).
    pub fn raft_durable_index(&self) -> Option<u64> {
        self.raft.as_ref().map(|r| r.wals[r.leader].durable_index())
    }

    /// Reset the allocation-count baseline (only effective under the `alloc-count` feature:
    /// call after warmup; subsequent `health().alloc_ok` reflects steady state).
    pub fn reset_alloc_baseline(&self) {
        #[cfg(feature = "alloc-count")]
        self.alloc_baseline.set(rti_db::alloc_count());
    }

    /// Unified health view.
    pub fn health(&self) -> EdgeHealth {
        #[cfg(feature = "alloc-count")]
        let alloc_ok = rti_db::alloc_count() == self.alloc_baseline.get();
        #[cfg(not(feature = "alloc-count"))]
        let alloc_ok = true;
        EdgeHealth {
            profile: self.profile,
            mirror_stats: if self.has_mirror { Some(self.db.mirror_stats()) } else { None },
            raft_role: self.raft.as_ref().map(|r| {
                // prefer reporting the current real Leader; during an election gap, report the tracked node's role
                r.wals
                    .iter()
                    .find(|w| w.is_leader())
                    .map(|w| w.node().role())
                    .unwrap_or_else(|| r.wals[r.leader].node().role())
            }),
            segment_count: self.db.segment_count(),
            alloc_ok,
        }
    }

    /// Underlying `Db` reference (diagnostics / advanced usage).
    pub fn db(&self) -> &Db {
        &self.db
    }
}
