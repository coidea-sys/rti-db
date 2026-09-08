//! rti-edge：把 rti-db 包装为 RTI-Edge 平台的数据面组件（v0.5，Wave 5）。
//!
//! 对应 RTI-L3 三层架构的三档预设（[`EdgeConfig`] 构造器）：
//!
//! | RTI-L3 层 | 预设 | 组成 |
//! |---|---|---|
//! | 安全岛（功能安全/硬实时） | [`EdgeConfig::safety_island`] | Deterministic（纯内存、LRU、零分配热路径）+ UDP 镜像 |
//! | 认知层（感知/融合） | [`EdgeConfig::cognition`] | Balanced + 3 节点内存 Raft 副本 |
//! | 规划层（长时程分析） | [`EdgeConfig::planning`] | Balanced + 冷分层归档 |
//!
//! 统一接口：[`EdgeNode::open`] → [`EdgeNode::ingest`]（可选经
//! [`TsAligner`] 网格对齐）→ [`EdgeNode::scan`] → [`EdgeNode::health`]。
//!
//! 范围（诚实声明）：
//! - Raft 副本为**单进程内存集群仿真**（[`MemoryNetwork`] +
//!   [`ReplicatedWal`]，虚拟时钟驱动，确定性）：演示/验证数据面
//!   集成语义用；跨进程部署需把 transport 换成 `TcpTransport` 并
//!   由部署方驱动时钟，本 crate 的 [`RaftConfig`] 暂不含该形态；
//! - 挂接 Raft 的 [`EdgeNode`] 因 `MemoryTransport` 内含 `Rc` 而
//!   **不是 `Send`**（不挂 Raft 时与普通 `Db` 相同）；
//! - `alloc_ok` 仅在 feature `alloc-count`（测试用全局计数分配器）
//!   下给出真实读数，否则恒 `true`（见 [`EdgeHealth`]）。

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use rti_core::{Config, Error, Mirror, Profile, Result, Sample, SeriesId, SyncPolicy, Timestamp, TsAligner};
use rti_db::{Db, MirrorStats};
use rti_query::{Agg, Pred};
use rti_raft::{MemoryNetwork, MemoryTransport, Node, NodeId, ReplicatedWal, Role, Transport};
use rti_store::{ColdTier, LocalFsColdTier};
use rti_wal::Record;

// ------------------------------------------------------------ 配置

/// Raft 副本配置（内存集群仿真）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftConfig {
    /// 集群节点 id（单进程仿真：`EdgeNode` 内部创建全部节点）。
    pub node_ids: Vec<NodeId>,
    /// 选举超时下界（逻辑毫秒）。
    pub election_min: u64,
    /// 选举超时随机 span。
    pub election_span: u64,
    /// 心跳间隔（逻辑毫秒）。
    pub heartbeat: u64,
}

impl Default for RaftConfig {
    /// 3 节点 + 与 rti-raft 测试一致的确定性时序参数。
    fn default() -> Self {
        Self { node_ids: vec![1, 2, 3], election_min: 150, election_span: 150, heartbeat: 40 }
    }
}

/// 冷分层配置。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColdTierConfig {
    /// 本地目录冷层（planning 档默认；语义等价单桶对象存储）。
    LocalFs {
        /// 冷层根目录（不存在则创建）。
        dir: PathBuf,
    },
}

impl ColdTierConfig {
    /// 实例化冷层句柄。
    fn build(&self) -> Result<Arc<dyn ColdTier>> {
        match self {
            ColdTierConfig::LocalFs { dir } => Ok(Arc::new(LocalFsColdTier::new(dir)?)),
        }
    }
}

/// 边缘节点配置。
#[derive(Clone, Debug)]
pub struct EdgeConfig {
    /// 运行配置档（Balanced / Deterministic）。
    pub profile: Profile,
    /// 数据目录（Deterministic 档可为 `None` 且绝不触碰文件系统）。
    pub data_dir: Option<PathBuf>,
    /// MemTable 最大采样点数（达到后 seal 为 segment）。
    pub memtable_max: usize,
    /// 可选 UDP 镜像（best-effort，不进热路径延迟预算）。
    pub mirror: Option<Mirror>,
    /// 可选 Raft 副本（内存集群仿真）。
    pub raft: Option<RaftConfig>,
    /// 可选冷分层。
    pub cold_tier: Option<ColdTierConfig>,
    /// 可选 TSN 网格对齐（纳秒）；`ingest` 的时间戳先向下取整到网格。
    pub tsn_align_ns: Option<u64>,
}

impl EdgeConfig {
    /// 安全岛档：Deterministic（纯内存、LRU 丢弃、零分配热路径）+ 镜像。
    ///
    /// 镜像默认指向 `127.0.0.1:7800`（UDP，对端不存在也不报错），
    /// 部署/测试时覆写 `mirror` 字段即可。
    pub fn safety_island() -> Self {
        Self {
            profile: Profile::Deterministic,
            data_dir: None,
            memtable_max: 1 << 16,
            mirror: Some(Mirror::new("127.0.0.1:7800".parse().expect("字面量地址合法"))),
            raft: None,
            cold_tier: None,
            tsn_align_ns: None,
        }
    }

    /// 认知层档：Balanced + 3 节点内存 Raft 副本。
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

    /// 规划层档：Balanced + 本地目录冷分层。
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

// ------------------------------------------------------------ 健康

/// 统一健康视图（三档预设共用）。
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeHealth {
    /// 运行配置档。
    pub profile: Profile,
    /// 镜像统计（未配置镜像为 `None`）。
    pub mirror_stats: Option<MirrorStats>,
    /// Raft 角色：集群当前 Leader 节点为 `Some(Role::Leader)`；
    /// 未启用 Raft 为 `None`；选主进行中为被跟踪节点的角色。
    pub raft_role: Option<Role>,
    /// 当前 segment 数（含已归档注册的）。
    pub segment_count: usize,
    /// 分配健康：feature `alloc-count` 下为「自上次
    /// [`EdgeNode::reset_alloc_baseline`] 以来热路径分配计数无增长」；
    /// 未启用该 feature 时恒 `true`（无计数可读，如实声明）。
    pub alloc_ok: bool,
}

// ------------------------------------------------------------ Raft 仿真

/// 单进程内存 Raft 集群（虚拟时钟，确定性）。
struct RaftSim {
    wals: Vec<ReplicatedWal<MemoryTransport>>,
    /// 当前 Leader 在 `wals` 中的下标。
    leader: usize,
    now: u64,
}

impl RaftSim {
    /// 泵一个逻辑步：全员 tick + 消息泵到队列清空。
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

    /// 选主（虚拟时钟有界步进；内存网络下必然收敛）。
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

    /// 提议一批记录并泵到多数派确认 durable。
    fn replicate(&mut self, records: Vec<Record>) -> Result<()> {
        let before = self.wals[self.leader].durable_index();
        // Leader 可能已轮换：失败时重新定位一次。
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

// ------------------------------------------------------------ 节点

/// RTI-Edge 数据面节点：`Db` + 可选镜像/Raft/冷层/TSN 对齐的统一封装。
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
    /// 按配置打开节点：建 `Db` → 挂冷层 → 起 Raft 集群并选主。
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

    /// 写入一个采样点：可选经 TSN 网格对齐 → 本地 `Db::put` →
    /// 可选经 Raft 复制到多数派 durable。
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

    /// 扫描（透传 `Db::scan`，读己之写）。
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

    /// 阻塞直到已入队记录全部落盘并可见（透传 `Db::flush`）。
    pub fn flush(&self) -> Result<()> {
        self.db.flush()
    }

    /// 冷分层归档（透传 `Db::archive_older_than`；planning 档）。
    pub fn archive_older_than(&self, ts: Timestamp) -> Result<usize> {
        self.db.archive_older_than(ts)
    }

    /// 已归档 segment 数（透传 `Db::archived_segment_count`）。
    pub fn archived_segment_count(&self) -> usize {
        self.db.archived_segment_count()
    }

    /// Raft 多数派确认 durable 的最大日志索引（未启用 Raft 为 `None`）。
    pub fn raft_durable_index(&self) -> Option<u64> {
        self.raft.as_ref().map(|r| r.wals[r.leader].durable_index())
    }

    /// 重置分配计数基线（仅 feature `alloc-count` 下有效果：
    /// 预热结束后调用，随后的 `health().alloc_ok` 反映稳态）。
    pub fn reset_alloc_baseline(&self) {
        #[cfg(feature = "alloc-count")]
        self.alloc_baseline.set(rti_db::alloc_count());
    }

    /// 统一健康视图。
    pub fn health(&self) -> EdgeHealth {
        #[cfg(feature = "alloc-count")]
        let alloc_ok = rti_db::alloc_count() == self.alloc_baseline.get();
        #[cfg(not(feature = "alloc-count"))]
        let alloc_ok = true;
        EdgeHealth {
            profile: self.profile,
            mirror_stats: if self.has_mirror { Some(self.db.mirror_stats()) } else { None },
            raft_role: self.raft.as_ref().map(|r| {
                // 优先报告当前真实 Leader；选主空窗期报告被跟踪节点角色
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

    /// 底层 `Db` 引用（诊断/高级用法）。
    pub fn db(&self) -> &Db {
        &self.db
    }
}
