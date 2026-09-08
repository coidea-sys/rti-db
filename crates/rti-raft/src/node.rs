//! Raft 协议状态机：选主（PreVote）/ 心跳 / 日志复制 / 多数派提交 /
//! 快照与日志压缩 / 单步成员变更。
//!
//! 设计要点：
//! - **逻辑时钟注入**：[`Node::tick`] 由调用方传入 `now`（建议毫秒），
//!   测试用虚拟时钟实现完全确定性；
//! - 选举超时"随机化"用确定性哈希 `hash(seed, term)`——同种子同任期
//!   结果一致（测试可复现），不同节点/任期错开（活性保证）；
//! - **PreVote**：超时后先以 `term+1` 发起预投票，不抬任期；只有拿到
//!   多数派预投票才进入正式选主。分区节点因此无法抬高集群任期
//!   （收方仅在"自己的 Leader 租约已过期"（`>= election_min`）且
//!   候选人在配置内、日志不落后时才投预投票）；
//! - 日志索引对外 1 起且**绝对化**：快照压缩后 `log[0]` 对应绝对索引
//!   `snap_index + 1`，`prev_log_index == snap_index` 表示以快照点为前缀；
//! - **快照**：`take_snapshot` 丢弃已提交前缀并留存状态字节；Leader 发现
//!   跟随者的 `next_index <= snap_index`（所需前缀已压缩）时改发
//!   `InstallSnapshot` RPC 而非 AppendEntries；
//! - **单步成员变更**：一次只变一个节点，新配置编码为日志条目
//!   （哨兵 series = `u32::MAX` 的 [`Record`]，对用户数据透明）经多数派
//!   **提交后**生效；被移除的节点降级为非投票成员（不再发起选举、
//!   不参与投票与多数派计数）；
//! - 提交规则遵循 Raft 论文：Leader 只按计数推进**当前任期**的条目，
//!   旧任期条目随之间接提交。

use std::collections::BTreeMap;

use rti_core::{Error, Result};
use rti_wal::Record;

/// 集群节点标识。
pub type NodeId = u64;

/// 成员变更日志条目的哨兵 series id（保留，用户数据不得使用）。
///
/// 简化取舍（诚实声明）：单服务器变更把新配置编码为一条普通日志
/// 条目内的哨兵 [`Record`]，复用既有复制/编解码路径，零线格式变更；
/// 代价是 `u32::MAX` 从用户 series 命名空间中保留。
pub const CONF_SERIES: u32 = u32::MAX;

/// 节点角色。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// 跟随者。
    Follower,
    /// 预投票候选人（尚未抬高任期）。
    PreCandidate,
    /// 候选人（正式选主，任期已 +1）。
    Candidate,
    /// 领导者。
    Leader,
}

/// 一条复制日志条目 = 一个 WAL Record 批次 + 写入时的任期。
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// 条目被 Leader 追加时的任期。
    pub term: u64,
    /// 本批 WAL 记录（成员变更条目内含 [`CONF_SERIES`] 哨兵记录）。
    pub records: Vec<Record>,
}

/// 快照：已压缩日志前缀的替代物 + 上层状态机字节。
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// 快照覆盖的最大日志索引（含）。
    pub last_included_index: u64,
    /// 该索引处条目的任期。
    pub last_included_term: u64,
    /// 上层状态机字节（语义由调用方定义，协议层不透明转发）。
    pub state: Vec<u8>,
}

/// 单步成员变更操作。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfChange {
    /// 加入一个投票成员。
    AddPeer(NodeId),
    /// 移除一个投票成员（被移除者降级为非投票成员）。
    RemovePeer(NodeId),
}

/// 把成员变更编码为哨兵记录（`value` 位模式：1=Add，2=Remove）。
pub(crate) fn encode_conf(cc: ConfChange) -> Record {
    let (id, tag) = match cc {
        ConfChange::AddPeer(id) => (id, 1u64),
        ConfChange::RemovePeer(id) => (id, 2u64),
    };
    Record::new(CONF_SERIES, id as i64, f64::from_bits(tag))
}

/// 从记录解码成员变更；非哨兵记录返回 `None`。
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

/// 协议消息。
#[derive(Clone, Debug, PartialEq)]
pub enum Msg {
    /// 预投票请求（`term` 为候选人*将要*使用的任期 = 其当前任期 + 1，
    /// 收方不因此抬高自己的任期）。
    PreVote {
        /// 候选人预备任期。
        term: u64,
        /// 候选人 id。
        candidate: NodeId,
        /// 候选人最后日志索引。
        last_log_index: u64,
        /// 候选人最后日志条目的任期。
        last_log_term: u64,
    },
    /// 预投票应答。
    PreVoteResponse {
        /// 投票人任期。
        term: u64,
        /// 是否投赞成票。
        granted: bool,
    },
    /// 选主请求。
    RequestVote {
        /// 候选人任期。
        term: u64,
        /// 候选人 id。
        candidate: NodeId,
        /// 候选人最后日志索引。
        last_log_index: u64,
        /// 候选人最后日志条目的任期。
        last_log_term: u64,
    },
    /// 选主应答。
    VoteResponse {
        /// 投票人任期。
        term: u64,
        /// 是否投赞成票。
        granted: bool,
    },
    /// 日志复制 / 心跳。
    AppendEntries {
        /// Leader 任期。
        term: u64,
        /// Leader id。
        leader: NodeId,
        /// 新条目之前一条的索引（0 = 日志头；`snap_index` = 快照点）。
        prev_log_index: u64,
        /// 前一条目的任期。
        prev_log_term: u64,
        /// 新条目（空 = 心跳）。
        entries: Vec<Entry>,
        /// Leader 的 commit 水位。
        leader_commit: u64,
    },
    /// 复制应答（亦作 InstallSnapshot 的应答，`match_index` =
    /// 快照的 `last_included_index`）。
    AppendResponse {
        /// 应答者任期。
        term: u64,
        /// 是否复制成功。
        success: bool,
        /// 已确认复制的最大索引（供 Leader 推进 commit）。
        match_index: u64,
    },
    /// 快照安装：跟随者落后超过 Leader 日志起点时替代 AppendEntries。
    InstallSnapshot {
        /// Leader 任期。
        term: u64,
        /// Leader id。
        leader: NodeId,
        /// 快照本体。
        snapshot: Snapshot,
    },
}

/// 传输抽象：消息允许丢失/延迟/乱序（Raft 全部容忍），
/// 故 `send` 不返回错误——失败即丢包，由协议层重试。
pub trait Transport {
    /// 向 `to` 发送一条消息（best-effort）。
    fn send(&mut self, to: NodeId, msg: Msg);
    /// 非阻塞收取一条消息。
    fn recv(&mut self) -> Option<(NodeId, Msg)>;
}

/// 单分片 Raft 节点。
pub struct Node<T: Transport> {
    id: NodeId,
    /// 其余投票成员（不含自己）。
    peers: Vec<NodeId>,
    /// 自己是否为投票成员（被 remove_peer 移除后为 false：
    /// 不发起选举、不投票、不计入多数派，仍以 Follower 身份追日志）。
    voter: bool,
    transport: T,
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<Entry>,
    /// 快照压缩点：`log[i]` 的绝对索引 = `snap_index + 1 + i`。
    snap_index: u64,
    /// 快照点处条目任期。
    snap_term: u64,
    /// 最近一次快照（Leader 发 InstallSnapshot 需要状态字节，故留存）。
    snapshot: Option<Snapshot>,
    commit_index: u64,
    /// 已交付给上层（take_committed）的水位。
    applied_up_to: u64,
    /// candidate：已获票数（含自投）。
    votes_received: usize,
    /// pre-candidate：已获预投票数（含自投）。
    pre_votes: usize,
    /// leader：是否有未提交的本任期成员变更（一次只变一个）。
    pending_conf: bool,
    /// leader：每个跟随者的复制进度。
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,
    leader_id: Option<NodeId>,
    now: u64,
    /// 上次"选举计时重置"（收到合法心跳/投票/开始选举）。
    last_progress: u64,
    last_heartbeat_sent: u64,
    election_min: u64,
    election_span: u64,
    heartbeat_interval: u64,
    /// 预竞选轮数（混入超时哈希，见 [`Node::election_timeout`]）。
    campaign: u64,
    seed: u64,
}

impl<T: Transport> Node<T> {
    /// 创建节点（初始为 Follower，任期 0，投票成员）。
    ///
    /// - `peers`：其余节点 id（不含自己）；集群 = peers ∪ {id}；
    /// - `election_timeout ∈ [min, min+span)` 由 `hash(seed, term)` 决定；
    /// - `heartbeat_interval` 必须显著小于 `min`（建议 ≤ min/3）。
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

    // ------------------------------------------------------------ 观测

    /// 本节点 id。
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// 当前角色。
    pub fn role(&self) -> Role {
        self.role
    }

    /// 是否为 Leader。
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// 当前任期。
    pub fn term(&self) -> u64 {
        self.current_term
    }

    /// 已知的 Leader（未知为 None）。
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// 多数派 commit 水位。
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// 最大日志索引（绝对；含快照点前缀）。
    pub fn log_len(&self) -> u64 {
        self.last_log_index()
    }

    /// 日志内容（未压缩后缀，测试/诊断）。
    pub fn log_entries(&self) -> &[Entry] {
        &self.log
    }

    /// 快照压缩点：绝对索引 ≤ 此值的条目已被丢弃（0 = 无快照）。
    pub fn compacted_index(&self) -> u64 {
        self.snap_index
    }

    /// 最近一次快照（若有）。
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// 当前生效配置（投票成员全集，含自己若在配置内；升序）。
    pub fn membership(&self) -> Vec<NodeId> {
        let mut m = self.peers.clone();
        if self.voter {
            m.push(self.id);
        }
        m.sort_unstable();
        m
    }

    /// 自己是否为投票成员。
    pub fn is_voter(&self) -> bool {
        self.voter
    }

    /// 传输层引用（驱动消息泵）。
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    // ------------------------------------------------------------ 时钟

    /// 本轮竞选的选举超时（确定性"随机"）。
    ///
    /// 哈希混入 `(seed, current_term, campaign)`：预投票不抬任期，
    /// 若超时只随任期变化，同刻超时的节点会每轮同步重试、互相
    /// 拒绝预投票而活锁；竞选轮数使每次重试的退避错开，
    /// 同一事件序列下结果恒定（确定性不受影响）。
    fn election_timeout(&self) -> u64 {
        // SplitMix64 最终混淆。
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

    /// 推进逻辑时钟：Leader 到点发心跳；投票成员的
    /// Follower/PreCandidate/Candidate 到点发起（预）选举。
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
                    return; // 非投票成员永不开票
                }
                if now.saturating_sub(self.last_progress) >= self.election_timeout() {
                    self.start_pre_vote();
                }
            }
        }
    }

    // ------------------------------------------------------------ 上层接口

    /// 提议一批 WAL 记录（仅 Leader）。返回条目索引（1 起，绝对）。
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

    /// 提议一次单步成员变更（仅 Leader，且无上一条未提交的变更）。
    ///
    /// 新配置编码为日志条目，**多数派提交后**才在本地生效；生效前
    /// 多数派仍按旧配置计算，保证一次只变一个的安全性。
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

    /// 加入一个投票成员（[`ConfChange::AddPeer`] 的便捷封装）。
    pub fn add_peer(&mut self, id: NodeId) -> Result<u64> {
        self.change_membership(ConfChange::AddPeer(id))
    }

    /// 移除一个投票成员（[`ConfChange::RemovePeer`] 的便捷封装）。
    /// 允许移除自己（Leader 将在变更提交后退位）。
    pub fn remove_peer(&mut self, id: NodeId) -> Result<u64> {
        self.change_membership(ConfChange::RemovePeer(id))
    }

    /// 取走 `(applied, commit_index]` 的已提交条目（多数派确认 durable）。
    ///
    /// 成员变更条目也会原样返回（内含 [`CONF_SERIES`] 哨兵记录），
    /// 由上层（如 `ReplicatedWal`）过滤。
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

    // ------------------------------------------------------------ 快照

    /// 拍摄快照并压缩日志：丢弃 `compact_upto`（含）之前的已提交前缀。
    ///
    /// - 要求 `snap_index < compact_upto <= commit_index`（只能压缩
    ///   已提交且尚未压缩的条目），否则返回 `Err`；
    /// - `state` 为上层状态机字节，协议层不透明保存/转发；
    /// - 返回的 [`Snapshot`] 同时留存在节点内，供 Leader 向落后的
    ///   跟随者发送 `InstallSnapshot`。
    pub fn take_snapshot(&mut self, compact_upto: u64, state: Vec<u8>) -> Result<Snapshot> {
        if compact_upto <= self.snap_index || compact_upto > self.commit_index {
            return Err(Error::Corrupt(format!(
                "raft: bad snapshot point {compact_upto} (compacted={}, commit={})",
                self.snap_index, self.commit_index
            )));
        }
        let term = self.term_at(compact_upto).expect("compact_upto 在日志范围内");
        let snap = Snapshot { last_included_index: compact_upto, last_included_term: term, state };
        // 丢弃前缀
        let drop = (compact_upto - self.snap_index) as usize;
        self.log.drain(..drop);
        self.snap_index = compact_upto;
        self.snap_term = term;
        self.snapshot = Some(snap.clone());
        Ok(snap)
    }

    /// 本地安装一份快照（例如重启恢复；等价于收到 InstallSnapshot
    /// 但不涉及任期/角色仲裁）。
    ///
    /// 快照点 ≤ 当前压缩点时为空操作。安装后日志只保留快照点之后
    /// 且任期连续的后缀（快照点处任期不匹配则整段丢弃）；
    /// `commit_index` / `applied` 水位抬升到至少快照点——被跳过的
    /// 条目视为已包含在快照状态内，不再经 `take_committed` 交付。
    pub fn install_snapshot(&mut self, snap: Snapshot) -> Result<()> {
        if snap.last_included_index <= self.snap_index {
            return Ok(()); // 陈旧快照
        }
        self.apply_snapshot(snap);
        Ok(())
    }

    /// 快照落地的公共部分。
    fn apply_snapshot(&mut self, snap: Snapshot) {
        let idx = snap.last_included_index;
        let keep_from = if self.term_at(idx) == Some(snap.last_included_term) {
            // 快照点恰好在本地日志中且任期一致：保留其后后缀
            (idx - self.snap_index) as usize
        } else {
            // 快照点超出本地日志或任期冲突：整段丢弃
            self.log.len()
        };
        self.log.drain(..keep_from.min(self.log.len()));
        self.snap_index = idx;
        self.snap_term = snap.last_included_term;
        self.commit_index = self.commit_index.max(idx);
        self.applied_up_to = self.applied_up_to.max(idx);
        self.snapshot = Some(snap);
    }

    // ------------------------------------------------------------ 消息处理

    /// 处理一条收到的协议消息。
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

    /// 见到更高任期：退回 Follower 并更新任期。
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

    /// 日志"新鲜度"比较：候选人的 (last_log_term, last_log_index)
    /// 是否不落后于本地。
    fn log_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        let my_last_term = self.last_log_term();
        last_log_term > my_last_term
            || (last_log_term == my_last_term && last_log_index >= self.last_log_index())
    }

    /// 预投票收方逻辑：不抬任期、不改角色。仅在
    /// 「候选人是投票成员 + 预备任期更高 + 日志不落后 + 自己的
    /// Leader 租约已过期（>= election_min）」时投赞成。
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

        // 前缀已被本地快照压缩：跳过被覆盖的条目，从快照点对齐。
        if prev_log_index < self.snap_index {
            let skip = (self.snap_index - prev_log_index) as usize;
            if entries.len() <= skip {
                // 全部条目都已被快照覆盖：只需推进 commit
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

        // prev 一致性检查
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
        // 冲突截断 + 追加（逐条目对齐；快照点前的条目跳过）
        let mut idx = prev_log_index;
        for e in entries {
            idx += 1;
            if idx <= self.snap_index {
                continue; // 已被快照覆盖
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
        // 推进本地 commit（不超过本地最后索引），并应用新提交的配置
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

    /// 安装来自 Leader 的快照（跟随者落后超过 Leader 日志起点）。
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
            // 回退一格并立即补发（若已退到快照点之下，
            // send_append 会自动改发 InstallSnapshot）
            let next = self.next_index.get(&from).copied().unwrap_or(self.last_log_index() + 1);
            let next = next.saturating_sub(1).max(1);
            self.next_index.insert(from, next);
            self.send_append(from);
        }
    }

    // ------------------------------------------------------------ 内部

    /// 绝对索引 `idx` 处条目的任期；`idx == 0`（空日志头）返回
    /// `Some(0)`，已被压缩的前缀（`idx < snap_index`）返回 `None`。
    fn term_at(&self, idx: u64) -> Option<u64> {
        if idx == 0 {
            return Some(0);
        }
        if idx == self.snap_index {
            return Some(self.snap_term);
        }
        if idx < self.snap_index {
            return None; // 已压缩，不可考
        }
        self.log.get((idx - self.snap_index - 1) as usize).map(|e| e.term)
    }

    fn last_log_index(&self) -> u64 {
        self.snap_index + self.log.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.snap_term)
    }

    /// 当前配置的多数派人数。
    fn majority(&self) -> usize {
        let cluster = self.peers.len() + usize::from(self.voter);
        cluster / 2 + 1
    }

    /// 选举超时后的第一步：预投票（不抬任期）。
    fn start_pre_vote(&mut self) {
        self.campaign += 1; // 新一轮退避（见 election_timeout）
        self.role = Role::PreCandidate;
        self.pre_votes = 1; // 自投
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

    /// 预投票获多数后进入正式选主（任期 +1）。
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

    /// 向某跟随者按其 next_index 发送 AppendEntries；所需前缀已被
    /// 压缩（`next <= snap_index`）时改发 InstallSnapshot。
    fn send_append(&mut self, to: NodeId) {
        let next = self.next_index.get(&to).copied().unwrap_or(self.last_log_index() + 1);
        if next <= self.snap_index {
            // 跟随者落后超过日志起点：走 InstallSnapshot 而非 AppendEntries。
            // snapshot 自 take_snapshot/install_snapshot 起留存，必为 Some。
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
        let prev_log_term = self.term_at(prev_log_index).expect("prev 在快照点之后，必可查");
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

    /// Leader 按多数派 match_index 推进 commit（仅当前任期条目可直接
    /// 推进，旧任期条目随之间接提交；只计数当前配置内的投票成员）。
    /// 条目提交后立即应用其中的成员变更（单步协议：变更条目本身
    /// 仍按旧配置计票）。
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

    /// 应用 `(from, to]`（含两端、绝对索引）区间内新提交条目中的
    /// 成员变更。逐条扫描，哨兵记录之外的用户数据不受影响。
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

    /// 应用一条已提交的单步成员变更。
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
                    // 被移出配置：退位并降级为非投票成员
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
