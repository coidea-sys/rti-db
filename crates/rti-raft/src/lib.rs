//! rti-raft：rti-db 的单分片简化 Raft 副本（v0.5）。
//!
//! 范围（诚实声明）：单分片、内存日志；覆盖 Raft 核心四件事——
//! 选主（确定性种子的"随机"超时 + PreVote 预投票）、心跳、
//! 日志复制（AppendEntries 含冲突截断）、多数派提交；v0.5 增加
//! 快照（InstallSnapshot RPC + 日志压缩）与单步成员变更
//! （`add_peer`/`remove_peer`，提交生效，一次只变一个）。
//!
//! - [`Node`]：协议状态机，**时间由调用方以逻辑时钟注入**
//!   （`tick(now)`），测试用虚拟时钟做到完全确定性；
//! - [`Transport`]：传输抽象（`send`/`recv`，允许丢包——Raft 天然容忍），
//!   提供 [`MemoryTransport`]（测试/仿真，支持网络分区）与
//!   [`TcpTransport`]（loopback/真实部署）；
//! - [`ReplicatedWal`]：与 rti-wal 的集成点——日志条目即 WAL
//!   [`Record`] 批次，**多数派确认（commit）后才算 durable**。

#![forbid(unsafe_code)]

mod codec;
mod node;
mod transport;

pub use codec::{decode_msg, encode_msg};
pub use node::{ConfChange, Entry, Msg, Node, NodeId, Role, Snapshot, Transport, CONF_SERIES};
pub use transport::{MemoryNetwork, MemoryTransport, TcpTransport};

use rti_core::{Error, Result};
use rti_wal::Record;

/// 复制 WAL：[`Node`] 的薄包装，把 Raft 提交语义映射为 WAL 持久性语义。
///
/// - [`ReplicatedWal::append_batch`] 向 Leader 提议一批 [`Record`]
///   （一个 Raft 日志条目 = 一个 WAL Record 批次）；
/// - 条目被多数派复制后才推进 commit，[`ReplicatedWal::take_durable`]
///   只返回**已确认 durable** 的 Record——在此之前的记录在多数派
///   崩溃下可能丢失，调用方不得将其视为崩溃安全。
pub struct ReplicatedWal<T: Transport> {
    node: Node<T>,
}

impl<T: Transport> ReplicatedWal<T> {
    /// 包装一个 Raft 节点。
    pub fn new(node: Node<T>) -> Self {
        Self { node }
    }

    /// 提议一批记录（仅 Leader；非 Leader 返回 `Err(Error::SeriesFull)`
    /// 语义的背压信号——调用方应转发给 Leader 或等待选主）。
    ///
    /// 返回条目索引；**返回时不保证 durable**，用
    /// [`ReplicatedWal::durable_index`] / [`ReplicatedWal::take_durable`]
    /// 跟踪多数派确认进度。
    pub fn append_batch(&mut self, records: Vec<Record>) -> Result<u64> {
        self.node.propose(records)
    }

    /// 驱动协议时钟（心跳/选主超时），时间语义由调用方定义（毫秒建议）。
    pub fn tick(&mut self, now: u64) {
        self.node.tick(now);
    }

    /// 处理一条收到的协议消息。
    pub fn handle(&mut self, from: NodeId, msg: Msg) {
        self.node.handle(from, msg);
    }

    /// 已确认 durable 的最大日志索引（多数派 commit 水位）。
    pub fn durable_index(&self) -> u64 {
        self.node.commit_index()
    }

    /// 取走新确认 durable 的记录（按日志顺序摊平批次）。
    ///
    /// 成员变更条目（哨兵 series = [`CONF_SERIES`]）对用户数据透明，
    /// 在此过滤，不会出现在返回流中。
    pub fn take_durable(&mut self) -> Vec<Record> {
        let entries = self.node.take_committed();
        let n: usize = entries.iter().map(|e| e.records.len()).sum();
        let mut out = Vec::with_capacity(n);
        for e in entries {
            out.extend(e.records.into_iter().filter(|r| r.series != CONF_SERIES));
        }
        out
    }

    /// 本节点是否为 Leader。
    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    /// 当前任期。
    pub fn term(&self) -> u64 {
        self.node.term()
    }

    /// 底层节点引用（诊断/测试）。
    pub fn node(&self) -> &Node<T> {
        &self.node
    }

    /// 底层节点可变引用（驱动/测试）。
    pub fn node_mut(&mut self) -> &mut Node<T> {
        &mut self.node
    }
}

/// 非 Leader 提议的错误构造（背压语义，供调用方转发/重试）。
pub(crate) fn not_leader_err() -> Error {
    Error::Corrupt("raft: not leader (redirect or wait for election)".into())
}
