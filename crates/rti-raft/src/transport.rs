//! 传输实现：内存网络（测试/仿真，支持分区）与 TCP（loopback/部署）。

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::time::Duration;

use crate::codec::encode_msg;
use crate::node::{Msg, NodeId, Transport};

// ------------------------------------------------------------- 内存网络

#[derive(Default)]
struct SharedNet {
    queues: BTreeMap<NodeId, VecDeque<(NodeId, Msg)>>,
    /// 被阻断的有向边集合（partition 会同时加入两个方向）。
    blocked: Vec<(NodeId, NodeId)>,
}

/// 单线程内存网络：节点共享的投递中枢，支持双向分区（测试用）。
///
/// 经 [`MemoryNetwork::transport`] 为每个节点创建端点；
/// [`MemoryNetwork::partition`] / [`MemoryNetwork::heal`] 模拟网络分区。
/// `Clone` 得到的句柄与所有端点共享同一份状态。
#[derive(Clone, Default)]
pub struct MemoryNetwork {
    shared: Rc<RefCell<SharedNet>>,
}

impl MemoryNetwork {
    /// 创建空网络。
    pub fn new() -> Self {
        Self::default()
    }

    /// 为节点 `id` 注册并返回其传输端点。
    pub fn transport(&self, id: NodeId) -> MemoryTransport {
        self.shared.borrow_mut().queues.entry(id).or_default();
        MemoryTransport { id, shared: Rc::clone(&self.shared) }
    }

    /// 网络分区：阻断集合 a 与集合 b 之间的双向流量。
    pub fn partition(&self, a: &[NodeId], b: &[NodeId]) {
        let mut net = self.shared.borrow_mut();
        for &x in a {
            for &y in b {
                net.blocked.push((x, y));
                net.blocked.push((y, x));
            }
        }
    }

    /// 恢复全部流量。
    pub fn heal(&self) {
        self.shared.borrow_mut().blocked.clear();
    }
}

/// 内存传输端点（单线程测试用；与 [`MemoryNetwork`] 共享状态）。
pub struct MemoryTransport {
    id: NodeId,
    shared: Rc<RefCell<SharedNet>>,
}

impl Transport for MemoryTransport {
    fn send(&mut self, to: NodeId, msg: Msg) {
        let mut net = self.shared.borrow_mut();
        if net.blocked.contains(&(self.id, to)) {
            return; // 分区丢包
        }
        net.queues.entry(to).or_default().push_back((self.id, msg));
    }

    fn recv(&mut self) -> Option<(NodeId, Msg)> {
        self.shared.borrow_mut().queues.get_mut(&self.id)?.pop_front()
    }
}

// ------------------------------------------------------------- TCP

/// TCP 传输：每节点一个监听 socket，按需建立短连接发送帧。
///
/// 线格式：`len u32` + `from u64` + payload（发送者 id 显式携带，
/// 因为短连接的对端端口是临时端口，无法反查）。
///
/// 简化取舍（诚实声明）：`send` 为「连接-写-关闭」的短连接
/// （无连接池），失败静默丢包（Raft 容忍，由协议层重试）；
/// 面向 loopback 与小集群验证，不是高吞吐 RPC 层。
pub struct TcpTransport {
    id: NodeId,
    listener: TcpListener,
    peers: BTreeMap<NodeId, SocketAddr>,
    /// 已接受的入站连接及其读缓冲。
    inbound: Vec<(TcpStream, Vec<u8>)>,
    inbox: VecDeque<(NodeId, Msg)>,
}

impl TcpTransport {
    /// 绑定 `bind` 地址并注册对等节点地址表。
    ///
    /// 非阻塞 accept/read；`send` 用 100ms 连接超时，避免对端
    /// 不存在时挂死热路径。
    pub fn new(id: NodeId, bind: SocketAddr, peers: &[(NodeId, SocketAddr)]) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            id,
            listener,
            peers: peers.iter().copied().collect(),
            inbound: Vec::new(),
            inbox: VecDeque::new(),
        })
    }

    /// 本端实际监听地址（`bind` 端口为 0 时查询分配结果）。
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// 本节点 id。
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// 追加/更新对等节点地址（构造后补注册用）。
    pub fn add_peer(&mut self, id: NodeId, addr: SocketAddr) {
        self.peers.insert(id, addr);
    }

    /// 非阻塞收包：accept 新连接、读空各连接、切出完整帧。
    fn poll_io(&mut self) {
        while let Ok((stream, _)) = self.listener.accept() {
            if stream.set_nonblocking(true).is_ok() {
                self.inbound.push((stream, Vec::new()));
            }
        }
        let mut tmp: Vec<(TcpStream, Vec<u8>)> = Vec::new();
        for (mut s, mut acc) in std::mem::take(&mut self.inbound) {
            let mut chunk = [0u8; 8192];
            let mut alive = true;
            loop {
                match s.read(&mut chunk) {
                    Ok(0) => {
                        alive = false; // 对端关闭
                        break;
                    }
                    Ok(n) => acc.extend_from_slice(&chunk[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        alive = false;
                        break;
                    }
                }
            }
            // 切帧：len u32 + from u64 + payload
            loop {
                if acc.len() < 4 {
                    break;
                }
                let len = u32::from_le_bytes(acc[0..4].try_into().unwrap()) as usize;
                if len > 16 << 20 {
                    acc.clear(); // 畸形长度：丢弃缓冲（视为丢包）
                    break;
                }
                if acc.len() < 4 + len || len < 8 {
                    if len < 8 {
                        acc.clear();
                    }
                    break;
                }
                let from = u64::from_le_bytes(acc[4..12].try_into().unwrap());
                match crate::codec::decode_payload(&acc[12..4 + len]) {
                    Some(msg) => {
                        acc.drain(..4 + len);
                        self.inbox.push_back((from, msg));
                    }
                    None => {
                        acc.clear();
                        break;
                    }
                }
            }
            if alive {
                tmp.push((s, acc));
            }
        }
        self.inbound = tmp;
    }
}

impl Transport for TcpTransport {
    fn send(&mut self, to: NodeId, msg: Msg) {
        let Some(&addr) = self.peers.get(&to) else { return };
        let mut inner = Vec::new();
        encode_msg(&msg, &mut inner); // len u32 + payload
        let payload = &inner[4..];
        let mut frame = Vec::with_capacity(12 + payload.len());
        frame.extend_from_slice(&((8 + payload.len()) as u32).to_le_bytes());
        frame.extend_from_slice(&self.id.to_le_bytes());
        frame.extend_from_slice(payload);
        // 短连接发送；任何失败 = 丢包（协议层会重试）。
        if let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            let _ = s.write_all(&frame);
        }
    }

    fn recv(&mut self) -> Option<(NodeId, Msg)> {
        self.poll_io();
        self.inbox.pop_front()
    }
}
