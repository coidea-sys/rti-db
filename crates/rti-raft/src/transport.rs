//! Transport implementations: in-memory network (test/simulation, supports partitions) and TCP (loopback/deployment).

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::time::Duration;

use crate::codec::encode_msg;
use crate::node::{Msg, NodeId, Transport};

// ------------------------------------------------------------- in-memory network

#[derive(Default)]
struct SharedNet {
    queues: BTreeMap<NodeId, VecDeque<(NodeId, Msg)>>,
    /// Set of blocked directed edges (partition adds both directions).
    blocked: Vec<(NodeId, NodeId)>,
}

/// Single-threaded in-memory network: a shared delivery hub for nodes, supports bidirectional partitions (for tests).
///
/// Endpoints are created per node via [`MemoryNetwork::transport`];
/// [`MemoryNetwork::partition`] / [`MemoryNetwork::heal`] simulate network partitions.
/// Handles obtained via `Clone` share the same state with all endpoints.
#[derive(Clone, Default)]
pub struct MemoryNetwork {
    shared: Rc<RefCell<SharedNet>>,
}

impl MemoryNetwork {
    /// Create an empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register node `id` and return its transport endpoint.
    pub fn transport(&self, id: NodeId) -> MemoryTransport {
        self.shared.borrow_mut().queues.entry(id).or_default();
        MemoryTransport { id, shared: Rc::clone(&self.shared) }
    }

    /// Partition the network: block bidirectional traffic between set a and set b.
    pub fn partition(&self, a: &[NodeId], b: &[NodeId]) {
        let mut net = self.shared.borrow_mut();
        for &x in a {
            for &y in b {
                net.blocked.push((x, y));
                net.blocked.push((y, x));
            }
        }
    }

    /// Restore all traffic.
    pub fn heal(&self) {
        self.shared.borrow_mut().blocked.clear();
    }
}

/// In-memory transport endpoint (single-threaded tests; shares state with [`MemoryNetwork`]).
pub struct MemoryTransport {
    id: NodeId,
    shared: Rc<RefCell<SharedNet>>,
}

impl Transport for MemoryTransport {
    fn send(&mut self, to: NodeId, msg: Msg) {
        let mut net = self.shared.borrow_mut();
        if net.blocked.contains(&(self.id, to)) {
            return; // packets dropped by the partition
        }
        net.queues.entry(to).or_default().push_back((self.id, msg));
    }

    fn recv(&mut self) -> Option<(NodeId, Msg)> {
        self.shared.borrow_mut().queues.get_mut(&self.id)?.pop_front()
    }
}

// ------------------------------------------------------------- TCP

/// TCP transport: one listening socket per node; frames are sent over on-demand short connections.
///
/// Wire format: `len u32` + `from u64` + payload (the sender id is carried explicitly,
/// because the peer port of a short connection is ephemeral and cannot be looked up).
///
/// Simplifying trade-off (honest disclosure): `send` uses connect-write-close short connections
/// (no connection pooling), and failures silently drop the packet (Raft tolerates this; the protocol layer retries);
/// aimed at loopback and small-cluster validation, not a high-throughput RPC layer.
pub struct TcpTransport {
    id: NodeId,
    listener: TcpListener,
    peers: BTreeMap<NodeId, SocketAddr>,
    /// Accepted inbound connections and their read buffers.
    inbound: Vec<(TcpStream, Vec<u8>)>,
    inbox: VecDeque<(NodeId, Msg)>,
}

impl TcpTransport {
    /// Bind the `bind` address and register the peer address table.
    ///
    /// Non-blocking accept/read; `send` uses a 100ms connect timeout to avoid hanging the hot
    /// path when a peer does not exist.
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

    /// The actual local listening address (query the assigned port when `bind` uses port 0).
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Add/update a peer address (for post-construction registration).
    pub fn add_peer(&mut self, id: NodeId, addr: SocketAddr) {
        self.peers.insert(id, addr);
    }

    /// Non-blocking receive: accept new connections, drain each connection, cut out complete frames.
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
                        alive = false; // peer closed
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
            // frame cutting: len u32 + from u64 + payload
            loop {
                if acc.len() < 4 {
                    break;
                }
                let len = u32::from_le_bytes(acc[0..4].try_into().unwrap()) as usize;
                if len > 16 << 20 {
                    acc.clear(); // malformed length: discard the buffer (treated as packet loss)
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
        // short-connection send; any failure = packet loss (the protocol layer retries).
        if let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            let _ = s.write_all(&frame);
        }
    }

    fn recv(&mut self) -> Option<(NodeId, Msg)> {
        self.poll_io();
        self.inbox.pop_front()
    }
}
