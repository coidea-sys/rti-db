//! Transport abstraction (SPEC §2 implementation note).
//!
//! The bridge core is transport-agnostic: every public contract — registry, backpressure,
//! stats, replay — is implemented against the [`Transport`] trait. The default build uses
//! [`InProcessTransport`] (in-process channels), so the full contract is testable without
//! ROS 2; the production `rclrs`/DDS backend ships behind the non-default `ros2-rclrs`
//! feature with an identical public API.
//!
//! Message model: transports carry [`RawMessage`] — topic name, nanosecond timestamp
//! (the transport stamps it at receive time; it becomes the stored sample timestamp and
//! the basis of the lag measurement) and a structured payload ([`serde_json::Value`]).

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rti_core::{Error, Result, Timestamp};

/// One message delivered by a [`Transport`]: topic + receive timestamp + structured payload.
#[derive(Clone, Debug)]
pub struct RawMessage {
    /// Topic the message arrived on (e.g. `"/joint_states"`).
    pub topic: String,
    /// Nanosecond timestamp stamped by the transport at receive time.
    pub ts: Timestamp,
    /// Structured message payload (a DDS backend deserializes into this same shape).
    pub payload: serde_json::Value,
}

/// Topic transport abstraction: the bridge's only coupling to the outside bus.
///
/// **Non-blocking contract.** Neither method may block the caller: `try_recv` returns
/// `None` when no message is currently available, and `publish` must hand the message
/// off (or fail with an error) immediately — the bridge must never block DDS/transport
/// senders, and symmetrically a transport must never block the bridge's replay path.
pub trait Transport: Send + Sync {
    /// Pull the next pending inbound message; `None` when the inbound side is drained.
    fn try_recv(&self) -> Option<RawMessage>;

    /// Publish `payload` onto `topic` (replay/sim direction). Never blocks; failures
    /// are reported as errors, not retries.
    fn publish(&self, topic: &str, payload: serde_json::Value) -> Result<()>;

    /// Backend name, for diagnostics (`"in-process"`, `"rclrs"`, ...).
    fn name(&self) -> &str;
}

/// Injection handle of an [`InProcessTransport`]: the "ROS side" feeding the bridge.
///
/// `Clone`, `Send`; [`InProcessSender::send`] is an unbounded-channel push — it never
/// blocks and never fails while the transport exists, which is exactly the property the
/// drop-oldest backpressure contract protects (congestion is absorbed inside the bridge,
/// never pushed back onto the sender).
#[derive(Clone)]
pub struct InProcessSender {
    tx: Sender<RawMessage>,
}

impl InProcessSender {
    /// Inject one inbound message. Never blocks.
    pub fn send(&self, msg: RawMessage) -> Result<()> {
        self.tx
            .send(msg)
            .map_err(|_| Error::Corrupt("in-process transport closed".into()))
    }

    /// Convenience: inject `payload` on `topic` stamped with `ts`.
    pub fn send_value(&self, topic: &str, ts: Timestamp, payload: serde_json::Value) -> Result<()> {
        self.send(RawMessage {
            topic: topic.to_string(),
            ts,
            payload,
        })
    }
}

/// One message recorded on the publish (replay) side of an [`InProcessTransport`].
#[derive(Clone, Debug)]
pub struct PublishedMsg {
    /// Topic the message was published onto.
    pub topic: String,
    /// Published payload.
    pub payload: serde_json::Value,
    /// Wall-clock instant the transport accepted the publication (replay pacing tests).
    pub at: Instant,
}

struct InProcessState {
    rx: Receiver<RawMessage>,
    published: Vec<PublishedMsg>,
}

/// Default transport: in-process channels standing in for the DDS bus.
///
/// - inbound: [`InProcessSender`] (the simulated ROS side) → unbounded channel →
///   [`Transport::try_recv`] drained by the bridge pump thread;
/// - outbound: [`Transport::publish`] appends to a recorded log inspectable via
///   [`InProcessTransport::published`] — this is how replay pacing is verified.
pub struct InProcessTransport {
    state: Arc<Mutex<InProcessState>>,
}

impl InProcessTransport {
    /// Create a transport plus its injection handle.
    pub fn new() -> (Self, InProcessSender) {
        let (tx, rx) = channel();
        (
            InProcessTransport {
                state: Arc::new(Mutex::new(InProcessState {
                    rx,
                    published: Vec::new(),
                })),
            },
            InProcessSender { tx },
        )
    }

    /// Snapshot of everything published so far (in publish order).
    pub fn published(&self) -> Vec<PublishedMsg> {
        self.state.lock().unwrap().published.clone()
    }

    /// Number of messages published so far.
    pub fn published_len(&self) -> usize {
        self.state.lock().unwrap().published.len()
    }
}

impl Transport for InProcessTransport {
    fn try_recv(&self) -> Option<RawMessage> {
        self.state.lock().unwrap().rx.try_recv().ok()
    }

    fn publish(&self, topic: &str, payload: serde_json::Value) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        st.published.push(PublishedMsg {
            topic: topic.to_string(),
            payload,
            at: Instant::now(),
        });
        Ok(())
    }

    fn name(&self) -> &str {
        "in-process"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn send_recv_round_trip() {
        let (t, tx) = InProcessTransport::new();
        tx.send_value("/a", 7, json!({ "v": 1.5 })).unwrap();
        let m = t.try_recv().unwrap();
        assert_eq!(m.topic, "/a");
        assert_eq!(m.ts, 7);
        assert_eq!(m.payload["v"], 1.5);
        assert!(t.try_recv().is_none());
    }

    #[test]
    fn publish_is_recorded() {
        let (t, _tx) = InProcessTransport::new();
        t.publish("/replay", json!({ "value": 2.0 })).unwrap();
        assert_eq!(t.published_len(), 1);
        assert_eq!(t.published()[0].topic, "/replay");
        assert_eq!(t.name(), "in-process");
    }
}
