//! rti-ros2: ROS 2 topic bridge — turns rti-db into the flight recorder of any ROS 2
//! robot with zero application code changes (v0.7, SPEC §2).
//!
//! Architecture: a standalone bridge process (`rti-ros2d`), never a library linked into
//! control nodes. It subscribes to sensor topics through a [`Transport`], maps topics to
//! [`SeriesId`]s through an explicit registry file ([`TopicBinding`], loaded/saved as TOML
//! via [`load_bindings`] / [`save_bindings`]), and calls [`Db::put`] (default) or
//! [`Db::put_durable`] (topics marked `durable`, the flight-recorder path).
//!
//! Contracts (SPEC §2):
//! - topic → series mapping is **explicit** (no hashing surprises);
//! - non-scalar message fields require an explicit [`FieldSelector`]
//!   (`"/joint_states/effort[3]"`-style paths over a structured message);
//! - backpressure policy is **drop-oldest on the ROS side with a counter**
//!   ([`BridgeStats::dropped`]) — the bridge never blocks the transport/DDS layer;
//! - [`Ros2Bridge::replay`] publishes a historical window back onto a topic at
//!   `speed ×` the original timing using stored nanosecond timestamps.
//!
//! Transport abstraction (SPEC §2 implementation note): the bridge core is
//! transport-agnostic — all public contracts above are implemented against the
//! [`Transport`] trait. The default build uses [`InProcessTransport`] (in-process
//! channels), which makes the full contract — including the 100-topic / 50 kHz
//! acceptance logic — testable without ROS 2. The production `rclrs`/DDS backend ships
//! behind the non-default `ros2-rclrs` feature (requires a system ROS 2 installation);
//! the public API is identical across backends.

#![forbid(unsafe_code)]

mod bridge;
mod field;
mod registry;
mod transport;

#[cfg(feature = "ros2-rclrs")]
pub mod rclrs_backend;

pub use bridge::{BridgeStats, NodeConfig, ReplayHandle, Ros2Bridge};
pub use field::FieldSelector;
pub use registry::{load_bindings, save_bindings, TopicBinding};
pub use transport::{InProcessSender, InProcessTransport, PublishedMsg, RawMessage, Transport};
