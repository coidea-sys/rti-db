//! Real `rclrs`/DDS transport backend — **placeholder** (SPEC §2 implementation note).
//!
//! This module only exists under the non-default `ros2-rclrs` feature:
//!
//! ```toml
//! rti-ros2 = { version = "0.7", features = ["ros2-rclrs"] }
//! ```
//!
//! ## Building against a real ROS 2 installation
//!
//! Enabling this feature documents the *intended* production backend. A working
//! implementation requires:
//!
//! - a system ROS 2 installation (Humble or newer): `rcl` discoverable by the linker
//!   (`AMENT_PREFIX_PATH` / `LD_LIBRARY_PATH` set up via `source /opt/ros/<distro>/setup.bash`);
//! - the `rclrs` crate (ROS 2 client library for Rust) added as an optional dependency;
//! - message deserialization into the transport-agnostic [`serde_json::Value`] shape
//!   (per-message-type dynamic introspection via `rosidl_runtime_c`), so the existing
//!   [`crate::FieldSelector`] paths apply unchanged.
//!
//! ## Why it is a placeholder in v0.7.0
//!
//! The sandbox/CI target of v0.7.0 has no ROS 2 installation, and the SPEC requires the
//! full bridge contract — including the 100-topic / 50 kHz acceptance logic — to be
//! testable in the default build. Hence the entire contract is implemented against the
//! [`Transport`] trait and shipped on [`crate::InProcessTransport`]; this backend keeps
//! the public API surface identical ([`Ros2Bridge::open_with_transport`]) and will be
//! filled in on a ROS 2-equipped target without any API change.
//!
//! [`Ros2Bridge::open_with_transport`]: crate::Ros2Bridge::open_with_transport

use rti_core::Result;

use crate::transport::{RawMessage, Transport};
use crate::NodeConfig;

/// `rclrs`/DDS transport (placeholder; see module docs for the bring-up recipe).
///
/// The type exists so downstream code can be written against the production backend
/// today; constructing or using it panics with `unimplemented!()` until a system
/// ROS 2 installation is wired up.
pub struct RclrsTransport {
    _private: (),
}

impl RclrsTransport {
    /// Create the rclrs-backed transport for `node_cfg`.
    ///
    /// # Panics
    /// Always `unimplemented!()` in v0.7.0: requires a system ROS 2 installation and
    /// the `rclrs` dependency (see module docs).
    pub fn new(_node_cfg: &NodeConfig) -> Result<Self> {
        unimplemented!(
            "rclrs backend: build on a host with a system ROS 2 installation \
             (source /opt/ros/<distro>/setup.bash) and wire the optional rclrs \
             dependency; the bridge contract is meanwhile fully available via \
             InProcessTransport"
        )
    }
}

impl Transport for RclrsTransport {
    fn try_recv(&self) -> Option<RawMessage> {
        unimplemented!("rclrs backend placeholder (see module docs)")
    }

    fn publish(&self, _topic: &str, _payload: serde_json::Value) -> Result<()> {
        unimplemented!("rclrs backend placeholder (see module docs)")
    }

    fn name(&self) -> &str {
        "rclrs"
    }
}
