//! Topic → series registry (SPEC §2).
//!
//! The mapping is **explicit** — no hashing, no auto-assignment at runtime: each
//! [`TopicBinding`] pins one ROS 2 topic to one stable [`SeriesId`], assigned once and
//! persisted. The registry is loaded from `rti-ros2.toml` at startup; topics not present
//! in the registry are counted as skipped, never auto-registered.

use std::fs;
use std::path::Path;

use rti_core::{Error, Result, SeriesId};
use serde::{Deserialize, Serialize};

use crate::FieldSelector;

/// Registry entry: one ROS 2 topic → one rti-db series (SPEC §2, verbatim).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicBinding {
    /// Topic name, e.g. `"/joint_states"`.
    pub topic: String,
    /// Stable series id; assigned once, persisted.
    pub series: SeriesId,
    /// `true` → `put_durable` path (flight-recorder topics).
    pub durable: bool,
    /// Scalar field extraction from the message, e.g. `"effort[3]"`.
    pub field: FieldSelector,
}

/// TOML document shape: `[[bindings]]` entries.
#[derive(Serialize, Deserialize)]
struct RegistryFile {
    bindings: Vec<TopicBinding>,
}

/// Load a registry TOML document from `path`.
///
/// Unknown fields and malformed selectors are hard errors (fail fast at startup, before
/// any message flows) — only unknown *topics at runtime* are skipped, not config errors.
pub fn load_bindings(path: &Path) -> Result<Vec<TopicBinding>> {
    let text = fs::read_to_string(path)?;
    let file: RegistryFile =
        toml::from_str(&text).map_err(|e| Error::Corrupt(format!("registry {}: {e}", path.display())))?;
    Ok(file.bindings)
}

/// Persist `bindings` as a TOML registry document at `path` (stable, human-editable).
pub fn save_bindings(path: &Path, bindings: &[TopicBinding]) -> Result<()> {
    let file = RegistryFile {
        bindings: bindings.to_vec(),
    };
    let text = toml::to_string_pretty(&file)
        .map_err(|e| Error::Corrupt(format!("registry serialize: {e}")))?;
    fs::write(path, text)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_tmp(name: &str) -> std::path::PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rti-ros2-{name}-{}-{n}.toml", std::process::id()))
    }

    #[test]
    fn toml_round_trip_field_by_field() {
        let bindings = vec![
            TopicBinding {
                topic: "/joint_states".into(),
                series: 1,
                durable: true,
                field: FieldSelector::parse("effort[3]").unwrap(),
            },
            TopicBinding {
                topic: "/imu".into(),
                series: 2,
                durable: false,
                field: FieldSelector::parse("/linear_acceleration/z").unwrap(),
            },
        ];
        let path = unique_tmp("round-trip");
        save_bindings(&path, &bindings).unwrap();
        let loaded = load_bindings(&path).unwrap();
        fs::remove_file(&path).ok();
        assert_eq!(loaded.len(), bindings.len());
        for (a, b) in loaded.iter().zip(bindings.iter()) {
            assert_eq!(a.topic, b.topic);
            assert_eq!(a.series, b.series);
            assert_eq!(a.durable, b.durable);
            assert_eq!(a.field, b.field);
        }
    }

    #[test]
    fn load_rejects_malformed_registry() {
        let path = unique_tmp("bad");
        fs::write(&path, "[[bindings]]\ntopic = \"/x\"\nseries = \"oops\"\n").unwrap();
        assert!(load_bindings(&path).is_err());
        fs::remove_file(&path).ok();
    }
}
