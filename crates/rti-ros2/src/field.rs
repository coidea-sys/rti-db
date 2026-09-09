//! Scalar field extraction from structured messages.
//!
//! Messages are modeled as [`serde_json::Value`] (the transport-agnostic message
//! representation; a real DDS backend deserializes into the same shape). A
//! [`FieldSelector`] is a parsed path such as `"effort[3]"`, `"/pose/position/x"` or
//! `"scan/ranges[0][1]"`: `/`-separated object fields, each optionally followed by one
//! or more `[index]` array subscripts. The selected node must be a JSON number,
//! extracted as `f64`.

use std::fmt;
use std::str::FromStr;

use rti_core::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One parsed path segment: an object field plus zero or more array subscripts.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PathSeg {
    field: String,
    indices: Vec<usize>,
}

/// Scalar field extraction path into a structured message (SPEC §2).
///
/// Parse with [`FieldSelector::parse`] (or `"...".parse()`), apply with
/// [`FieldSelector::extract`]. Serializes to/from its canonical string form so a
/// [`crate::TopicBinding`] registry round-trips through TOML unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldSelector {
    /// Canonical string form (no leading `/`; indices kept inline, e.g. `effort[3]`).
    raw: String,
    segs: Vec<PathSeg>,
}

impl FieldSelector {
    /// Parse a selector path: `/`-separated fields with optional `[index]` subscripts.
    ///
    /// A single leading `/` is accepted and stripped (topic-style paths). Errors on an
    /// empty path, empty field names, malformed or out-of-range indices.
    pub fn parse(path: &str) -> Result<Self> {
        let p = path.strip_prefix('/').unwrap_or(path);
        if p.is_empty() {
            return Err(Error::Corrupt(format!("field selector: empty path {path:?}")));
        }
        let mut segs = Vec::new();
        for seg in p.split('/') {
            segs.push(parse_seg(seg, path)?);
        }
        Ok(FieldSelector {
            raw: p.to_string(),
            segs,
        })
    }

    /// Canonical string form (as parsed, without a leading `/`).
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Walk `msg` along the path and extract the selected number as `f64`.
    ///
    /// Errors (as [`Error::Corrupt`]) when a field is missing, an index is out of
    /// bounds, an intermediate node has the wrong shape, or the selected node is not a
    /// number.
    pub fn extract(&self, msg: &serde_json::Value) -> Result<f64> {
        let mut cur = msg;
        for seg in &self.segs {
            cur = cur.get(&seg.field).ok_or_else(|| {
                Error::Corrupt(format!(
                    "field selector {:?}: missing field {:?}",
                    self.raw, seg.field
                ))
            })?;
            for &i in &seg.indices {
                cur = cur.get(i).ok_or_else(|| {
                    Error::Corrupt(format!(
                        "field selector {:?}: index {i} out of bounds in field {:?}",
                        self.raw, seg.field
                    ))
                })?;
            }
        }
        cur.as_f64().ok_or_else(|| {
            Error::Corrupt(format!(
                "field selector {:?}: selected node is not a number (got {cur})",
                self.raw
            ))
        })
    }
}

/// Parse one `/`-separated segment: `name` followed by zero or more `[index]` suffixes.
fn parse_seg(seg: &str, whole: &str) -> Result<PathSeg> {
    let err = |why: &str| Error::Corrupt(format!("field selector {whole:?}: {why}"));
    let field_end = seg.find('[').unwrap_or(seg.len());
    let field = &seg[..field_end];
    if field.is_empty() {
        return Err(err("empty field name"));
    }
    if field.contains(']') {
        return Err(err("unexpected ']' in field name"));
    }
    let mut indices = Vec::new();
    let mut rest = &seg[field_end..];
    while !rest.is_empty() {
        let (num, tail) = rest
            .strip_prefix('[')
            .and_then(|r| r.split_once(']'))
            .ok_or_else(|| err("malformed index (expected `[<usize>]`)"))?;
        let i: usize = num
            .parse()
            .map_err(|_| err("index is not a non-negative integer"))?;
        indices.push(i);
        rest = tail;
    }
    Ok(PathSeg {
        field: field.to_string(),
        indices,
    })
}

impl fmt::Display for FieldSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl FromStr for FieldSelector {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl Serialize for FieldSelector {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for FieldSelector {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        FieldSelector::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_field_and_index() {
        let msg = json!({
            "name": ["joint0", "joint1"],
            "effort": [0.1, 0.2, 0.3, -4.5],
            "pose": { "position": { "x": 1.25, "y": 2 } },
        });
        assert_eq!(FieldSelector::parse("effort[3]").unwrap().extract(&msg).unwrap(), -4.5);
        assert_eq!(
            FieldSelector::parse("/pose/position/x").unwrap().extract(&msg).unwrap(),
            1.25
        );
        // integer node → f64
        assert_eq!(
            FieldSelector::parse("pose/position/y").unwrap().extract(&msg).unwrap(),
            2.0
        );
        // canonical form strips the leading '/'
        assert_eq!(FieldSelector::parse("/pose/position/x").unwrap().as_str(), "pose/position/x");
    }

    #[test]
    fn multi_dimensional_index() {
        let msg = json!({ "scan": { "ranges": [[1, 2], [3, 4]] } });
        let sel = FieldSelector::parse("scan/ranges[1][0]").unwrap();
        assert_eq!(sel.extract(&msg).unwrap(), 3.0);
    }

    #[test]
    fn invalid_paths_are_rejected() {
        for bad in ["", "/", "a//b", "[0]", "a[", "a[]", "a[x]", "a[0", "a]0[", "a[18446744073709551616]"] {
            assert!(FieldSelector::parse(bad).is_err(), "expected {bad:?} to fail");
        }
    }

    #[test]
    fn extraction_errors_on_wrong_shape() {
        let msg = json!({ "a": { "b": [1, 2] }, "s": "text" });
        // missing field
        assert!(FieldSelector::parse("a/nope").unwrap().extract(&msg).is_err());
        // index out of bounds
        assert!(FieldSelector::parse("a/b[5]").unwrap().extract(&msg).is_err());
        // index into a non-array
        assert!(FieldSelector::parse("s[0]").unwrap().extract(&msg).is_err());
        // selected node is not a number
        assert!(FieldSelector::parse("s").unwrap().extract(&msg).is_err());
        assert!(FieldSelector::parse("a").unwrap().extract(&msg).is_err());
    }

    #[test]
    fn string_round_trip() {
        let sel = FieldSelector::parse("/joint_states/effort[3]").unwrap();
        let s = sel.to_string();
        assert_eq!(s, "joint_states/effort[3]");
        assert_eq!(s.parse::<FieldSelector>().unwrap(), sel);
    }
}
