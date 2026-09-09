//! `episodes.toml` parsing (CLI input).
//!
//! Format:
//!
//! ```toml
//! [[episode]]
//! name = "pick_place_000"
//! start_ns = 0
//! end_ns = 1_000_000_000
//! frame_hz = 50               # optional; omit for the union-of-timestamps grid
//!
//! [[episode.series]]
//! column = "joint0.pos"
//! id = 7
//!
//! [[episode.series]]
//! column = "joint1.pos"
//! id = 8
//! ```

use std::path::Path;

use serde::Deserialize;

use crate::{EpisodeSpec, Error, Result};

#[derive(Debug, Deserialize)]
struct SpecFile {
    episode: Vec<EpisodeToml>,
}

#[derive(Debug, Deserialize)]
struct EpisodeToml {
    name: String,
    start_ns: i64,
    end_ns: i64,
    frame_hz: Option<u32>,
    series: Vec<SeriesToml>,
}

#[derive(Debug, Deserialize)]
struct SeriesToml {
    column: String,
    id: u32,
}

impl From<EpisodeToml> for EpisodeSpec {
    fn from(e: EpisodeToml) -> Self {
        EpisodeSpec {
            name: e.name,
            series: e.series.into_iter().map(|s| (s.column, s.id)).collect(),
            start: e.start_ns,
            end: e.end_ns,
            frame_hz: e.frame_hz,
        }
    }
}

/// Parse `episodes.toml` content into a list of [`EpisodeSpec`]s.
pub fn parse_spec_str(content: &str) -> Result<Vec<EpisodeSpec>> {
    let file: SpecFile = toml::from_str(content).map_err(|e| Error::Spec(e.to_string()))?;
    Ok(file.episode.into_iter().map(EpisodeSpec::from).collect())
}

/// Load and parse an `episodes.toml` file from disk.
pub fn load_spec_file(path: &Path) -> Result<Vec<EpisodeSpec>> {
    let content = std::fs::read_to_string(path)?;
    parse_spec_str(&content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multi_episode_spec() {
        let toml = r#"
[[episode]]
name = "a"
start_ns = 0
end_ns = 1_000_000_000
frame_hz = 50

[[episode.series]]
column = "joint0.pos"
id = 7

[[episode.series]]
column = "joint1.pos"
id = 8

[[episode]]
name = "b"
start_ns = 10
end_ns = 20

[[episode.series]]
column = "cam"
id = 1
"#;
        let specs = parse_spec_str(toml).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "a");
        assert_eq!(specs[0].frame_hz, Some(50));
        assert_eq!(specs[0].series, vec![("joint0.pos".to_string(), 7), ("joint1.pos".to_string(), 8)]);
        assert_eq!(specs[1].frame_hz, None);
        assert_eq!(specs[1].start, 10);
        assert_eq!(specs[1].end, 20);
    }

    #[test]
    fn rejects_malformed_toml() {
        assert!(parse_spec_str("not = [valid").is_err());
    }
}
