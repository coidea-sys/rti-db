//! LeRobot-layout Parquet episode sink.
//!
//! Layout written under the dataset root:
//!
//! ```text
//! <root>/
//!   data/chunk-000/episode_XXXXXX.parquet   # timestamp (Int64, ns) + one Float64 column per series
//!   meta/info.json                          # dataset/episode info (name, fps, features, frames)
//!   meta/stats.json                         # per-column stats + per-frame original sample counts
//!   meta/episodes.jsonl                     # one line per exported episode
//! ```

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow2::array::{Array, Float64Array, Int64Array};
use arrow2::chunk::Chunk as ArrowChunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator,
    Version, WriteOptions,
};

use crate::{Chunk, EpisodeSink, EpisodeSpec, Error, Result};

/// Chunk (row-group shard) directory index, fixed to `chunk-000` for single-episode exports.
const CHUNK_DIR: &str = "chunk-000";

/// Running per-column statistics (NaN values are excluded from min/max/mean).
#[derive(Default)]
struct ColStats {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
    total: u64,
}

impl ColStats {
    fn update(&mut self, values: &[f64]) {
        for &v in values {
            self.total += 1;
            if v.is_nan() {
                continue;
            }
            if self.count == 0 || v < self.min {
                self.min = v;
            }
            if self.count == 0 || v > self.max {
                self.max = v;
            }
            self.sum += v;
            self.count += 1;
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "min": if self.count > 0 { self.min } else { f64::NAN },
            "max": if self.count > 0 { self.max } else { f64::NAN },
            "mean": if self.count > 0 { self.sum / self.count as f64 } else { f64::NAN },
            "count": self.count,
            "frames": self.total,
        })
    }
}

/// [`EpisodeSink`] writing the LeRobot dataset layout (see module docs).
pub struct LeRobotParquetSink {
    root: PathBuf,
    episode_index: u32,
    parquet_path: PathBuf,
    /// Episode name (metadata only).
    name: String,
    frame_hz: Option<u32>,
    columns: Vec<String>,
    schema: Schema,
    options: WriteOptions,
    encodings: Vec<Vec<Encoding>>,
    writer: Option<FileWriter<File>>,
    frames: u64,
    frame_samples: Vec<u32>,
    series_samples: Vec<u64>,
    stats: Vec<ColStats>,
}

impl LeRobotParquetSink {
    /// Create a sink for episode `episode_index` of the dataset at `root`
    /// (creates `data/chunk-000/` and `meta/`; the index formats as `episode_000000`).
    pub fn new(root: impl Into<PathBuf>, episode_index: u32, spec: &EpisodeSpec) -> Result<Self> {
        let root = root.into();
        let data_dir = root.join("data").join(CHUNK_DIR);
        let meta_dir = root.join("meta");
        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&meta_dir)?;
        let parquet_path = data_dir.join(format!("episode_{episode_index:06}.parquet"));

        let mut fields = vec![Field::new("timestamp", DataType::Int64, false)];
        for (name, _) in &spec.series {
            fields.push(Field::new(name, DataType::Float64, true));
        }
        let schema = Schema::from(fields);
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Snappy,
            version: Version::V2,
            data_pagesize_limit: None,
        };
        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect::<Vec<_>>();
        let file = File::create(&parquet_path)?;
        let writer = FileWriter::try_new(file, schema.clone(), options)?;

        let n = spec.series.len();
        Ok(Self {
            root,
            episode_index,
            parquet_path,
            name: spec.name.clone(),
            frame_hz: spec.frame_hz,
            columns: spec.series.iter().map(|(n, _)| n.clone()).collect(),
            schema,
            options,
            encodings,
            writer: Some(writer),
            frames: 0,
            frame_samples: Vec::new(),
            series_samples: vec![0; n],
            stats: (0..n).map(|_| ColStats::default()).collect(),
        })
    }

    fn write_meta(&self) -> Result<()> {
        let meta_dir = self.root.join("meta");

        let features = self
            .columns
            .iter()
            .map(|c| (c.clone(), serde_json::json!({ "dtype": "float64", "shape": [1] })))
            .chain(std::iter::once((
                "timestamp".to_string(),
                serde_json::json!({ "dtype": "int64", "shape": [1] }),
            )))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let info = serde_json::json!({
            "name": self.name,
            "fps": self.frame_hz,
            "total_frames": self.frames,
            "features": features,
        });
        fs::write(meta_dir.join("info.json"), serde_json::to_string_pretty(&info)?)?;

        let columns = self
            .columns
            .iter()
            .zip(&self.stats)
            .map(|(c, s)| (c.clone(), s.to_json()))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let series_samples = self
            .columns
            .iter()
            .cloned()
            .zip(self.series_samples.iter().copied())
            .map(|(c, n)| (c, n.into()))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let stats = serde_json::json!({
            "episode": self.name,
            "frame_hz": self.frame_hz,
            "frames": self.frames,
            "columns": columns,
            "series_samples": series_samples,
            // per-frame count of original samples folded into each frame (LVCF accounting).
            "frame_samples": self.frame_samples,
        });
        fs::write(meta_dir.join("stats.json"), serde_json::to_string_pretty(&stats)?)?;

        let episode = serde_json::json!({
            "episode_index": self.episode_index,
            "name": self.name,
            "length": self.frames,
        });
        let mut line = serde_json::to_string(&episode)?;
        line.push('\n');
        fs::write(meta_dir.join("episodes.jsonl"), line)?;
        Ok(())
    }
}

impl EpisodeSink for LeRobotParquetSink {
    fn write_chunk(&mut self, cols: &Chunk) -> Result<()> {
        if cols.ts.is_empty() {
            return Ok(());
        }
        if cols.cols.len() != self.columns.len() {
            return Err(Error::Spec(format!(
                "chunk has {} columns, sink expects {}",
                cols.cols.len(),
                self.columns.len()
            )));
        }
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(cols.cols.len() + 1);
        arrays.push(Arc::new(Int64Array::from_vec(cols.ts.clone())));
        for (i, (_, values)) in cols.cols.iter().enumerate() {
            if values.len() != cols.ts.len() {
                return Err(Error::Spec(format!(
                    "column {i}: {} values for {} frames",
                    values.len(),
                    cols.ts.len()
                )));
            }
            self.stats[i].update(values);
            arrays.push(Arc::new(Float64Array::from_vec(values.clone())));
        }
        let arrow_chunk = ArrowChunk::new(arrays);
        let row_groups = RowGroupIterator::try_new(
            std::iter::once(Ok(arrow_chunk)),
            &self.schema,
            self.options,
            self.encodings.clone(),
        )?;
        let writer = self.writer.as_mut().ok_or_else(|| Error::Parquet("sink already closed".into()))?;
        for group in row_groups {
            writer.write(group?)?;
        }
        self.frames += cols.ts.len() as u64;
        self.frame_samples.extend_from_slice(&cols.frame_samples);
        for (i, n) in cols.series_samples.iter().enumerate() {
            self.series_samples[i] += n;
        }
        Ok(())
    }

    fn close(mut self) -> Result<PathBuf> {
        if let Some(mut writer) = self.writer.take() {
            writer.end(None)?;
        }
        self.write_meta()?;
        Ok(self.parquet_path.clone())
    }
}

/// Convenience: dataset-relative episode Parquet path for `episode_index`
/// (`data/chunk-000/episode_XXXXXX.parquet`).
pub fn episode_parquet_path(root: &Path, episode_index: u32) -> PathBuf {
    root.join("data")
        .join(CHUNK_DIR)
        .join(format!("episode_{episode_index:06}.parquet"))
}
