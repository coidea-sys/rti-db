//! Segment：不可变列式存储文件（一个 segment 存一条序列的一段有序数据）。
//!
//! 文件格式（小端）：
//!
//! ```text
//! ┌─────────────┬─────────┬────────┬──────────────────────────┐
//! │ magic 8B    │ series  │ count  │ zone map: min_ts, max_ts │
//! │ "RTISEG01"  │ u32     │ u64    │ min_val, max_val (各 8B) │
//! ├─────────────┴─────────┴────────┴──────────────────────────┤
//! │ ts_len u64 │ ts 列 (delta-of-delta + varint)              │
//! │ val_len u64│ val 列 (XOR float)                          │
//! │ crc32 u32  │ 覆盖 magic 之后、crc 之前的全部字节         │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! 读路径：打开时一次预读入内存（预读缓冲代替 mmap，见 SPEC §1 回退条款），
//! zone map 用于跳过无关 segment；解码迭代器流式产出，逐点零分配。

use std::fs;
use std::path::Path;

use rti_core::{Error, Result, Sample, SeriesId, Timestamp};

use crate::encode::{self, TsDecoder, ValDecoder};

/// segment 文件魔数。
pub const MAGIC: &[u8; 8] = b"RTISEG01";
/// 固定头部长度：magic(8) + series(4) + count(8) + zone(32)。
const HEADER_LEN: usize = 60;

/// 段级 zone map：min/max 索引，用于读路径跳过与谓词下推。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ZoneMap {
    /// 最小时间戳。
    pub min_ts: Timestamp,
    /// 最大时间戳。
    pub max_ts: Timestamp,
    /// 最小值。
    pub min_val: f64,
    /// 最大值。
    pub max_val: f64,
    /// 采样点数。
    pub count: u64,
}

impl ZoneMap {
    /// 从有序样本构建 zone map。
    pub fn from_samples(samples: &[Sample]) -> Option<Self> {
        let first = samples.first()?;
        let mut zm = ZoneMap {
            min_ts: first.ts,
            max_ts: first.ts,
            min_val: first.value,
            max_val: first.value,
            count: samples.len() as u64,
        };
        for s in samples {
            zm.min_ts = zm.min_ts.min(s.ts);
            zm.max_ts = zm.max_ts.max(s.ts);
            zm.min_val = zm.min_val.min(s.value);
            zm.max_val = zm.max_val.max(s.value);
        }
        Some(zm)
    }

    /// 时间段 `[t0, t1]` 是否可能与本段相交。
    pub fn overlaps(&self, t0: Timestamp, t1: Timestamp) -> bool {
        self.max_ts >= t0 && self.min_ts <= t1
    }

    /// 值域谓词（`f(min,max) == false` 表示必然无匹配）是否可能命中。
    pub fn value_may_match(&self, f: impl Fn(f64, f64) -> bool) -> bool {
        f(self.min_val, self.max_val)
    }
}

/// Segment 写入器：把一条序列的有序样本编码为列式文件。
pub struct SegmentWriter;

impl SegmentWriter {
    /// 将 `samples`（按 ts 排序）写入 `path`，返回 zone map。
    ///
    /// 先写临时文件再 rename，保证崩溃时不会出现半个 segment。
    ///
    /// v0.6：rename 前对临时文件 `sync_data`、rename 后 fsync 目录——
    /// rti-db 的 WAL checkpoint 依赖「segment 落盘成功 ⇒ 数据已持久」，
    /// 否则截断 WAL 后崩溃可能丢数据。
    pub fn write(path: impl AsRef<Path>, series: SeriesId, samples: &[Sample]) -> Result<ZoneMap> {
        let zm = ZoneMap::from_samples(samples).ok_or(Error::Corrupt("empty segment".into()))?;
        let mut ts_col = Vec::new();
        let mut val_col = Vec::new();
        encode::encode_ts(samples, &mut ts_col);
        encode::encode_vals(samples, &mut val_col);

        let mut buf = Vec::with_capacity(HEADER_LEN + 16 + ts_col.len() + val_col.len() + 4);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&series.to_le_bytes());
        buf.extend_from_slice(&zm.count.to_le_bytes());
        buf.extend_from_slice(&zm.min_ts.to_le_bytes());
        buf.extend_from_slice(&zm.max_ts.to_le_bytes());
        buf.extend_from_slice(&zm.min_val.to_bits().to_le_bytes());
        buf.extend_from_slice(&zm.max_val.to_bits().to_le_bytes());
        buf.extend_from_slice(&(ts_col.len() as u64).to_le_bytes());
        buf.extend_from_slice(&ts_col);
        buf.extend_from_slice(&(val_col.len() as u64).to_le_bytes());
        buf.extend_from_slice(&val_col);
        let crc = crc32fast::hash(&buf[MAGIC.len()..]);
        buf.extend_from_slice(&crc.to_le_bytes());

        let path = path.as_ref();
        let tmp = path.with_extension("seg.tmp");
        {
            use std::io::Write;
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_data()?;
        }
        fs::rename(&tmp, path)?;
        if let Some(dir) = path.parent() {
            if let Ok(d) = fs::File::open(dir) {
                let _ = d.sync_data();
            }
        }
        Ok(zm)
    }
}

/// Segment 读取器：预读整文件，按 zone map 跳过，流式解码。
pub struct SegmentReader {
    series: SeriesId,
    zone: ZoneMap,
    /// 整个文件内容（预读缓冲）。
    buf: Vec<u8>,
    ts_range: (usize, usize),
    val_range: (usize, usize),
}

impl SegmentReader {
    /// 打开并校验一个 segment 文件。
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_bytes(fs::read(path)?)
    }

    /// 从内存字节构造（v0.4：冷分层透明读回路径）。
    ///
    /// 校验逻辑与 [`SegmentReader::open`] 完全一致。
    pub fn from_bytes(buf: Vec<u8>) -> Result<Self> {
        if buf.len() < HEADER_LEN + 4 || &buf[0..8] != MAGIC {
            return Err(Error::Corrupt("bad segment magic/len".into()));
        }
        let body_end = buf.len() - 4;
        let crc = u32::from_le_bytes(buf[body_end..].try_into().unwrap());
        if crc32fast::hash(&buf[MAGIC.len()..body_end]) != crc {
            return Err(Error::Corrupt("segment crc mismatch".into()));
        }
        let u32_at = |o: usize| -> Result<u32> {
            Ok(u32::from_le_bytes(buf.get(o..o + 4).ok_or_else(|| Error::Corrupt("truncated segment".into()))?.try_into().unwrap()))
        };
        let u64_at = |o: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(buf.get(o..o + 8).ok_or_else(|| Error::Corrupt("truncated segment".into()))?.try_into().unwrap()))
        };
        let i64_at = |o: usize| -> Result<i64> { Ok(u64_at(o)? as i64) };
        let f64_at = |o: usize| -> Result<f64> { Ok(f64::from_bits(u64_at(o)?)) };

        let series = u32_at(8)?;
        let count = u64_at(12)?;
        let zone = ZoneMap {
            min_ts: i64_at(20)?,
            max_ts: i64_at(28)?,
            min_val: f64_at(36)?,
            max_val: f64_at(44)?,
            count,
        };
        let ts_len = u64_at(52)? as usize;
        let ts_start = HEADER_LEN;
        let ts_end = ts_start.checked_add(ts_len).ok_or_else(|| Error::Corrupt("ts col overflow".into()))?;
        let val_len_off = ts_end;
        let val_len = u64_at(val_len_off)? as usize;
        let val_start = val_len_off + 8;
        let val_end = val_start.checked_add(val_len).ok_or_else(|| Error::Corrupt("val col overflow".into()))?;
        if val_end > body_end {
            return Err(Error::Corrupt("segment columns truncated".into()));
        }
        Ok(Self {
            series,
            zone,
            buf,
            ts_range: (ts_start, ts_end),
            val_range: (val_start, val_end),
        })
    }

    /// 本段的序列 id。
    pub fn series(&self) -> SeriesId {
        self.series
    }

    /// 本段的 zone map。
    pub fn zone_map(&self) -> ZoneMap {
        self.zone
    }

    /// 时间段 `[t0,t1]` 是否可能相交（zone map 跳过）。
    pub fn may_overlap(&self, t0: Timestamp, t1: Timestamp) -> bool {
        self.zone.overlaps(t0, t1)
    }

    /// 全段流式解码迭代器（零拷贝：直接读预读缓冲，逐点产出）。
    pub fn iter(&self) -> Result<DecodeIter<'_>> {
        DecodeIter::new(
            &self.buf[self.ts_range.0..self.ts_range.1],
            &self.buf[self.val_range.0..self.val_range.1],
            self.zone.count,
        )
    }

    /// 谓词下推到 decode 层：收集 `[t0,t1]` 内满足 `pred` 的样本，
    /// 不满足谓词的点在解码循环内直接丢弃、不物化。
    pub fn collect_range(
        &self,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<&dyn Fn(f64) -> bool>,
        out: &mut Vec<Sample>,
    ) -> Result<()> {
        if !self.may_overlap(t0, t1) {
            return Ok(());
        }
        for s in self.iter()? {
            if s.ts < t0 || s.ts > t1 {
                continue;
            }
            if let Some(p) = pred {
                if !p(s.value) {
                    continue;
                }
            }
            out.push(s);
        }
        Ok(())
    }
}

/// 段解码迭代器：时间戳列与值列双游标同步推进。
pub struct DecodeIter<'a> {
    ts: Option<TsDecoder<'a>>,
    val: Option<ValDecoder<'a>>,
    remaining: u64,
}

impl<'a> DecodeIter<'a> {
    fn new(ts_buf: &'a [u8], val_buf: &'a [u8], count: u64) -> Result<Self> {
        Ok(Self {
            ts: TsDecoder::new(ts_buf)?,
            val: ValDecoder::new(val_buf)?,
            remaining: count,
        })
    }
}

impl Iterator for DecodeIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        if self.remaining == 0 {
            return None;
        }
        let ts = self.ts.as_mut()?.next_ts()?;
        let value = self.val.as_mut()?.next_val()?;
        self.remaining -= 1;
        Some(Sample { ts, value })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.remaining as usize;
        (n, Some(n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-store-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn samples(n: usize) -> Vec<Sample> {
        (0..n)
            .map(|i| Sample::new(1_000_000 + i as i64 * 1_000, 20.0 + (i as f64 * 0.7).sin()))
            .collect()
    }

    /// SPEC §5 点名：压缩往返一致性测试。
    #[test]
    fn segment_compression_roundtrip() {
        let d = tmpdir("roundtrip");
        let p = d.join("s1.seg");
        let want = samples(4096);
        let zm = SegmentWriter::write(&p, 7, &want).unwrap();
        assert_eq!(zm.count, 4096);
        let r = SegmentReader::open(&p).unwrap();
        assert_eq!(r.series(), 7);
        let got: Vec<Sample> = r.iter().unwrap().collect();
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(want.iter()) {
            assert_eq!(g.ts, w.ts);
            assert_eq!(g.value.to_bits(), w.value.to_bits(), "float 必须 bit-exact");
        }
        // 压缩率检查
        let raw = want.len() * 16;
        let file = std::fs::metadata(&p).unwrap().len() as usize;
        assert!(file < raw, "file {} should beat raw {}", file, raw);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn zone_map_skip_and_pred_pushdown() {
        let d = tmpdir("zone");
        let p = d.join("s2.seg");
        let s = samples(1000); // ts 1_000_000 .. 1_999_000
        let zm = SegmentWriter::write(&p, 3, &s).unwrap();
        let r = SegmentReader::open(&p).unwrap();
        assert_eq!(r.zone_map(), zm);
        assert!(!r.may_overlap(0, 999_999), "不重叠区间必须被 zone map 跳过");
        assert!(r.may_overlap(1_500_000, 1_600_000));

        let mut out = Vec::new();
        r.collect_range(1_500_000, 1_500_999, None, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ts, 1_500_000);

        // 值谓词下推：只收集 value > 21.0 的点
        out.clear();
        let pred = |v: f64| v > 21.0;
        r.collect_range(1_000_000, 2_000_000, Some(&pred), &mut out).unwrap();
        assert!(out.iter().all(|s| s.value > 21.0));
        let expect = s.iter().filter(|s| s.value > 21.0).count();
        assert_eq!(out.len(), expect);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn corrupt_segment_rejected() {
        let d = tmpdir("corrupt");
        let p = d.join("s3.seg");
        SegmentWriter::write(&p, 1, &samples(10)).unwrap();
        let mut bytes = std::fs::read(&p).unwrap();
        let n = bytes.len();
        bytes[n - 10] ^= 0xFF; // 破坏数据区
        std::fs::write(&p, bytes).unwrap();
        assert!(matches!(SegmentReader::open(&p), Err(Error::Corrupt(_))));
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn zone_map_value_bounds() {
        let s = vec![
            Sample::new(1, -5.0),
            Sample::new(2, 10.0),
            Sample::new(3, 3.0),
        ];
        let zm = ZoneMap::from_samples(&s).unwrap();
        assert_eq!((zm.min_ts, zm.max_ts), (1, 3));
        assert_eq!((zm.min_val, zm.max_val), (-5.0, 10.0));
        assert_eq!(zm.count, 3);
        assert!(zm.overlaps(0, 1));
        assert!(!zm.overlaps(4, 9));
        assert!(zm.value_may_match(|lo, hi| hi > 100.0 || lo < 0.0));
    }
}
