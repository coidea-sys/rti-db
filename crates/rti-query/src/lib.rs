//! rti-query: query engine.
//!
//! - predicate pushdown: [`Pred`] takes effect inside the storage layer's decode loop
//!   (non-matching points are never materialized), and can combine with segment zone maps to skip whole segments;
//! - aggregation: [`Agg`] (min/max/sum/avg/count) accumulates in a single pass with O(1) extra memory;
//! - scanning: [`scan`] returns a lazy iterator with zero-copy semantics.
//!
//! To avoid an `rti-query <-> rti-db` dependency cycle, the scan target is abstracted as the
//! [`ScanSource`] trait; the facade crate rti-db implements it for `Db` and provides a
//! `scan(&Db, ...)` free function verbatim per SPEC §3.
//!
//! ## no_std (v0.9)
//!
//! With the default `std` feature disabled this crate is `no_std` (`core` + `alloc`);
//! the [`Pred`] / [`Agg`] / [`ScanSource`] / [`scan`] APIs are identical in both modes.

#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

use rti_core::{Result, Sample, SeriesId, Timestamp};

/// Value predicate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Pred {
    /// value > x
    Gt(f64),
    /// value < x
    Lt(f64),
    /// lo <= value <= hi
    Between(f64, f64),
}

impl Pred {
    /// Test whether a value satisfies the predicate.
    #[inline]
    pub fn matches(&self, v: f64) -> bool {
        match self {
            Pred::Gt(x) => v > *x,
            Pred::Lt(x) => v < *x,
            Pred::Between(lo, hi) => v >= *lo && v <= *hi,
        }
    }

    /// Zone-map-level pushdown: given a segment value range `[min,max]`, could a match exist?
    #[inline]
    pub fn zone_may_match(&self, min: f64, max: f64) -> bool {
        match self {
            Pred::Gt(x) => max > *x,
            Pred::Lt(x) => min < *x,
            Pred::Between(lo, hi) => max >= *lo && min <= *hi,
        }
    }
}

/// Aggregation operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    /// Minimum.
    Min,
    /// Maximum.
    Max,
    /// Sum.
    Sum,
    /// Average.
    Avg,
    /// Count.
    Count,
}

impl Agg {
    /// Single-pass aggregation over a sample set; returns `None` for the empty set.
    pub fn apply(&self, samples: &[Sample]) -> Option<f64> {
        if samples.is_empty() {
            return None;
        }
        match self {
            Agg::Min => Some(samples.iter().fold(f64::INFINITY, |a, s| a.min(s.value))),
            Agg::Max => Some(samples.iter().fold(f64::NEG_INFINITY, |a, s| a.max(s.value))),
            Agg::Sum => Some(samples.iter().map(|s| s.value).sum()),
            Agg::Avg => {
                Some(samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64)
            }
            Agg::Count => Some(samples.len() as f64),
        }
    }
}

/// Scan data-source abstraction (implemented by rti-db's `Db`).
///
/// Implementors are responsible for: memtable + segment merging, zone-map skipping,
/// and pushing `pred` down into the decode loop.
pub trait ScanSource {
    /// Collect samples of `series` within `[t0, t1]` satisfying `pred` (ordered by ts) into `out`.
    fn collect(
        &self,
        series: SeriesId,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<Pred>,
        out: &mut Vec<Sample>,
    ) -> Result<()>;
}

/// Scan samples of `series` within `[t0, t1]`.
///
/// - `pred` is a value predicate (pushed down to the storage layer);
/// - when `agg` is `Some`, returns an iterator of exactly one sample:
///   `ts = t0`, `value = aggregate result` (no samples produced for an empty set);
/// - otherwise yields all matching samples in ascending ts order.
pub fn scan<S: ScanSource + ?Sized>(
    db: &S,
    series: SeriesId,
    t0: Timestamp,
    t1: Timestamp,
    pred: Option<Pred>,
    agg: Option<Agg>,
) -> Result<Box<dyn Iterator<Item = Sample> + '_>> {
    let mut buf = Vec::new();
    db.collect(series, t0, t1, pred, &mut buf)?;
    match agg {
        None => Ok(Box::new(buf.into_iter())),
        Some(a) => match a.apply(&buf) {
            Some(v) => Ok(Box::new(core::iter::once(Sample { ts: t0, value: v }))),
            None => Ok(Box::new(core::iter::empty())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory mock data source validating scan semantics (tests against the real Db live in rti-db).
    struct Mock(Vec<Sample>);

    impl ScanSource for Mock {
        fn collect(
            &self,
            _series: SeriesId,
            t0: Timestamp,
            t1: Timestamp,
            pred: Option<Pred>,
            out: &mut Vec<Sample>,
        ) -> Result<()> {
            out.extend(
                self.0
                    .iter()
                    .copied()
                    .filter(|s| s.ts >= t0 && s.ts <= t1)
                    .filter(|s| pred.map(|p| p.matches(s.value)).unwrap_or(true)),
            );
            Ok(())
        }
    }

    fn mock() -> Mock {
        Mock((0..100).map(|i| Sample::new(i * 10, i as f64)).collect())
    }

    #[test]
    fn pred_matches_and_zone() {
        let gt = Pred::Gt(5.0);
        assert!(gt.matches(6.0));
        assert!(!gt.matches(5.0));
        assert!(gt.zone_may_match(0.0, 10.0));
        assert!(!gt.zone_may_match(0.0, 4.0)); // skip the whole segment
        let bt = Pred::Between(2.0, 3.0);
        assert!(bt.matches(2.5));
        assert!(!bt.matches(3.5));
        assert!(!bt.zone_may_match(4.0, 9.0));
    }

    #[test]
    fn agg_apply_all_ops() {
        let s = vec![
            Sample::new(0, 1.0),
            Sample::new(1, 2.0),
            Sample::new(2, 3.0),
        ];
        assert_eq!(Agg::Min.apply(&s), Some(1.0));
        assert_eq!(Agg::Max.apply(&s), Some(3.0));
        assert_eq!(Agg::Sum.apply(&s), Some(6.0));
        assert_eq!(Agg::Avg.apply(&s), Some(2.0));
        assert_eq!(Agg::Count.apply(&s), Some(3.0));
        assert_eq!(Agg::Min.apply(&[]), None);
    }

    #[test]
    fn scan_range_and_pred() {
        let m = mock();
        let got: Vec<Sample> = scan(&m, 0, 100, 290, Some(Pred::Gt(15.0)), None)
            .unwrap()
            .collect();
        // ts 100..290 -> i in 10..=29, then filter value>15 -> i in 16..=29
        assert_eq!(got.len(), 14);
        assert!(got.iter().all(|s| s.value > 15.0 && s.ts >= 100 && s.ts <= 290));
        assert!(got.windows(2).all(|w| w[0].ts <= w[1].ts));
    }

    #[test]
    fn scan_with_agg_yields_single_sample() {
        let m = mock();
        let got: Vec<Sample> = scan(&m, 0, 0, 990, None, Some(Agg::Avg)).unwrap().collect();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].ts, 0);
        assert_eq!(got[0].value, 49.5);

        // empty range + aggregation -> no samples
        let empty: Vec<Sample> = scan(&m, 0, 5000, 9000, None, Some(Agg::Count))
            .unwrap()
            .collect();
        assert!(empty.is_empty());
    }
}
