//! rti-query：查询引擎。
//!
//! - 谓词下推：[`Pred`] 在存储层 decode 循环内生效（不物化不满足的点），
//!   并可结合 segment zone map 直接跳过整段；
//! - 聚合：[`Agg`]（min/max/sum/avg/count）单遍累加，O(1) 额外内存；
//! - 扫描：[`scan`] 返回零拷贝语义的惰性迭代器。
//!
//! 为避免 `rti-query ↔ rti-db` 循环依赖，扫描目标抽象为 [`ScanSource`]
//! trait；门面 crate rti-db 为 `Db` 实现该 trait，并提供与 SPEC §3
//! 逐字一致的 `scan(&Db, ...)` 自由函数。

#![forbid(unsafe_code)]

use rti_core::{Result, Sample, SeriesId, Timestamp};

/// 值谓词。
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
    /// 判断值是否满足谓词。
    #[inline]
    pub fn matches(&self, v: f64) -> bool {
        match self {
            Pred::Gt(x) => v > *x,
            Pred::Lt(x) => v < *x,
            Pred::Between(lo, hi) => v >= *lo && v <= *hi,
        }
    }

    /// zone map 级下推：给定段值域 `[min,max]`，是否可能存在匹配。
    #[inline]
    pub fn zone_may_match(&self, min: f64, max: f64) -> bool {
        match self {
            Pred::Gt(x) => max > *x,
            Pred::Lt(x) => min < *x,
            Pred::Between(lo, hi) => max >= *lo && min <= *hi,
        }
    }
}

/// 聚合算子。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    /// 最小值。
    Min,
    /// 最大值。
    Max,
    /// 求和。
    Sum,
    /// 平均。
    Avg,
    /// 计数。
    Count,
}

impl Agg {
    /// 对样本集单遍聚合；空集返回 `None`。
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

/// 扫描数据源抽象（由 rti-db 的 `Db` 实现）。
///
/// 实现方负责：memtable + segment 合并、zone map 跳过、
/// 以及把 `pred` 下推到 decode 循环。
pub trait ScanSource {
    /// 收集 `series` 在 `[t0, t1]` 内满足 `pred` 的样本（按 ts 有序）到 `out`。
    fn collect(
        &self,
        series: SeriesId,
        t0: Timestamp,
        t1: Timestamp,
        pred: Option<Pred>,
        out: &mut Vec<Sample>,
    ) -> Result<()>;
}

/// 扫描 `series` 在 `[t0, t1]` 内的样本。
///
/// - `pred` 为值谓词（下推到存储层）；
/// - `agg` 为 `Some` 时返回恰好一个样本的迭代器：
///   `ts = t0`、`value = 聚合结果`（空集则不产出任何样本）；
/// - 否则按 ts 升序产出全部匹配样本。
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
            Some(v) => Ok(Box::new(std::iter::once(Sample { ts: t0, value: v }))),
            None => Ok(Box::new(std::iter::empty())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 内存 mock 数据源，验证 scan 语义（真实 Db 的测试在 rti-db）。
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
        assert!(!gt.zone_may_match(0.0, 4.0)); // 整段跳过
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
        // ts 100..290 → i ∈ 10..=29，再过滤 value>15 → i ∈ 16..=29
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

        // 空区间 + 聚合 → 无样本
        let empty: Vec<Sample> = scan(&m, 0, 5000, 9000, None, Some(Agg::Count))
            .unwrap()
            .collect();
        assert!(empty.is_empty());
    }
}
