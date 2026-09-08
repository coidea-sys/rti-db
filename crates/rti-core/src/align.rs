//! TSN 时间对齐（v0.2）。
//!
//! 面向 TSN（Time-Sensitive Networking）调度场景：给定纳秒网格
//! `align_ns`，把任意时间戳**向下取整**到网格边界（[`TsAligner::align`]），
//! 并给出相对网格的偏差（[`TsAligner::jitter`]，恒在 `[0, align_ns)`）。
//! 可选挂接 [`PtpProfile`]，在网格对齐前先做 PTP 时钟校正
//! （grandmaster 偏移 + 路径延迟），把从时钟读数换算到主时钟域。
//!
//! 全部算术使用 `saturating` / `rem_euclid`：负时间戳按数学下取整处理，
//! 极端值（`i64::MIN/MAX` 附近）饱和而不回绕、不 panic。

use crate::Timestamp;

/// PTP 时钟校正参数。
///
/// 约定：`grandmaster_offset_ns = slave_ts - master_ts`（从时钟超前主时钟
/// 的纳秒数），`path_delay_ns` 为单向链路延迟。校正公式：
///
/// ```text
/// master_ts ≈ slave_ts - grandmaster_offset_ns - path_delay_ns
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PtpProfile {
    /// 从时钟相对 grandmaster 的偏移（slave - master），纳秒。
    pub grandmaster_offset_ns: i64,
    /// 单向路径延迟，纳秒。
    pub path_delay_ns: i64,
}

impl PtpProfile {
    /// 构造校正参数。
    pub fn new(grandmaster_offset_ns: i64, path_delay_ns: i64) -> Self {
        Self { grandmaster_offset_ns, path_delay_ns }
    }
}

/// TSN 网格时间对齐器。`Copy`、零分配，可在热路径自由使用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TsAligner {
    /// 网格宽度（纳秒），恒 > 0。
    align_ns: i64,
    /// 可选 PTP 校正；`None` 表示原始时间戳即网格时钟域。
    ptp: Option<PtpProfile>,
}

impl TsAligner {
    /// 创建对齐器；`align_ns <= 0` 时返回 `None`（非法网格）。
    pub fn new(align_ns: i64) -> Option<Self> {
        if align_ns <= 0 {
            return None;
        }
        Some(Self { align_ns, ptp: None })
    }

    /// 创建带 PTP 校正的对齐器；`align_ns <= 0` 时返回 `None`。
    pub fn with_ptp(align_ns: i64, ptp: PtpProfile) -> Option<Self> {
        let mut a = Self::new(align_ns)?;
        a.ptp = Some(ptp);
        Some(a)
    }

    /// 网格宽度（纳秒）。
    pub fn align_ns(&self) -> i64 {
        self.align_ns
    }

    /// PTP 校正：`ts - offset - path_delay`，双向饱和（不回绕）。
    ///
    /// 未挂接 [`PtpProfile`] 时原样返回。
    pub fn correct(&self, ts: Timestamp) -> Timestamp {
        match self.ptp {
            Some(p) => ts
                .saturating_sub(p.grandmaster_offset_ns)
                .saturating_sub(p.path_delay_ns),
            None => ts,
        }
    }

    /// 向下取整到网格边界（先 PTP 校正再取整）。
    ///
    /// 使用 `rem_euclid`，负时间戳按数学 floor 处理：
    /// 网格 10 时 `align(-5) == -10`、`align(5) == 0`、`align(10) == 10`。
    pub fn align(&self, ts: Timestamp) -> Timestamp {
        let c = self.correct(ts);
        // rem ∈ [0, align_ns)；唯一的下溢情形是 c == i64::MIN 且 rem > 0
        // （此时数学上的网格点 MIN - rem 超出 i64 表示范围），
        // 用 saturating_sub 饱和到 i64::MIN，保证不回绕、不 panic。
        c.saturating_sub(c.rem_euclid(self.align_ns))
    }

    /// 相对网格边界的偏差：`ts - align(ts)`，恒在 `[0, align_ns)`。
    pub fn jitter(&self, ts: Timestamp) -> i64 {
        self.correct(ts).rem_euclid(self.align_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_positive_grid() {
        assert!(TsAligner::new(0).is_none());
        assert!(TsAligner::new(-1).is_none());
        assert!(TsAligner::new(i64::MIN).is_none());
        assert!(TsAligner::new(1).is_some());
    }

    #[test]
    fn aligns_to_grid_and_reports_jitter() {
        let a = TsAligner::new(10).unwrap();
        assert_eq!(a.align(0), 0);
        assert_eq!(a.align(7), 0);
        assert_eq!(a.align(9), 0);
        assert_eq!(a.align(10), 10);
        assert_eq!(a.align(11), 10);
        assert_eq!(a.jitter(0), 0);
        assert_eq!(a.jitter(7), 7);
        assert_eq!(a.jitter(19), 9);
    }

    /// 边界：网格整除时 align 恒等、jitter 为 0。
    #[test]
    fn grid_divisible_is_identity_with_zero_jitter() {
        let a = TsAligner::new(1_000_000).unwrap();
        for k in [-3i64, -1, 0, 1, 2, 1000] {
            let ts = k * 1_000_000;
            assert_eq!(a.align(ts), ts);
            assert_eq!(a.jitter(ts), 0);
        }
    }

    /// 边界：负时间戳按数学 floor（不是向零截断）。
    #[test]
    fn negative_timestamps_floor_not_truncate() {
        let a = TsAligner::new(10).unwrap();
        assert_eq!(a.align(-1), -10);
        assert_eq!(a.align(-5), -10);
        assert_eq!(a.align(-10), -10);
        assert_eq!(a.align(-11), -20);
        assert_eq!(a.jitter(-1), 9);
        assert_eq!(a.jitter(-10), 0);
        assert_eq!(a.jitter(-11), 9);
        // jitter 恒非负且小于网格
        for ts in [-999i64, -11, -1, 0, 3, 42, 1_000_001] {
            let j = a.jitter(ts);
            assert!((0..10).contains(&j), "jitter({ts}) = {j}");
            assert_eq!(a.align(ts) + j, a.correct(ts));
        }
    }

    /// 边界：PTP 校正溢出时饱和（i64::MIN/MAX 附近不回绕）。
    #[test]
    fn ptp_correction_saturates_on_extremes() {
        let p = PtpProfile::new(100, 50);
        let a = TsAligner::with_ptp(1, p).unwrap();
        assert_eq!(a.correct(1_000), 850);
        assert_eq!(a.correct(i64::MAX), i64::MAX - 150);
        // 欠溢饱和到 MIN，不回绕成正数
        assert_eq!(a.correct(i64::MIN), i64::MIN);
        let neg = PtpProfile::new(-100, -50); // 主时钟超前从时钟
        let b = TsAligner::with_ptp(1, neg).unwrap();
        assert_eq!(b.correct(i64::MAX), i64::MAX); // 上溢饱和
        assert_eq!(b.correct(0), 150);
    }

    #[test]
    fn ptp_then_align_uses_master_domain() {
        // 从时钟读数 105；offset=100, delay=0 → 主时钟 5 → 对齐到 0
        let a = TsAligner::with_ptp(10, PtpProfile::new(100, 0)).unwrap();
        assert_eq!(a.align(105), 0);
        assert_eq!(a.jitter(105), 5);
        assert_eq!(a.align(110), 10);
        // 极端值对齐不 panic、不回绕
        let b = TsAligner::new(3).unwrap();
        let lo = b.align(i64::MIN);
        let hi = b.align(i64::MAX);
        // MIN 的数学网格点超出 i64 范围 → 饱和到 MIN（不回绕）
        assert_eq!(lo, i64::MIN);
        // MAX 侧网格点在范围内 → 严格对齐
        assert!(hi > i64::MAX - 3); // hi is always <= i64::MAX by type
        assert_eq!(hi.rem_euclid(3), 0);
        assert_eq!(b.jitter(i64::MAX) + hi, i64::MAX);
    }
}
