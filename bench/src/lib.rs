//! 共享代码：确定性负载生成、HDR 延迟统计、结果 JSON 结构。

use serde::Serialize;
use std::fmt::Write as _;

pub const SERIES: u32 = 8;
pub const BASE_TS: i64 = 1_700_000_000_000_000_000;
pub const RESULT_DIR: &str = "/mnt/agents/output/bench-results";

/// SplitMix64 确定性伪随机。
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    pub fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    /// [0,1) f64
    pub fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// 为序列 `series` 生成 `n` 个点：ts 从 BASE_TS 起单调递增，
/// 步长 1000ns ± 500ns 抖动；value ∈ [-100,100)。
pub fn gen_series(series: u32, n: usize) -> Vec<(i64, f64)> {
    let mut rng = SplitMix64::new(0x5EED_1234_5678_9ABC ^ (series as u64).wrapping_mul(0xA0761D6478BD642F));
    let mut out = Vec::with_capacity(n);
    let mut ts = BASE_TS;
    for _ in 0..n {
        let jitter = (rng.next() % 1000) as i64; // [0,1000)
        ts += 1000 + jitter - 500; // 步长 1000 ± 500，恒正
        let v = rng.f64() * 200.0 - 100.0;
        out.push((ts, v));
    }
    out
}

/// 所有序列各 n_per 点，按轮询交错（模拟 8 序列并发写入）。
pub fn gen_interleaved(n_per: usize) -> Vec<(u32, i64, f64)> {
    let cols: Vec<Vec<(i64, f64)>> = (0..SERIES).map(|s| gen_series(s, n_per)).collect();
    let mut out = Vec::with_capacity(SERIES as usize * n_per);
    for i in 0..n_per {
        for s in 0..SERIES {
            let (ts, v) = cols[s as usize][i];
            out.push((s, ts, v));
        }
    }
    out
}

#[derive(Serialize, Clone, Copy)]
pub struct LatencyNs {
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

#[derive(Serialize)]
pub struct Workload {
    pub n: u64,
    pub series: u32,
}

#[derive(Serialize)]
pub struct BenchResult {
    pub system: String,
    pub variant: String,
    pub workload: Workload,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ns: Option<LatencyNs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_ops: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_pts_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_agg_pts_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lost_points: Option<i64>,
    pub notes: String,
}

impl BenchResult {
    pub fn new(system: &str, variant: &str, n: u64) -> Self {
        Self {
            system: system.into(),
            variant: variant.into(),
            workload: Workload { n, series: SERIES },
            latency_ns: None,
            throughput_ops: None,
            scan_pts_s: None,
            avg_agg_pts_s: None,
            disk_bytes: None,
            rss_bytes: None,
            recovery_ms: None,
            lost_points: None,
            notes: String::new(),
        }
    }
    pub fn note(&mut self, s: &str) {
        if !self.notes.is_empty() {
            self.notes.push_str("; ");
        }
        let _ = write!(self.notes, "{s}");
    }
}

/// 把结果追加到 {RESULT_DIR}/{system}.json（读取现有数组，追加，写回）。
pub fn emit(results: &[BenchResult], system: &str) {
    let path = format!("{RESULT_DIR}/{system}.json");
    let mut arr: Vec<serde_json::Value> = match std::fs::read_to_string(&path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    for r in results {
        arr.push(serde_json::to_value(r).unwrap());
    }
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&arr).unwrap()).unwrap();
    std::fs::rename(&tmp, &path).unwrap();
    eprintln!("[emit] {} 条结果 -> {}", results.len(), path);
}

/// 用 hdrhistogram 统计一组纳秒采样。
pub fn hdr_stats(samples: &[u64]) -> LatencyNs {
    let mut h = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    for &s in samples {
        h.record(s.max(1)).ok();
    }
    LatencyNs {
        p50: h.value_at_quantile(0.50),
        p99: h.value_at_quantile(0.99),
        p999: h.value_at_quantile(0.999),
        max: h.max(),
    }
}

pub fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

pub fn median_lat(rounds: &[LatencyNs]) -> LatencyNs {
    let m = |f: fn(&LatencyNs) -> u64| median(rounds.iter().map(f).map(|v| v as f64).collect()) as u64;
    LatencyNs { p50: m(|l| l.p50), p99: m(|l| l.p99), p999: m(|l| l.p999), max: m(|l| l.max) }
}

/// /proc/self/status VmRSS，字节。
pub fn self_rss() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

pub fn pid_rss(pid: u32) -> Option<u64> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// du -sb 等效（递归求和文件大小）。
pub fn dir_bytes(path: &str) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![std::path::PathBuf::from(path)];
    while let Some(p) = stack.pop() {
        if let Ok(md) = std::fs::metadata(&p) {
            if md.is_file() {
                total += md.len();
            } else if md.is_dir() {
                if let Ok(rd) = std::fs::read_dir(&p) {
                    for e in rd.flatten() {
                        stack.push(e.path());
                    }
                }
            }
        }
    }
    total
}

pub fn rm_rf(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
}

pub fn arg_flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
