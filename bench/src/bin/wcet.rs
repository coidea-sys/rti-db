//! rti-db WCET 分析工具（v0.9 主机端原型）。
//!
//! 消费 `rtidb` 基准在 `RTI_BENCH_DUMP_LATENCIES=1` 下转储的原始单点写延迟样本
//! （`latencies-rtidb-sync-<variant>-r<round>.csv`，每行一个纳秒值），输出：
//!   1. 完整分位数阶梯（p50…p99999 / max）、均值、标准差、抖动（p999−p50）；
//!   2. 阈值超限表（>1µs / >5µs / >10µs / >50µs 的经验频率）；
//!   3. **实验性** POT/GPD 尾部外推（peaks-over-threshold，矩估计），
//!      给出 1e6 / 1e7 / 1e8 次操作级别的返回水平估计。
//!
//! 重要声明：这是**主机端统计估计，不是经认证的 WCET**。真正的 WCET 上界需要：
//! 在目标硬件上测量、硬件时间戳标定（`--calibrate` 即为此预留的接口：
//! `corrected = raw * slope + offset_ns`，后续接入 TSN/PTP 硬件时基）、
//! 以及对中断/调度/缓存效应的系统性处理。本工具的输出不得作为安全认证证据。
//!
//! 用法：wcet [--dir <结果目录>] [--out <报告路径>] [--calibrate <slope> <offset_ns>]

use std::fmt::Write as _;

#[derive(Clone)]
struct Stats {
    variant: String,
    round: String,
    n: usize,
    min: u64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    p9999: u64,
    p99999: u64,
    max: u64,
    mean: f64,
    stddev: f64,
    // 超限经验频率
    ex_1us: f64,
    ex_5us: f64,
    ex_10us: f64,
    ex_50us: f64,
    // POT/GPD 估计（返回水平，纳秒）
    rl_1e6: Option<f64>,
    rl_1e7: Option<f64>,
    rl_1e8: Option<f64>,
    xi: Option<f64>,
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// POT/GPD 矩估计：阈值取 p99，对超出量拟合广义帕累托分布，
/// 返回 (xi, beta) —— 失败（样本不足/方差退化）时返回 None。
fn gpd_fit(sorted: &[u64], u: u64) -> Option<(f64, f64)> {
    let excess: Vec<f64> = sorted
        .iter()
        .filter(|&&x| x > u)
        .map(|&x| (x - u) as f64)
        .collect();
    let n = excess.len();
    if n < 50 {
        return None; // 尾部样本太少，矩估计不可靠
    }
    let mean = excess.iter().sum::<f64>() / n as f64;
    let var = excess.iter().map(|y| (y - mean) * (y - mean)).sum::<f64>() / n as f64;
    if mean <= 0.0 || var <= 0.0 {
        return None;
    }
    // 矩估计：xi = 0.5*(1 - mean^2/var), beta = 0.5*mean*(mean^2/var + 1)
    let xi = 0.5 * (1.0 - mean * mean / var);
    let beta = 0.5 * mean * (mean * mean / var + 1.0);
    Some((xi, beta))
}

/// 返回水平：平均每 N 次操作期望被超过一次的水平（纳秒）。
fn return_level(sorted: &[u64], u: u64, xi: f64, beta: f64, big_n: f64) -> Option<f64> {
    let n_u = sorted.iter().filter(|&&x| x > u).count() as f64;
    let n = sorted.len() as f64;
    if n_u <= 0.0 {
        return None;
    }
    let ratio = big_n * n_u / n;
    if ratio <= 1.0 {
        return Some(u as f64); // 该量级内预计不会超过阈值
    }
    let rl = if xi.abs() < 1e-9 {
        u as f64 + beta * ratio.ln() // 指数尾
    } else {
        u as f64 + beta / xi * (ratio.powf(xi) - 1.0)
    };
    Some(rl)
}

fn analyze(variant: &str, round: &str, mut samples: Vec<u64>) -> Stats {
    samples.sort_unstable();
    let n = samples.len();
    let mean = samples.iter().sum::<u64>() as f64 / n as f64;
    let stddev = (samples
        .iter()
        .map(|&x| {
            let d = x as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / n as f64)
        .sqrt();
    let ex = |thr: u64| samples.iter().filter(|&&x| x > thr).count() as f64 / n as f64;
    let p99 = percentile(&samples, 0.99);
    let fit = gpd_fit(&samples, p99);
    let (rl_1e6, rl_1e7, rl_1e8, xi) = match fit {
        Some((xi, beta)) => (
            return_level(&samples, p99, xi, beta, 1e6),
            return_level(&samples, p99, xi, beta, 1e7),
            return_level(&samples, p99, xi, beta, 1e8),
            Some(xi),
        ),
        None => (None, None, None, None),
    };
    Stats {
        variant: variant.into(),
        round: round.into(),
        n,
        min: samples[0],
        p50: percentile(&samples, 0.50),
        p90: percentile(&samples, 0.90),
        p99,
        p999: percentile(&samples, 0.999),
        p9999: percentile(&samples, 0.9999),
        p99999: percentile(&samples, 0.99999),
        max: *samples.last().unwrap(),
        mean,
        stddev,
        ex_1us: ex(1_000),
        ex_5us: ex(5_000),
        ex_10us: ex(10_000),
        ex_50us: ex(50_000),
        rl_1e6,
        rl_1e7,
        rl_1e8,
        xi,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |flag: &str| -> Option<String> {
        args.windows(2)
            .find(|w| w[0] == flag)
            .map(|w| w[1].clone())
    };
    let dir = get("--dir").unwrap_or_else(|| bench_harness::RESULT_DIR.to_string());
    let out_path = get("--out").unwrap_or_else(|| format!("{dir}/wcet-report.md"));
    // 标定接口：corrected = raw * slope + offset_ns（硬件时基标定预留）
    let slope: f64 = get("--calibrate")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1.0);
    let offset_ns: f64 = args
        .windows(3)
        .find(|w| w[0] == "--calibrate")
        .and_then(|w| w[2].parse().ok())
        .unwrap_or(0.0);

    let mut entries: Vec<(String, String, Vec<u64>)> = Vec::new();
    for e in std::fs::read_dir(&dir).expect("read results dir") {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        if !(name.starts_with("latencies-rtidb-sync-") && name.ends_with(".csv")) {
            continue;
        }
        // latencies-rtidb-sync-<variant>-r<round>.csv
        let stem = name
            .trim_start_matches("latencies-rtidb-sync-")
            .trim_end_matches(".csv");
        let (variant, round) = stem.rsplit_once("-r").unwrap_or((stem, "0"));
        let text = std::fs::read_to_string(format!("{dir}/{name}")).unwrap();
        let mut samples: Vec<u64> = text
            .lines()
            .filter_map(|l| l.trim().parse::<u64>().ok())
            .collect();
        if slope != 1.0 || offset_ns != 0.0 {
            for s in &mut samples {
                *s = ((*s as f64) * slope + offset_ns).max(0.0) as u64;
            }
        }
        entries.push((variant.to_string(), round.to_string(), samples));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    if entries.is_empty() {
        eprintln!("未找到 latencies-rtidb-sync-*.csv；先运行 RTI_BENCH_DUMP_LATENCIES=1 rtidb [--smoke]");
        std::process::exit(1);
    }

    let mut report = String::new();
    let _ = writeln!(
        report,
        "# rti-db WCET 分析报告（主机端原型）\n\n\
         > **重要**：本报告是主机端统计估计，**不是经认证的 WCET**。真正的 WCET 上界需要在目标\n\
         > 硬件上测量并做硬件时基标定（`--calibrate` 接口已预留），且需系统性处理中断/调度/缓存\n\
         > 效应。本报告不得作为安全认证证据。\n"
    );
    if slope != 1.0 || offset_ns != 0.0 {
        let _ = writeln!(report, "时基标定：slope={slope}, offset={offset_ns} ns\n");
    }

    let _ = writeln!(
        report,
        "## 单点写延迟分位数（ns）\n\n\
         | 变体 | 轮次 | 样本 | min | p50 | p90 | p99 | p999 | p9999 | p99999 | max |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    let mut all: Vec<Stats> = Vec::new();
    for (v, r, s) in &entries {
        let st = analyze(v, r, s.clone());
        let _ = writeln!(
            report,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            st.variant, st.round, st.n, st.min, st.p50, st.p90, st.p99, st.p999, st.p9999,
            st.p99999, st.max
        );
        all.push(st);
    }

    let _ = writeln!(
        report,
        "\n## 抖动与超限\n\n\
         | 变体 | 轮次 | 均值(ns) | 标准差 | 抖动 p999−p50 | >1µs | >5µs | >10µs | >50µs |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for st in &all {
        let _ = writeln!(
            report,
            "| {} | {} | {:.1} | {:.1} | {} | {:.4}% | {:.4}% | {:.4}% | {:.4}% |",
            st.variant,
            st.round,
            st.mean,
            st.stddev,
            st.p999.saturating_sub(st.p50),
            st.ex_1us * 100.0,
            st.ex_5us * 100.0,
            st.ex_10us * 100.0,
            st.ex_50us * 100.0
        );
    }

    let _ = writeln!(
        report,
        "\n## 实验性尾部外推（POT/GPD，阈值 = p99）\n\n\
         对超过 p99 的超出量做广义帕累托矩估计，给出「平均每 N 次操作被超过一次」的返回水平。\n\
         ξ<0 表示有界尾（短尾，估计端点有效）；ξ≈0 指数尾；ξ>0 重尾（外推不确定度大）。\n\n\
         | 变体 | 轮次 | ξ | 返回水平 @1e6 ops | @1e7 ops | @1e8 ops |\n\
         |---|---:|---:|---:|---:|---:|"
    );
    for st in &all {
        let f = |o: Option<f64>| o.map(|v| format!("{v:.0} ns")).unwrap_or("n/a".into());
        let _ = writeln!(
            report,
            "| {} | {} | {} | {} | {} | {} |",
            st.variant,
            st.round,
            st.xi.map(|x| format!("{x:.3}")).unwrap_or("n/a".into()),
            f(st.rl_1e6),
            f(st.rl_1e7),
            f(st.rl_1e8)
        );
    }

    let _ = writeln!(
        report,
        "\n## 结论与限制\n\n\
         - 以上均为**观测统计**与**模型外推**，受主机调度噪声影响（非独占核、非 RT 内核）。\n\
         - 通往形式化 WCET 的后续步骤：目标硬件实测、硬件时间戳标定（TSN/PTP 时基，接口已预留）、\n\
           静态时序分析（如 aiT/OTAWA）与测量法的证据融合。\n"
    );

    let tmp = format!("{out_path}.tmp");
    std::fs::write(&tmp, &report).unwrap();
    std::fs::rename(&tmp, &out_path).unwrap();
    eprintln!("[wcet] 报告 -> {out_path}");
    println!("{report}");
}
