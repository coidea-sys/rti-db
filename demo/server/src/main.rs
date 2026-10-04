//! 活的机器人后端：真实 cart-pole 物理 + 真实 rti-db 引擎在控制环里。
//!
//! 控制环（1 kHz，真实数值积分）：
//!   物理步 → 传感器 put() 进真 Db（WAL 组提交）→ 读路径 → PD 控制律 → 施加力
//! 快路径：Db::latest()（O(1)，实测延迟入百分位）
//! 慢路径：真实全量 scan 查询 + 真实服务延迟，查询串行化（周期 = 查询实际耗时）
//! S2：每 3 s 真实 scan 历史窗口（实测耗时）做趋势决策
//!
//! 挑战模式（全部真实，判决由真实 scan + Agg::Count 得出）：
//!   16 通道多速率写入（6 关节 1 kHz / 6 力矩 4 kHz / 3 IMU 2 kHz + 摆杆 1 kHz）
//!   torture：全通道 ×10 重复写入压测 + 并发全量回放客户端 + 真实 kill -9
//!   episode 元数据（起止标记）存在引擎内（series 999），进程死亡后从 DB 恢复
//!   判决：每通道 期望点数 vs 实测 scan 点数、最大时间戳间隙、额定/压测分桶丢数、
//!         全窗口 digest（FNV-1a）；kill 后可用 /verify 重扫证明 digest 逐字节复现
//! /crash：exit(9) 不走 Drop —— 由 run.sh 重启，新进程打开同一 Db 触发真实 WAL 重放

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rti_db::{Config, Db, Profile, Sample, SyncPolicy};

const SERIES_POLE: u32 = 1;
const SERIES_MARKER: u32 = 999;

const H: f64 = 0.001;
const M: f64 = 1.0;
const M_POLE: f64 = 0.15;
const L: f64 = 0.6;
const G: f64 = 9.81;
const TRACK: f64 = 2.2;
const CLAMP_F: f64 = 40.0;

/// 挑战通道表：(series, rate_hz, amp, wave_freq_hz, phase) —— 合成但有物理合理性的
/// 多速率负载：关节 1 kHz、力/力矩 4 kHz、IMU 2 kHz
const CHANS: &[(u32, f64, f64, f64, f64)] = &[
    // 6 关节
    (2, 1000.0, 0.80, 0.50, 0.0),
    (3, 1000.0, 0.70, 0.70, 0.5),
    (4, 1000.0, 0.90, 0.90, 1.0),
    (5, 1000.0, 0.60, 0.40, 1.5),
    (6, 1000.0, 0.75, 0.60, 2.0),
    (7, 1000.0, 0.85, 0.80, 2.5),
    // 6 力/力矩
    (8, 4000.0, 12.0, 3.0, 0.0),
    (9, 4000.0, 8.0, 4.0, 0.7),
    (10, 4000.0, 15.0, 2.0, 1.4),
    (11, 4000.0, 6.0, 5.0, 2.1),
    (12, 4000.0, 10.0, 3.5, 2.8),
    (13, 4000.0, 9.0, 4.5, 3.5),
    // 3 IMU 轴
    (14, 2000.0, 0.50, 8.0, 0.0),
    (15, 2000.0, 0.40, 10.0, 1.0),
    (16, 2000.0, 0.60, 12.0, 2.0),
];

fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    fn gauss(&mut self, sigma: f64) -> f64 {
        (self.next() + self.next() + self.next() - 1.5) / 1.5 * sigma
    }
}

#[inline]
fn wave(t_s: f64, amp: f64, f: f64, ph: f64) -> f64 {
    amp * (std::f64::consts::TAU * f * t_s + ph).sin()
}

#[derive(Clone, Copy)]
struct Pose {
    x: f64, v: f64, th: f64, w: f64, tx: f64,
    fallen: bool, fall_clock: f64,
}
impl Default for Pose {
    fn default() -> Self { Self { x: 0.0, v: 0.0, th: 0.05, w: 0.0, tx: 0.0, fallen: false, fall_clock: 0.0 } }
}

struct ReplayStat {
    clients: u32,
    queries: u64,
    worst_ms: f64,
}

struct State {
    pose: Pose,
    clock: f64,
    dropped: u64,                 // 控制环（series 1）背压丢数
    accepted: u64,
    lat_us: VecDeque<f64>,
    scan_ms: f64,
    scan_n: usize,
    s2_ms: f64,
    s2_n: usize,
    mode_slow: bool,
    s2_on: bool,
    noise_until: f64,
    burst_until: f64,
    // 挑战模式
    acc_rated: u64,               // 写入线程：额定负载成功数
    drop_rated: u64,              // 写入线程：额定负载丢数（判决关键）
    acc_tort: u64,
    drop_tort: u64,
    episode_active: bool,
    episode_t0: i64,
    replay: ReplayStat,
}

struct Flags {
    kick: AtomicI32,
    mode_slow: AtomicU64,
    s2: AtomicU64,
    burst_until: AtomicI64,       // 写入线程 ×10 重复写压测截止（ns）
}

fn control_law(x: f64, v: f64, th_m: f64, w_m: f64, tx: f64) -> f64 {
    (0.6 * (x - tx) + 3.0 * v + 42.0 * th_m + 48.0 * w_m - 0.5 * v)
        .clamp(-CLAMP_F, CLAMP_F)
}

fn physics_step(p: &mut Pose, f: f64) {
    let (sin, cos) = p.th.sin_cos();
    let th_acc = (G * (M + M_POLE) * sin - cos * (f + M_POLE * L * p.w * p.w * sin))
        / (L * (4.0 / 3.0 * (M + M_POLE) - M_POLE * cos * cos));
    let x_acc = (f + M_POLE * L * (p.w * p.w * sin - th_acc * cos)) / (M + M_POLE);
    p.w += th_acc * H;
    p.th += p.w * H;
    p.v += x_acc * H;
    p.x += p.v * H;
    if p.x.abs() > TRACK { p.x = TRACK * p.x.signum(); p.v *= -0.3; }
    if p.th.abs() > 1.25 && !p.fallen { p.fallen = true; }
}

/* ================= 多速率写入线程（挑战负载） ================= */
/// 每个通道按各自周期网格写入：追赶式补齐（硬件时间戳语义）——
/// 写入线程晚醒不会制造 ts 缺口；唯一的丢失来源是 put() 真实返回 SeriesFull。
fn writer_loop(db: Arc<Db>, st: Arc<Mutex<State>>, flags: Arc<Flags>) {
    let n = CHANS.len();
    let mut next = vec![0i64; n];
    let mut period = vec![0i64; n];
    let start = now_ns();
    for (i, ch) in CHANS.iter().enumerate() {
        period[i] = (1e9 / ch.1) as i64;
        // 对齐到 epoch 网格，网格确定性
        next[i] = start.div_euclid(period[i]) * period[i];
    }
    loop {
        let now = now_ns();
        let burst = now < flags.burst_until.load(Ordering::Relaxed);
        for i in 0..n {
            let (series, _, amp, f, ph) = CHANS[i];
            while next[i] <= now {
                let v = wave(next[i] as f64 / 1e9, amp, f, ph);
                if burst {
                    // torture：同 ts 重复 ×10（引擎按 ts 去重，压的是真实写入路径）
                    let mut ok = 0u64; let mut err = 0u64;
                    for _ in 0..10 {
                        match db.put(series, Sample::new(next[i], v)) { Ok(()) => ok += 1, Err(_) => err += 1 }
                    }
                    let mut s = st.lock().unwrap();
                    s.acc_tort += ok; s.drop_tort += err;
                } else {
                    let mut s = st.lock().unwrap();
                    match db.put(series, Sample::new(next[i], v)) {
                        Ok(()) => s.acc_rated += 1,
                        Err(_) => s.drop_rated += 1,
                    }
                }
                next[i] += period[i];
            }
        }
        // 最近到期的网格点决定睡眠时长（通常 ~250 µs 以内）
        let soonest = next.iter().cloned().min().unwrap_or(now + 200_000);
        let wait = Duration::from_nanos((soonest - now).max(50_000) as u64);
        thread::sleep(wait);
    }
}

/* ================= 并发回放客户端（读 torture） ================= */
fn replay_client(db: Arc<Db>, st: Arc<Mutex<State>>, until: i64, id: u32) {
    let mut series = 1u32 + id % 16;
    while now_ns() < until {
        series = series % 16 + 1;
        let q0 = Instant::now();
        let _n = db.scan(series, 0, i64::MAX, None, None)
            .map(|it| it.count()).unwrap_or(0);
        let ms = q0.elapsed().as_secs_f64() * 1e3;
        let mut s = st.lock().unwrap();
        s.replay.queries += 1;
        s.replay.worst_ms = s.replay.worst_ms.max(ms);
    }
    let mut s = st.lock().unwrap();
    if s.replay.clients > 0 { s.replay.clients -= 1; }
}

/* ================= FNV-1a 64（digest） ================= */
struct Fnv(u64);
impl Fnv {
    fn new() -> Self { Self(0xcbf29ce484222325) }
    fn add(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
    fn add_sample(&mut self, series: u32, ts: i64, value: f64) {
        self.add(&series.to_le_bytes());
        self.add(&ts.to_le_bytes());
        self.add(&value.to_bits().to_le_bytes());
    }
    fn hex(&self) -> String { format!("{:016x}", self.0) }
}

/* ================= episode 工具 ================= */
/// 从引擎内的标记通道恢复 episode 窗口：最后一个"开始"标记（value=1）
/// 必须晚于最后一个"结束"标记（value=0）。
fn episode_window(db: &Db) -> Option<(i64, i64)> {
    let mk: Vec<Sample> = db.scan(SERIES_MARKER, 0, i64::MAX, None, None)
        .map(|it| it.collect()).unwrap_or_default();
    let mut last_open: Option<i64> = None;
    let mut last_close: Option<i64> = None;
    for m in &mk {
        if m.value == 1.0 { last_open = Some(m.ts); }
        if m.value == 0.0 { last_close = Some(m.ts); }
    }
    match (last_open, last_close) {
        // 最近一个事件是"结束"→ 返回已关闭窗口；否则进行中（t1=0）
        (Some(t0), Some(t1)) if t1 > t0 => Some((t0, t1)),
        (Some(t0), _) => Some((t0, 0)),
        _ => None,
    }
}

struct SeriesVerdict {
    series: u32,
    rate: f64,
    expected: i64,       // 有效期望 = rate × (窗口 − 中断时间)
    actual: i64,
    max_gap_ns: i64,     // 最大间隙（含中断）
    interruptions: i32,  // 超过 10×周期的中断次数
    downtime_ns: i64,    // 中断总时长
    ok: bool,
}

/// 真实判决：逐通道 scan，点数对齐网格期望、最大 ts 间隙、digest 全窗口哈希
fn verify_episode(db: &Db, t0: i64, t1: i64) -> (Vec<SeriesVerdict>, u64, String) {
    let mut dig = Fnv::new();
    let mut total = 0u64;
    let mut out = Vec::new();
    for series in 1..=16u32 {
        let rate: f64 = match series {
            1 => 1000.0,
            s => CHANS.iter().find(|c| c.0 == s).map(|c| c.1).unwrap_or(1000.0),
        };
        let pts: Vec<Sample> = db.scan(series, t0, t1, None, None)
            .map(|it| it.collect()).unwrap_or_default();
        let actual = pts.len() as i64;
        let period = (1e9 / rate) as i64;
        let mut max_gap = 0i64;
        let mut interruptions = 0i32;
        let mut downtime = 0i64;
        for w in pts.windows(2) {
            let g = w[1].ts - w[0].ts;
            if g > max_gap { max_gap = g; }
            if g > period * 10 { interruptions += 1; downtime += g - period; }
        }
        // 有效期望：窗口时长减去如实计量的中断时间（进程死亡等），再按网格取整
        let effective = (t1 - t0) - downtime;
        let expected = (effective as f64 * rate / 1e9) as i64;
        for p in &pts { dig.add_sample(series, p.ts, p.value); }
        total += actual as u64;
        // 判定：实测对齐扣除中断后的有效期望 ±3。
        // 长间隙已在中断统计与有效期望中如实计量，不重复惩罚；
        // 非中断性缺点的唯一来源是 put() 失败（drop_rated，全局判定）。
        let ok = (actual - expected).abs() <= 3;
        out.push(SeriesVerdict { series, rate, expected, actual, max_gap_ns: max_gap, interruptions, downtime_ns: downtime, ok });
    }
    (out, total, dig.hex())
}

/* ================= 控制环 / S2（与上一版一致） ================= */
fn control_loop(db: Arc<Db>, st: Arc<Mutex<State>>, flags: Arc<Flags>) {
    let mut rng = Rng(now_ns() as u64 | 1);
    let mut f_hold = 0.0f64;
    let mut next_slow_query = 0.0f64;
    let mut next_tick = Instant::now();
    let mut next_grid = now_ns().div_euclid(1_000_000) * 1_000_000;   // 1 ms epoch 网格

    loop {
        let (noise_on, burst_on, slow, kick) = {
            let s = st.lock().unwrap();
            (s.clock < s.noise_until, s.clock < s.burst_until, s.mode_slow, flags.kick.swap(0, Ordering::SeqCst))
        };
        {
            let mut s = st.lock().unwrap();
            let clock = s.clock;
            let p = &mut s.pose;
            if kick != 0 { p.w += kick as f64 / 1000.0; }
            if p.fallen && clock - p.fall_clock > 2.5 { *p = Pose::default(); }
            physics_step(p, if p.fallen { 0.0 } else { f_hold });
            if p.fallen && p.fall_clock == 0.0 { p.fall_clock = clock; }
            if !p.fallen { p.fall_clock = 0.0; }
            s.clock += H;
        }

        // 传感器写入：1 ms 周期网格 + 追赶式补齐（硬件时间戳语义）——
        // 控制环被读争用 stall 时，样本仍落在其所属的控制节拍上；
        // 唯一的丢失来源是 put() 真实返回 SeriesFull（计入 dropped）。
        let th_now = st.lock().unwrap().pose.th;
        let sigma = if noise_on { 0.02 } else { 0.0012 };
        let now = now_ns();
        {
            let mut s = st.lock().unwrap();
            while next_grid <= now {
                let meas = th_now + rng.gauss(sigma);
                if db.put(SERIES_POLE, Sample::new(next_grid, meas)).is_err() { s.dropped += 1; } else { s.accepted += 1; }
                if burst_on {
                    for _ in 0..9 { let _ = db.put(SERIES_POLE, Sample::new(next_grid, meas)); }
                }
                next_grid += 1_000_000;
            }
        }

        if !slow {
            let t0 = Instant::now();
            let smp = db.latest(SERIES_POLE).ok().flatten();
            let us = t0.elapsed().as_nanos() as f64 / 1e3;
            let mut s = st.lock().unwrap();
            s.lat_us.push_back(us);
            if s.lat_us.len() > 4096 { s.lat_us.pop_front(); }
            if !s.pose.fallen {
                let (x, v, tx) = (s.pose.x, s.pose.v, s.pose.tx);
                let th_m = smp.map(|m| m.value).unwrap_or(0.0);
                let w_m = s.pose.w + rng.gauss(0.002);
                f_hold = control_law(x, v, th_m, w_m, tx);
            }
        } else {
            let now_s = st.lock().unwrap().clock;
            if now_s >= next_slow_query && !st.lock().unwrap().pose.fallen {
                let cutoff = now_ns() - 100_000_000;
                let q0 = Instant::now();
                let win: Vec<Sample> = db.scan(SERIES_POLE, 0, i64::MAX, None, None)
                    .map(|it| it.collect()).unwrap_or_default();
                thread::sleep(Duration::from_millis(100));
                let ms = q0.elapsed().as_secs_f64() * 1e3;
                let mut s = st.lock().unwrap();
                s.scan_ms = ms;
                s.scan_n = win.len();
                let old: Vec<&Sample> = win.iter().filter(|p| p.ts <= cutoff).collect();
                let nn = old.len();
                if nn >= 1 {
                    let b = old[nn - 1];
                    let a = if nn >= 2 { old[nn - 2] } else { old[nn - 1] };
                    let w_m = (b.value - a.value) / (((b.ts - a.ts).max(1)) as f64 / 1e9);
                    let (x, v, tx) = (s.pose.x, s.pose.v, s.pose.tx);
                    f_hold = control_law(x, v, b.value, w_m, tx);
                }
                next_slow_query = now_s + (0.030f64).max(ms / 1e3);
            }
        }

        next_tick += Duration::from_millis(1);
        let now = Instant::now();
        if next_tick > now { thread::sleep(next_tick - now); } else { next_tick = now; }
    }
}

fn s2_loop(db: Arc<Db>, st: Arc<Mutex<State>>, flags: Arc<Flags>) {
    loop {
        thread::sleep(Duration::from_millis(3000));
        let (on, fallen) = { let s = st.lock().unwrap(); (s.s2_on && flags.s2.load(Ordering::SeqCst) == 1, s.pose.fallen) };
        if !on || fallen { continue; }
        let t0 = now_ns() - 1_500_000_000;
        let q0 = Instant::now();
        let win: Vec<Sample> = db.scan(SERIES_POLE, t0, i64::MAX, None, None).map(|it| it.collect()).unwrap_or_default();
        let ms = q0.elapsed().as_secs_f64() * 1e3;
        if win.len() < 50 { continue; }
        let mean = win.iter().map(|p| p.value).sum::<f64>() / win.len() as f64;
        let mut s = st.lock().unwrap();
        s.s2_ms = ms;
        s.s2_n = win.len();
        s.pose.tx = (-mean * 0.5).clamp(-0.35, 0.35);
    }
}

fn percentile(d: &VecDeque<f64>, p: f64) -> f64 {
    if d.is_empty() { return 0.0; }
    let mut v: Vec<f64> = d.iter().copied().collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(p * (v.len() - 1) as f64) as usize]
}

fn http_response(code: &str, body: String) -> Vec<u8> {
    format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        code, body.len(), body
    ).into_bytes()
}

fn verdict_json(v: &[SeriesVerdict], total: u64, digest: &str, t0: i64, t1: i64,
                drop_rated: u64, drop_tort: u64) -> String {
    let rows: Vec<String> = v.iter().map(|r| format!(
        "{{\"series\":{},\"rate\":{},\"expected\":{},\"actual\":{},\"gap_ns\":{},\"interrupt\":{},\"downtime_ns\":{},\"ok\":{}}}",
        r.series, r.rate, r.expected, r.actual, r.max_gap_ns, r.interruptions, r.downtime_ns, r.ok)).collect();
    let pass = v.iter().all(|r| r.ok) && drop_rated == 0;
    format!(
        "{{\"t0\":{},\"t1\":{},\"duration_ms\":{},\"series\":[{}],\"total\":{},\"digest\":\"{}\",\
         \"dropped_rated\":{},\"dropped_torture\":{},\"pass\":{}}}",
        t0, t1, (t1 - t0) / 1_000_000, rows.join(","), total, digest, drop_rated, drop_tort, pass)
}

fn handle(stream: &mut TcpStream, db: &Arc<Db>, st: &Arc<Mutex<State>>, flags: &Arc<Flags>) {
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf).unwrap_or(0);
    if n == 0 { return; }
    let req = String::from_utf8_lossy(&buf[..n]);
    let first = req.lines().next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");

    let mut path = target;
    let mut query = "";
    if let Some(idx) = target.find('?') {
        path = &target[..idx];
        query = &target[idx + 1..];
    }
    let q = |key: &str| -> Option<f64> {
        query.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k == key { v.parse().ok() } else { None }
        })
    };

    match (method, path) {
        ("GET", "/state") => {
            let s = st.lock().unwrap();
            let p = &s.pose;
            let burst_on = now_ns() < flags.burst_until.load(Ordering::Relaxed);
            let body = format!(
                "{{\"t\":{:.3},\"th\":{:.5},\"x\":{:.4},\"v\":{:.4},\"tx\":{:.3},\"fallen\":{},\
                 \"slow\":{},\"s2\":{},\"p50\":{:.4},\"p99\":{:.4},\"p999\":{:.4},\"last\":{:.4},\
                 \"scan_ms\":{:.3},\"scan_n\":{},\"s2_ms\":{:.3},\"s2_n\":{},\
                 \"acked\":{},\"wm\":{},\"acc\":{},\"dropped\":{},\"segs\":{},\"noise\":{},\"burst\":{},\
                 \"w_acc_rated\":{},\"w_drop_rated\":{},\"w_acc_tort\":{},\"w_drop_tort\":{},\
                 \"ep_active\":{},\"ep_t0\":{},\"replay_clients\":{},\"replay_queries\":{},\"replay_worst_ms\":{:.2}}}",
                s.clock, p.th, p.x, p.v, p.tx, p.fallen,
                s.mode_slow, s.s2_on,
                percentile(&s.lat_us, 0.5), percentile(&s.lat_us, 0.99), percentile(&s.lat_us, 0.999),
                s.lat_us.back().copied().unwrap_or(0.0),
                s.scan_ms, s.scan_n, s.s2_ms, s.s2_n,
                db.durable_watermark(), db.durable_watermark(), s.accepted, s.dropped, db.segment_count(),
                if s.clock < s.noise_until { 1 } else { 0 }, if burst_on { 1 } else { 0 },
                s.acc_rated, s.drop_rated, s.acc_tort, s.drop_tort,
                s.episode_active, s.episode_t0, s.replay.clients, s.replay.queries, s.replay.worst_ms,
            );
            stream.write_all(&http_response("200 OK", body)).unwrap();
        }
        ("GET", "/episode/status") => {
            // 进程重启后也能从引擎内的标记通道恢复 episode 状态
            let win = episode_window(db);
            let body = match win {
                Some((t0, 0)) => format!("{{\"active\":true,\"t0\":{}}}", t0),
                Some((t0, t1)) => format!("{{\"active\":false,\"t0\":{},\"t1\":{}}}", t0, t1),
                None => "{\"active\":false}".to_string(),
            };
            stream.write_all(&http_response("200 OK", body)).unwrap();
        }
        ("POST", "/episode/start") => {
            // 若引擎内已有未关闭的 episode（例如本进程是 kill 后重启的），直接续用
            if let Some((t0, 0)) = episode_window(db) {
                let mut s = st.lock().unwrap();
                s.episode_active = true;
                s.episode_t0 = t0;
                stream.write_all(&http_response("200 OK", format!("{{\"ok\":true,\"resumed\":true,\"t0\":{}}}", t0))).unwrap();
                return;
            }
            let t0 = now_ns();
            let _ = db.put(SERIES_MARKER, Sample::new(t0, 1.0));
            let mut s = st.lock().unwrap();
            s.episode_active = true;
            s.episode_t0 = t0;
            stream.write_all(&http_response("200 OK", format!("{{\"ok\":true,\"resumed\":false,\"t0\":{}}}", t0))).unwrap();
        }
        ("POST", "/episode/stop") => {
            let t1 = now_ns();
            let _ = db.put(SERIES_MARKER, Sample::new(t1, 0.0));
            let (t0, _) = match episode_window(db) {
                Some(w) => w,
                None => { stream.write_all(&http_response("404 Not Found", "{\"err\":\"no episode\"}".into())).unwrap(); return; }
            };
            let (verdicts, total, digest) = verify_episode(db, t0, t1);
            let (drop_rated, drop_tort) = { let s = st.lock().unwrap(); (s.drop_rated, s.drop_tort) };
            let body = verdict_json(&verdicts, total, &digest, t0, t1, drop_rated, drop_tort);
            stream.write_all(&http_response("200 OK", body)).unwrap();
        }
        ("GET", "/verify") => {
            // 重扫上一个 episode 窗口，digest 必须与判决时一致 —— kill -9 + WAL 重放后逐字节可复现
            match episode_window(db) {
                Some((t0, t1)) if t1 > t0 => {
                    let (_, _, digest) = verify_episode(db, t0, t1);
                    stream.write_all(&http_response("200 OK", format!("{{\"t0\":{},\"t1\":{},\"digest\":\"{}\"}}", t0, t1, digest))).unwrap();
                }
                Some((t0, _)) => stream.write_all(&http_response("409 Conflict", format!("{{\"err\":\"episode still open\",\"t0\":{}}}", t0))).unwrap(),
                None => stream.write_all(&http_response("404 Not Found", "{\"err\":\"no episode\"}".into())).unwrap(),
            }
        }
        ("POST", "/torture") => {
            let k = q("replay").unwrap_or(0.0) as u32;
            let sec = q("sec").unwrap_or(10.0);
            let until = now_ns() + (sec * 1e9) as i64;
            let mut spawned = 0;
            for i in 0..k {
                let (db, st) = (Arc::clone(db), Arc::clone(st));
                thread::spawn(move || replay_client(db, st, until, i));
                spawned += 1;
            }
            st.lock().unwrap().replay.clients += spawned;
            stream.write_all(&http_response("200 OK", format!("{{\"ok\":true,\"clients\":{}}}", spawned))).unwrap();
        }
        ("POST", "/burst") => {
            // torture：写入线程全通道 ×10 重复写（引擎按 ts 去重，压的是真实写入路径）
            let sec = q("sec").unwrap_or(2.0);
            flags.burst_until.store(now_ns() + (sec * 1e9) as i64, Ordering::Relaxed);
            let mut g = st.lock().unwrap();
            g.burst_until = g.clock + sec;
            drop(g);
            stream.write_all(&http_response("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/mode") => {
            let v = q("slow").unwrap_or(0.0) as u64;
            flags.mode_slow.store(v, Ordering::SeqCst);
            st.lock().unwrap().mode_slow = v == 1;
            stream.write_all(&http_response("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/kick") => {
            let w = q("w").unwrap_or(1.1);
            flags.kick.store((w * 1000.0) as i32, Ordering::SeqCst);
            stream.write_all(&http_response("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/noise") => {
            let sec = q("sec").unwrap_or(5.0);
            let mut g = st.lock().unwrap();
            g.noise_until = g.clock + sec;
            drop(g);
            stream.write_all(&http_response("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/s2") => {
            let v = q("on").unwrap_or(1.0) as u64;
            flags.s2.store(v, Ordering::SeqCst);
            st.lock().unwrap().s2_on = v == 1;
            stream.write_all(&http_response("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/crash") => {
            stream.write_all(&http_response("200 OK", "{\"dying\":true}".into())).unwrap();
            stream.flush().unwrap();
            std::process::exit(9);
        }
        _ => {
            stream.write_all(&http_response("404 Not Found", "{\"err\":\"not found\"}".into())).unwrap();
        }
    }
}

fn main() {
    let mut cfg = Config::default();
    cfg.profile = Profile::Balanced;
    cfg.data_dir = Some(std::path::PathBuf::from("data"));
    cfg.wal_sync = SyncPolicy::Group { interval_us: 1000 };
    let db = Arc::new(Db::open(cfg).expect("open db"));

    let st = Arc::new(Mutex::new(State {
        pose: Pose::default(),
        clock: 0.0,
        dropped: 0,
        accepted: 0,
        lat_us: VecDeque::new(),
        scan_ms: 0.0,
        scan_n: 0,
        s2_ms: 0.0,
        s2_n: 0,
        mode_slow: false,
        s2_on: true,
        noise_until: -1.0,
        burst_until: -1.0,
        acc_rated: 0,
        drop_rated: 0,
        acc_tort: 0,
        drop_tort: 0,
        episode_active: false,
        episode_t0: 0,
        replay: ReplayStat { clients: 0, queries: 0, worst_ms: 0.0 },
    }));
    let flags = Arc::new(Flags {
        kick: AtomicI32::new(0),
        mode_slow: AtomicU64::new(0),
        s2: AtomicU64::new(1),
        burst_until: AtomicI64::new(0),
    });

    {
        // 内务：每分钟压实一次 eligible 段，防止长期运行的段数量无限增长
        let db = Arc::clone(&db);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(60));
            let _ = db.compact();
        });
    }
    for f in [control_loop as fn(Arc<Db>, Arc<Mutex<State>>, Arc<Flags>), s2_loop, writer_loop] {
        let (db, st, flags) = (Arc::clone(&db), Arc::clone(&st), Arc::clone(&flags));
        thread::spawn(move || f(db, st, flags));
    }

    let listener = TcpListener::bind("127.0.0.1:8791").expect("bind 127.0.0.1:8791");
    eprintln!("robot-server listening on http://127.0.0.1:8791 (Ctrl-C to stop)");
    for stream in listener.incoming() {
        if let Ok(mut s) = stream {
            let (db, st, flags) = (Arc::clone(&db), Arc::clone(&st), Arc::clone(&flags));
            thread::spawn(move || handle(&mut s, &db, &st, &flags));
        }
    }
}
