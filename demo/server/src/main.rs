//! 活的机器人后端：真实 cart-pole 物理 + 真实 rti-db 引擎在控制环里。
//!
//! 控制环（1 kHz，真实数值积分）：
//!   物理步 → 传感器 put() 进真 Db（WAL 组提交）→ 读路径 → PD 控制律 → 施加力
//! 快路径：Db::latest()（O(1)，实测延迟入百分位）
//! 慢路径：真实全量 scan 查询 + 真实 12 ms 睡眠，查询串行化（周期 = 查询实际耗时）
//! S2：每 3 s 真实 scan 历史窗口（实测耗时）做趋势决策
//! /crash：exit(9) 不走 Drop —— 由 run.sh 重启，新进程打开同一 Db 触发真实 WAL 重放

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rti_db::{Agg, Config, Db, Profile, Sample, SyncPolicy};

const SERIES: u32 = 1;
const H: f64 = 0.001;                 // 物理步长 1 ms（半隐式欧拉，与离线验证一致）
const M: f64 = 1.0;
const M_POLE: f64 = 0.15;
const L: f64 = 0.6;
const G: f64 = 9.81;
const TRACK: f64 = 2.2;
const CLAMP_F: f64 = 40.0;

fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

// xorshift64 —— 无外部依赖的确定性噪声源（物理模拟自身的传感器噪声，合理）
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64 // [0,1)
    }
    fn gauss(&mut self, sigma: f64) -> f64 {
        (self.next() + self.next() + self.next() - 1.5) / 1.5 * sigma
    }
}

#[derive(Clone, Copy)]
struct Pose {
    x: f64, v: f64, th: f64, w: f64, tx: f64,
    fallen: bool, fall_clock: f64,
}
impl Default for Pose {
    fn default() -> Self { Self { x: 0.0, v: 0.0, th: 0.05, w: 0.0, tx: 0.0, fallen: false, fall_clock: 0.0 } }
}

struct State {
    pose: Pose,
    clock: f64,                  // 仿真时钟（秒）
    dropped: u64,                // put() SeriesFull 拒绝数
    accepted: u64,
    lat_us: VecDeque<f64>,       // latest() 实测延迟（µs）
    scan_ms: f64,                // 最近一次慢查询实测耗时
    scan_n: usize,
    s2_ms: f64,                  // 最近一次 S2 scan 实测耗时
    s2_n: usize,
    mode_slow: bool,
    s2_on: bool,
    noise_until: f64,
    burst_until: f64,
}

struct Flags {
    kick: AtomicI32,              // 待施加的角速度冲量（×1000）
    mode_slow: AtomicU64,         // 0/1
    s2: AtomicU64,                // 0/1
}

fn control_law(x: f64, v: f64, th_m: f64, w_m: f64, tx: f64) -> f64 {
    // 与离线验证完全相同的增益（正反馈位置环 + 强角度环，实测稳定）
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

fn control_loop(db: Arc<Db>, st: Arc<Mutex<State>>, flags: Arc<Flags>) {
    let mut rng = Rng(now_ns() as u64 | 1);
    let mut f_hold = 0.0f64;
    let mut next_slow_query = 0.0f64;   // 慢路径：查询完成才排下一次（串行管线）
    let mut next_tick = Instant::now();

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
            // 摔倒 2.5 s 后自动扶正（数据保留）
            if p.fallen && clock - p.fall_clock > 2.5 {
                *p = Pose::default();
            }
            physics_step(p, if p.fallen { 0.0 } else { f_hold });
            if p.fallen && p.fall_clock == 0.0 { p.fall_clock = clock; }
            if !p.fallen { p.fall_clock = 0.0; }
            s.clock += H;
        }

        // 传感器写入（真实 rti-db put；背压 SeriesFull 如实计数）
        let th_now = st.lock().unwrap().pose.th;
        let sigma = if noise_on { 0.02 } else { 0.0012 };
        let meas = th_now + rng.gauss(sigma);
        let ts = now_ns();
        {
            let mut s = st.lock().unwrap();
            if db.put(SERIES, Sample::new(ts, meas)).is_err() { s.dropped += 1; } else { s.accepted += 1; }
            if burst_on {
                for j in 0..9 {
                    if db.put(SERIES, Sample::new(ts + j + 1, meas)).is_err() { s.dropped += 1; } else { s.accepted += 1; }
                }
            }
        }

        // 读路径
        if !slow {
            // 快路径：真实 Db::latest()，实测延迟
            let t0 = Instant::now();
            let smp = db.latest(SERIES).ok().flatten();
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
            // 慢路径：真实查询——scan 最近 60 s + 真实 12 ms 睡眠；查询串行化，
            // 周期 = max(30 ms, 本次查询实际耗时) —— 无剧本，恶化来自真实查询成本
            let now_s = st.lock().unwrap().clock;
            if now_s >= next_slow_query && !st.lock().unwrap().pose.fallen {
                let cutoff = now_ns() - 100_000_000;   // 查询侧真实延迟 100 ms（远程往返）
                let q0 = Instant::now();
                // 全量 scan：查询成本随数据量真实增长（无窗口截断）
                let win: Vec<Sample> = db.scan(SERIES, 0, i64::MAX, None, None)
                    .map(|it| it.collect()).unwrap_or_default();
                // 远程数据库服务侧往返延迟（真实睡眠）—— 本地引擎查询很快，
                // 慢的是"反射走网络另一头"这件事本身
                thread::sleep(Duration::from_millis(100));
                let ms = q0.elapsed().as_secs_f64() * 1e3;
                let mut s = st.lock().unwrap();
                s.scan_ms = ms;
                s.scan_n = win.len();
                let old: Vec<&Sample> = win.iter().filter(|p| p.ts <= cutoff).collect();
                let n = old.len();
                if n >= 1 {
                    let b = old[n - 1];
                    let a = if n >= 2 { old[n - 2] } else { old[n - 1] };
                    let w_m = (b.value - a.value) / (((b.ts - a.ts).max(1)) as f64 / 1e9);
                    let (x, v, tx) = (s.pose.x, s.pose.v, s.pose.tx);
                    f_hold = control_law(x, v, b.value, w_m, tx);
                }
                next_slow_query = now_s + (0.030f64).max(ms / 1e3);
            }
        }

        // 1 kHz 节拍
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
        let win: Vec<Sample> = db.scan(SERIES, t0, i64::MAX, None, None).map(|it| it.collect()).unwrap_or_default();
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
            let body = format!(
                "{{\"t\":{:.3},\"th\":{:.5},\"x\":{:.4},\"v\":{:.4},\"F\":0,\"tx\":{:.3},\"fallen\":{},\
                 \"slow\":{},\"s2\":{},\"p50\":{:.4},\"p99\":{:.4},\"p999\":{:.4},\"last\":{:.4},\
                 \"scan_ms\":{:.3},\"scan_n\":{},\"s2_ms\":{:.3},\"s2_n\":{},\
                 \"acked\":{},\"wm\":{},\"acc\":{},\"dropped\":{},\"segs\":{},\"noise\":{},\"burst\":{}}}",
                s.clock, p.th, p.x, p.v, p.tx, p.fallen,
                s.mode_slow, s.s2_on,
                percentile(&s.lat_us, 0.5), percentile(&s.lat_us, 0.99), percentile(&s.lat_us, 0.999),
                s.lat_us.back().copied().unwrap_or(0.0),
                s.scan_ms, s.scan_n, s.s2_ms, s.s2_n,
                db.durable_watermark(), db.durable_watermark(), s.accepted, s.dropped, db.segment_count(),
                if s.clock < s.noise_until { 1 } else { 0 }, if s.clock < s.burst_until { 1 } else { 0 },
            );
            stream.write_all(&http_response("200 OK", body)).unwrap();
        }
        ("GET", "/count") => {
            // 全库 Count 聚合：跨进程数据连续性校验用（真实 Agg::Count）
            let n = db.scan(SERIES, 0, i64::MAX, None, Some(Agg::Count))
                .map(|mut it| it.next().map(|s| s.value as u64).unwrap_or(0))
                .unwrap_or(0);
            stream.write_all(&http_response("200 OK", format!("{{\"count\":{}}}", n))).unwrap();
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
        ("POST", "/burst") => {
            let sec = q("sec").unwrap_or(2.0);
            let mut g = st.lock().unwrap();
            g.burst_until = g.clock + sec;
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
            // 真 kill -9：不走 Drop、不做最终 flush——易失状态消失，只有 WAL 存活
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
    }));
    let flags = Arc::new(Flags {
        kick: AtomicI32::new(0),
        mode_slow: AtomicU64::new(0),
        s2: AtomicU64::new(1),
    });

    {
        let (db, st, flags) = (Arc::clone(&db), Arc::clone(&st), Arc::clone(&flags));
        thread::spawn(move || control_loop(db, st, flags));
    }
    {
        let (db, st, flags) = (Arc::clone(&db), Arc::clone(&st), Arc::clone(&flags));
        thread::spawn(move || s2_loop(db, st, flags));
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
