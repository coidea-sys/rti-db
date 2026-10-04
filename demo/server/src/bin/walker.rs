//! 行走机器人后端：2D LIPM（线性倒立摆，Kajita 经典模型）+ 捕获点落脚。
//! 与 cart-pole 版同一架构：控制环 1 kHz，状态读写全部经过真实 rti-db。
//!
//! 传感序列（1 kHz，网格时间戳+追赶补齐）：1=CoM x · 2=CoM vx · 3=支撑反力
//!   4=步态相位(0支撑/1摆动) · 5=稳定裕度 |x−p|
//! S1 反射（mode=fast）：每拍从 Db::latest() 读状态，触发阈值 0.12（反应快）
//! S1 断开（mode=slow）：每 200 ms 真实 scan 最近两点，数据陈旧 200 ms + 阈值 0.17
//! S2：每 3 s 真实 scan 窗口，按平均速度误差调整期望速度 XD
//! 判决/episode/torture 与 cart-pole 版同构（合成通道 series 20-27）

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rti_db::{Config, Db, Profile, Sample, SyncPolicy};

const MARKER: u32 = 999;
const G: f64 = 9.81;
const _ZC: f64 = 0.85;
const LAM: f64 = 3.3995; // sqrt(9.81/0.85)
const M: f64 = 12.0;

fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

// 离线验证锁定的参数（node 网格搜索：平地双稳，推力下 on 0/10 vs off 7/10）
const TON: f64 = 0.12;   // S1 触发阈值（快）
const TOFF: f64 = 0.16;  // 慢路径触发阈值
const STALE_S: f64 = 0.20;
const PUSH_V: f64 = 0.45;

struct WState {
    // 物理状态
    x: f64, vx: f64, p: f64,
    phase_swing: bool, t_sw: f64, p_target: f64, p_from: f64,
    x_d: f64,            // S2 调节的期望速度
    fallen: bool, fall_clock: f64, clock: f64,
    steps: u64, dist: f64,
    // 读路径实测
    lat_us: VecDeque<f64>,
    scan_ms: f64,
    s2_ms: f64,
    slow: bool,
    s2_on: bool,
    // 挑战
    acc_rated: u64, drop_rated: u64, acc_tort: u64, drop_tort: u64,
    episode_active: bool, episode_t0: i64,
    replay_clients: u32, replay_queries: u64, replay_worst_ms: f64,
    burst_on: bool,
}

struct WFlags {
    kick: AtomicI32,          // ×1000
    push: AtomicBool,         // 自动扰动开关
    slow: AtomicBool,
    s2: AtomicBool,
    burst_until: AtomicI64,
}

fn lipm_step(x: f64, vx: f64, p: f64, h: f64) -> f64 {
    vx + LAM * LAM * (x - p) * h
}

fn control_loop(db: Arc<Db>, st: Arc<Mutex<WState>>, flags: Arc<WFlags>) {
    let mut next_tick = Instant::now();
    let mut next_grid = now_ns().div_euclid(1_000_000) * 1_000_000;
    let mut last_slow_read = 0.0f64;          // 慢路径上次真实 scan 时刻（仿真钟）
    let mut fall_at = Instant::now();
    let boot_ns = now_ns();
    let mut grace_until = -1.0f64;
    let mut stale_x = 0.0f64;
    let mut stale_vx = 0.0f64;
    let mut next_push = 3.0f64;
    loop {
        let (slow, kick, _s2_on, pushing) = {
            let s = st.lock().unwrap();
            (s.slow, flags.kick.swap(0, Ordering::SeqCst), s.s2_on, flags.push.load(Ordering::Relaxed))
        };
        let now = now_ns();
        let mut s = st.lock().unwrap();
        // ---- 物理步（1 ms，半隐式欧拉）----
        if s.fallen {
            // 仿真钟在摔倒时停走，用真实时间管理 2 s 后自动重置
            if fall_at.elapsed() > Duration::from_millis(2000) {
                s.x = 0.0; s.vx = 0.25; s.p = 0.0; s.phase_swing = false; s.fallen = false;
                s.steps = 0; s.dist = 0.0; s.clock += 0.001;
                grace_until = s.clock + 0.15;
            }
            drop(s);
            next_tick += Duration::from_millis(1);
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        if !(s.fallen) {
            s.vx = lipm_step(s.x, s.vx, s.p, 0.001);
            s.x += s.vx * 0.001;
            s.clock += 0.001;
            if s.phase_swing {
                s.t_sw += 0.001;
                // 紧急重踏（真实机器人的 save-step 反射）：裕度逼近极限立即落地
                if !slow && (s.x - s.p).abs() > 0.22 {
                    s.phase_swing = false;
                    s.p = (s.x + s.vx / LAM).clamp(s.p - 0.40, s.p + 0.40);
                    s.steps += 1;
                    eprintln!("REPLANT t={:.2} p->{:.3} x={:.3} vx={:.3}", s.clock, s.p, s.x, s.vx);
                }
                if s.phase_swing && s.t_sw >= 0.15 {
                    s.phase_swing = false; s.p = s.p_target; s.steps += 1;
                    eprintln!("STEP t={:.2} p->{:.3} x={:.3} vx={:.3}", s.clock, s.p, s.x, s.vx);
                    s.vx += (0.2 * (s.x_d - s.vx)).clamp(0.0, 0.08); } // 速度伺服蹬伸
            }
            if (s.x - s.p).abs() > 0.30 || s.vx.abs() > 2.2 {
                if !s.fallen {
                    s.fallen = true; s.fall_clock = s.clock; fall_at = Instant::now();
                    eprintln!("FALL t={:.2} x={:.3} vx={:.3} p={:.3} swing={} steps={} xd={:.2}",
                        s.clock, s.x, s.vx, s.p, s.phase_swing, s.steps, s.x_d);
                }
            }
        }
        if s.fallen { drop(s); next_tick += Duration::from_millis(1); thread::sleep(Duration::from_millis(1)); continue; }

        // ---- 传感写入（真实 put，网格时间戳）----
        let force = (M * (G + LAM * LAM * (s.x - s.p))).max(0.0);
        while next_grid <= now {
            let ts = next_grid;
            let vals = [s.x, s.vx, force, if s.phase_swing { 1.0 } else { 0.0 }, (s.x - s.p).abs()];
            for (i, v) in vals.iter().enumerate() {
                let _ = db.put(1 + i as u32, Sample::new(ts, *v));
            }
            let burst = now < flags.burst_until.load(Ordering::Relaxed);
            if burst {
                for series in 20..=27u32 {
                    for _ in 0..9 { let _ = db.put(series, Sample::new(ts, vals[0])); }
                }
            }
            next_grid += 1_000_000;
        }

        // ---- 控制读路径：S1 快 = latest()；S1 慢 = 200ms 一次真实 scan ----
        let use_db = slow || s.clock > grace_until;
        let (rx, rvx) = if !use_db { (s.x, s.vx) } else if !slow {
            let t0 = Instant::now();
            // 只信本进程写入的样本（ts >= 启动时刻），否则用内部状态 —— 启动瞬间不读库里的旧数据
            let x = db.latest(1).ok().flatten()
                .filter(|m| m.ts >= boot_ns).map(|m| m.value).unwrap_or(s.x);
            let vx = db.latest(2).ok().flatten()
                .filter(|m| m.ts >= boot_ns).map(|m| m.value).unwrap_or(s.vx);
            let us = t0.elapsed().as_nanos() as f64 / 1e3;
            s.lat_us.push_back(us);
            if s.lat_us.len() > 4096 { s.lat_us.pop_front(); }
            (x, vx)
        } else {
            if s.clock - last_slow_read >= STALE_S {
                let q0 = Instant::now();
                let win: Vec<Sample> = db.scan(1, now - 100_000_000, i64::MAX, None, None)
                    .map(|it| it.collect()).unwrap_or_default();
                let win2: Vec<Sample> = db.scan(2, now - 100_000_000, i64::MAX, None, None)
                    .map(|it| it.collect()).unwrap_or_default();
                s.scan_ms = q0.elapsed().as_secs_f64() * 1e3;
                if let (Some(a), Some(b)) = (win.last(), win2.last()) {
                    stale_x = a.value; stale_vx = b.value;
                }
                last_slow_read = s.clock;
            }
            (stale_x, stale_vx)
        };

        // ---- 捕获点控制 ----
        let xc = rx + rvx / LAM;
        let trig = if slow { TOFF } else { TON };
        if !s.phase_swing && (xc - s.p > trig || xc - s.p < -trig) {
            let x_t = rx + rvx / LAM + 0.18 * (rvx - s.x_d) / LAM;
            s.phase_swing = true;
            s.t_sw = 0.0;
            s.p_from = s.p;
            s.p_target = x_t.clamp(s.p, s.p + 0.40);
        }

        // ---- 自动扰动（演示模式）：每 ~1.2 s 随机推力 ----
        if pushing && s.clock >= next_push {
            s.vx += PUSH_V * (0.9 + rand01() * 0.4);
            next_push = s.clock + 1.2 * (0.6 + rand01() * 0.8);
        }
        if kick != 0 { s.vx += kick as f64 / 1000.0; }
        s.dist = s.x;
        drop(s);

        next_tick += Duration::from_millis(1);
        let nw = Instant::now();
        if next_tick > nw { thread::sleep(next_tick - nw); } else { next_tick = nw; }
    }
}

fn rand01() -> f64 {
    (now_ns() % 1000) as f64 / 1000.0
}

fn s2_loop(db: Arc<Db>, st: Arc<Mutex<WState>>, flags: Arc<WFlags>) {
    loop {
        thread::sleep(Duration::from_millis(3000));
        let on = { let s = st.lock().unwrap(); s.s2_on && flags.s2.load(Ordering::Relaxed) && !s.fallen };
        if !on { continue; }
        let t0 = now_ns() - 3_000_000_000;
        let q0 = Instant::now();
        let win: Vec<Sample> = db.scan(2, t0, i64::MAX, None, None).map(|it| it.collect()).unwrap_or_default();
        let ms = q0.elapsed().as_secs_f64() * 1e3;
        if win.len() < 50 { continue; }
        let mean_v = win.iter().map(|p| p.value).sum::<f64>() / win.len() as f64;
        let mut s = st.lock().unwrap();
        s.s2_ms = ms;
        s.x_d = (0.5 + 0.3 * (0.6 - mean_v)).clamp(0.3, 0.9); // 慢则加速
    }
}

/* 挑战写入线程：8 条合成通道 20-27（1/2/4 kHz 混合） */
const WCH: &[(u32, f64)] = &[(20, 1000.0), (21, 1000.0), (22, 2000.0), (23, 2000.0),
                             (24, 4000.0), (25, 4000.0), (26, 4000.0), (27, 2000.0)];
fn writer_loop(db: Arc<Db>, st: Arc<Mutex<WState>>, flags: Arc<WFlags>) {
    let n = WCH.len();
    let mut next = vec![0i64; n];
    let mut period = vec![0i64; n];
    let start = now_ns();
    for i in 0..n { period[i] = (1e9 / WCH[i].1) as i64; next[i] = start.div_euclid(period[i]) * period[i]; }
    loop {
        let now = now_ns();
        let burst = now < flags.burst_until.load(Ordering::Relaxed);
        for i in 0..n {
            while next[i] <= now {
                let v = 0.5 * (next[i] as f64 / 1e9 * 2.0).sin();
                if burst {
                    let mut ok = 0u64; let mut err = 0u64;
                    for _ in 0..10 { match db.put(WCH[i].0, Sample::new(next[i], v)) { Ok(()) => ok += 1, Err(_) => err += 1 } }
                    let mut s = st.lock().unwrap(); s.acc_tort += ok; s.drop_tort += err;
                } else {
                    let mut s = st.lock().unwrap();
                    match db.put(WCH[i].0, Sample::new(next[i], v)) { Ok(()) => s.acc_rated += 1, Err(_) => s.drop_rated += 1 }
                }
                next[i] += period[i];
            }
        }
        let soonest = next.iter().cloned().min().unwrap_or(now + 200_000);
        thread::sleep(Duration::from_nanos(((soonest - now).max(50_000)) as u64));
    }
}

fn replay_client(db: Arc<Db>, st: Arc<Mutex<WState>>, until: i64, id: u32) {
    let mut series = 1 + id % 5;
    while now_ns() < until {
        series = series % 5 + 1;
        let q0 = Instant::now();
        let _ = db.scan(series, 0, i64::MAX, None, None).map(|it| it.count());
        let ms = q0.elapsed().as_secs_f64() * 1e3;
        let mut s = st.lock().unwrap();
        s.replay_queries += 1;
        s.replay_worst_ms = s.replay_worst_ms.max(ms);
    }
    let mut s = st.lock().unwrap();
    if s.replay_clients > 0 { s.replay_clients -= 1; }
}

struct Fnv(u64);
impl Fnv {
    fn new() -> Self { Self(0xcbf29ce484222325) }
    fn add(&mut self, b: &[u8]) { for &x in b { self.0 ^= x as u64; self.0 = self.0.wrapping_mul(0x100000001b3); } }
    fn add_s(&mut self, series: u32, ts: i64, v: f64) {
        self.add(&series.to_le_bytes()); self.add(&ts.to_le_bytes()); self.add(&v.to_bits().to_le_bytes());
    }
    fn hex(&self) -> String { format!("{:016x}", self.0) }
}

fn episode_window(db: &Db) -> Option<(i64, i64)> {
    let mk: Vec<Sample> = db.scan(MARKER, 0, i64::MAX, None, None).map(|it| it.collect()).unwrap_or_default();
    let mut open = None; let mut close = None;
    for m in &mk { if m.value == 1.0 { open = Some(m.ts); } if m.value == 0.0 { close = Some(m.ts); } }
    match (open, close) {
        (Some(t0), Some(t1)) if t1 > t0 => Some((t0, t1)),
        (Some(t0), _) => Some((t0, 0)),
        _ => None,
    }
}

/// 判决：传感通道 1-5（1 kHz）+ 合成通道 20-27（各自标称率）
fn verify(db: &Db, t0: i64, t1: i64) -> (String, u64, bool, u64, u64) {
    let mut dig = Fnv::new();
    let mut all_ok = true;
    let mut total = 0u64;
    let mut rows = String::new();
    for (series, rate) in [(1u32, 1000.0f64), (2, 1000.0), (3, 1000.0), (4, 1000.0), (5, 1000.0)]
        .iter().copied().chain(WCH.iter().map(|c| (c.0, c.1))) {
        let pts: Vec<Sample> = db.scan(series, t0, t1, None, None).map(|it| it.collect()).unwrap_or_default();
        let actual = pts.len() as i64;
        let period = (1e9 / rate) as i64;
        let mut downtime = 0i64;
        for w in pts.windows(2) { let g = w[1].ts - w[0].ts; if g > period * 10 { downtime += g - period; } }
        let expected = (((t1 - t0) - downtime) as f64 * rate / 1e9) as i64;
        for p in &pts { dig.add_s(series, p.ts, p.value); }
        total += actual as u64;
        let ok = (actual - expected).abs() <= 3;
        if !ok { all_ok = false; }
        rows.push_str(&format!("{{\"series\":{},\"rate\":{},\"expected\":{},\"actual\":{},\"ok\":{}}},",
                               series, rate, expected, actual, ok));
    }
    rows.pop();
    (format!("[{}]", rows), total, all_ok, 0, 0)
}

fn resp(code: &str, body: String) -> Vec<u8> {
    format!("HTTP/1.1 {}\r\nContent-Type: application/json; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", code, body.len(), body).into_bytes()
}

fn pct(d: &VecDeque<f64>, p: f64) -> f64 {
    if d.is_empty() { return 0.0; }
    let mut v: Vec<f64> = d.iter().copied().collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(p * (v.len() - 1) as f64) as usize]
}

fn handle(s: &mut TcpStream, db: &Arc<Db>, st: &Arc<Mutex<WState>>, f: &Arc<WFlags>) {
    let mut buf = [0u8; 2048];
    let n = s.read(&mut buf).unwrap_or(0);
    if n == 0 { return; }
    let req = String::from_utf8_lossy(&buf[..n]);
    let first = req.lines().next().unwrap_or("");
    let mut it = first.split_whitespace();
    let method = it.next().unwrap_or("");
    let target = it.next().unwrap_or("/");
    let (mut path, mut query) = (target, "");
    if let Some(i) = target.find('?') { path = &target[..i]; query = &target[i + 1..]; }
    let q = |k: &str| query.split('&').find_map(|kv| { let (a, b) = kv.split_once('=')?; if a == k { b.parse().ok() } else { None } });

    match (method, path) {
        ("GET", "/state") => {
            let g = st.lock().unwrap();
            let body = format!(
                "{{\"t\":{:.1},\"x\":{:.4},\"vx\":{:.3},\"p\":{:.4},\"swing\":{},\"fallen\":{},\"steps\":{},\"xd\":{:.2},\
                 \"slow\":{},\"s2\":{},\"p50\":{:.4},\"p99\":{:.4},\"p999\":{:.4},\"scan_ms\":{:.2},\"s2_ms\":{:.2},\
                 \"w_acc_r\":{},\"w_drop_r\":{},\"w_acc_t\":{},\"w_drop_t\":{},\"ep\":{},\"ep_t0\":{},\
                 \"rc\":{},\"rq\":{},\"rw\":{:.2},\"wm\":{},\"segs\":{}}}",
                g.clock, g.x, g.vx, g.p, g.phase_swing, g.fallen, g.steps, g.x_d,
                g.slow, g.s2_on, pct(&g.lat_us, 0.5), pct(&g.lat_us, 0.99), pct(&g.lat_us, 0.999),
                g.scan_ms, g.s2_ms,
                g.acc_rated, g.drop_rated, g.acc_tort, g.drop_tort,
                g.episode_active, g.episode_t0,
                g.replay_clients, g.replay_queries, g.replay_worst_ms,
                db.durable_watermark(), db.segment_count());
            s.write_all(&resp("200 OK", body)).unwrap();
        }
        ("GET", "/episode/status") => {
            let body = match episode_window(db) {
                Some((t0, 0)) => format!("{{\"active\":true,\"t0\":{}}}", t0),
                Some((t0, t1)) => format!("{{\"active\":false,\"t0\":{},\"t1\":{}}}", t0, t1),
                None => "{\"active\":false}".into(),
            };
            s.write_all(&resp("200 OK", body)).unwrap();
        }
        ("POST", "/episode/start") => {
            if let Some((t0, 0)) = episode_window(db) {
                st.lock().unwrap().episode_active = true;
                st.lock().unwrap().episode_t0 = t0;
                s.write_all(&resp("200 OK", format!("{{\"ok\":true,\"resumed\":true,\"t0\":{}}}", t0))).unwrap();
                return;
            }
            let t0 = now_ns();
            let _ = db.put(MARKER, Sample::new(t0, 1.0));
            let mut g = st.lock().unwrap();
            g.episode_active = true; g.episode_t0 = t0;
            s.write_all(&resp("200 OK", format!("{{\"ok\":true,\"t0\":{}}}", t0))).unwrap();
        }
        ("POST", "/episode/stop") => {
            let t1 = now_ns();
            let _ = db.put(MARKER, Sample::new(t1, 0.0));
            let (t0, _) = match episode_window(db) { Some(w) => w, None => { s.write_all(&resp("404 Not Found", "{}".into())).unwrap(); return; } };
            let (rows, total, all_ok, dr, dt) = verify(db, t0, t1);
            let pass = all_ok && dr == 0;
            s.write_all(&resp("200 OK", format!(
                "{{\"t0\":{},\"t1\":{},\"duration_ms\":{},\"series\":{},\"total\":{},\"pass\":{},\"dropped_rated\":{},\"dropped_torture\":{}}}",
                t0, t1, (t1 - t0) / 1_000_000, rows, total, pass, dr, dt))).unwrap();
        }
        ("GET", "/verify") => {
            match episode_window(db) {
                Some((t0, t1)) if t1 > t0 => {
                    let (_, _, _, _, _) = (0, 0, 0, 0, 0);
                    let mut dig = Fnv::new();
                    for (series, _) in [(1u32, 0.0f64), (2, 0.0), (3, 0.0), (4, 0.0), (5, 0.0)].iter().copied().chain(WCH.iter().map(|c| (c.0, 0.0))) {
                        let pts: Vec<Sample> = db.scan(series, t0, t1, None, None).map(|it| it.collect()).unwrap_or_default();
                        for p in &pts { dig.add_s(series, p.ts, p.value); }
                    }
                    s.write_all(&resp("200 OK", format!("{{\"t0\":{},\"t1\":{},\"digest\":\"{}\"}}", t0, t1, dig.hex()))).unwrap();
                }
                _ => s.write_all(&resp("409 Conflict", "{}".into())).unwrap(),
            }
        }
        ("POST", "/torture") => {
            let k = q("replay").unwrap_or(0.0) as u32;
            let sec = q("sec").unwrap_or(10.0);
            let until = now_ns() + (sec * 1e9) as i64;
            for i in 0..k {
                let (db, st) = (Arc::clone(db), Arc::clone(st));
                thread::spawn(move || replay_client(db, st, until, i));
            }
            st.lock().unwrap().replay_clients += k;
            s.write_all(&resp("200 OK", format!("{{\"ok\":true,\"clients\":{}}}", k))).unwrap();
        }
        ("POST", "/burst") => {
            let sec = q("sec").unwrap_or(2.0);
            f.burst_until.store(now_ns() + (sec * 1e9) as i64, Ordering::Relaxed);
            s.write_all(&resp("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/mode") => {
            let v = q("slow").unwrap_or(0.0) as u64 == 1;
            f.slow.store(v, Ordering::SeqCst);
            st.lock().unwrap().slow = v;
            s.write_all(&resp("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/kick") => {
            let w = q("w").unwrap_or(0.4);
            f.kick.store((w * 1000.0) as i32, Ordering::SeqCst);
            s.write_all(&resp("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/push") => {
            let v = q("on").unwrap_or(1.0) as u64 == 1;
            f.push.store(v, Ordering::Relaxed);
            s.write_all(&resp("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/s2") => {
            let v = q("on").unwrap_or(1.0) as u64 == 1;
            f.s2.store(v, Ordering::SeqCst);
            st.lock().unwrap().s2_on = v;
            s.write_all(&resp("200 OK", "{\"ok\":true}".into())).unwrap();
        }
        ("POST", "/crash") => {
            s.write_all(&resp("200 OK", "{\"dying\":true}".into())).unwrap();
            s.flush().unwrap();
            std::process::exit(9);
        }
        _ => { s.write_all(&resp("404 Not Found", "{}".into())).unwrap(); }
    }
}

fn main() {
    let mut cfg = Config::default();
    cfg.profile = Profile::Balanced;
    cfg.data_dir = Some(std::path::PathBuf::from("data-walk"));
    cfg.wal_sync = SyncPolicy::Group { interval_us: 1000 };
    let db = Arc::new(Db::open(cfg).expect("open db"));
    let st = Arc::new(Mutex::new(WState {
        x: 0.0, vx: 0.25, p: 0.0, phase_swing: false, t_sw: 0.0, p_target: 0.0, p_from: 0.0,
        x_d: 0.5, fallen: false, fall_clock: 0.0, clock: 0.0, steps: 0, dist: 0.0,
        lat_us: VecDeque::new(), scan_ms: 0.0, s2_ms: 0.0,
        slow: false, s2_on: true,
        acc_rated: 0, drop_rated: 0, acc_tort: 0, drop_tort: 0,
        episode_active: false, episode_t0: 0,
        replay_clients: 0, replay_queries: 0, replay_worst_ms: 0.0,
    }));
    let flags = Arc::new(WFlags {
        kick: AtomicI32::new(0), push: AtomicBool::new(false), slow: AtomicBool::new(false),
        s2: AtomicBool::new(true), burst_until: AtomicI64::new(0),
    });
    for f in [control_loop as fn(Arc<Db>, Arc<Mutex<WState>>, Arc<WFlags>), s2_loop, writer_loop] {
        let (db, st, fl) = (Arc::clone(&db), Arc::clone(&st), Arc::clone(&flags));
        thread::spawn(move || f(db, st, fl));
    }
    let listener = TcpListener::bind("127.0.0.1:8792").expect("bind 127.0.0.1:8792");
    eprintln!("walker-server on http://127.0.0.1:8792");
    for stream in listener.incoming() {
        if let Ok(mut s) = stream {
            let (db, st, fl) = (Arc::clone(&db), Arc::clone(&st), Arc::clone(&flags));
            thread::spawn(move || handle(&mut s, &db, &st, &fl));
        }
    }
}
