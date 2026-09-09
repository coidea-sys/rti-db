//! C ABI smoke test: drive the frozen extern "C" surface through its full
//! open/next/close lifecycle (rti_latest, rti_window_*, rti_chunk_*).

use std::sync::Arc;

use rti_core::{Config, Error, Sample};
use rti_db::Db;
use rti_vla::ffi::{
    self, RtiSample, RTI_ERR_FULL, RTI_ERR_INVALID, RTI_ERR_NULL, RTI_OK, RTI_SAMPLE,
};

fn db() -> Db {
    let mut cfg = Config::deterministic();
    cfg.memtable_max = 1 << 16;
    Db::open(cfg).unwrap()
}

fn put_retry(db: &Db, series: u32, ts: i64) {
    loop {
        match db.put(series, Sample::new(ts, ts as f64 * 10.0)) {
            Ok(()) => return,
            Err(Error::SeriesFull) => std::thread::yield_now(),
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

#[test]
fn ffi_latest_window_chunk_lifecycle() {
    let db = Arc::new(db());
    for i in 0..6 {
        put_retry(&db, 1, i * 2); // series 1: ts 0,2,4,6,8,10
        put_retry(&db, 2, i * 2 + 1); // series 2: ts 1,3,5,7,9,11
    }
    db.flush().unwrap();

    // safe Rust-side constructor; the handle is closed once at the end of the test.
    let h = ffi::db_handle(Arc::clone(&db));
    assert!(!h.is_null());

    // --- rti_latest ---
    let mut out = RtiSample { ts: 0, value: 0.0 };
    // SAFETY: h is live; out is a valid writable RtiSample.
    let rc = unsafe { ffi::rti_latest(h, 1, &mut out) };
    assert_eq!(rc, RTI_SAMPLE);
    assert_eq!(out, RtiSample { ts: 10, value: 100.0 });
    // empty series -> RTI_OK, no sample
    // SAFETY: same as above.
    let rc = unsafe { ffi::rti_latest(h, 42, &mut out) };
    assert_eq!(rc, RTI_OK);
    // null handling
    // SAFETY: exercising the documented null-argument contract.
    let rc = unsafe { ffi::rti_latest(std::ptr::null(), 1, &mut out) };
    assert_eq!(rc, RTI_ERR_NULL);
    // SAFETY: h is live; null out is the documented error path.
    let rc = unsafe { ffi::rti_latest(h, 1, std::ptr::null_mut()) };
    assert_eq!(rc, RTI_ERR_NULL);

    // --- rti_window_open / next / close ---
    let series = [1u32, 2u32];
    // SAFETY: h is live; `series` is readable for 2 elements; span [1, 11).
    let w = unsafe { ffi::rti_window_open(h, series.as_ptr(), series.len(), 1, 11) };
    assert!(!w.is_null());
    let mut got = Vec::new();
    loop {
        let mut s = RtiSample { ts: 0, value: 0.0 };
        // SAFETY: w is a live window handle; s is a valid writable RtiSample.
        let rc = unsafe { ffi::rti_window_next(w, &mut s) };
        if rc == RTI_OK {
            break;
        }
        assert_eq!(rc, RTI_SAMPLE);
        got.push(s.ts);
    }
    assert_eq!(got, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "merged in order over [1, 11)");
    // SAFETY: w came from rti_window_open and is closed exactly once.
    unsafe { ffi::rti_window_close(w) };

    // invalid span -> null handle
    // SAFETY: h is live; `series` readable for 2 elements; degenerate span is the
    // documented error path.
    let w = unsafe { ffi::rti_window_open(h, series.as_ptr(), series.len(), 5, 5) };
    assert!(w.is_null());
    // null series pointer -> null handle
    // SAFETY: null series is the documented error path.
    let w = unsafe { ffi::rti_window_open(h, std::ptr::null(), 2, 0, 10) };
    assert!(w.is_null());
    let _ = RTI_ERR_INVALID; // reserved code is part of the frozen surface

    // --- rti_chunk_new / push / seal / close ---
    let wm0 = db.durable_watermark();
    // SAFETY: h is live.
    let c = unsafe { ffi::rti_chunk_new(h, 9, 2, 20_000_000) };
    assert!(!c.is_null());
    // SAFETY: c is a live chunk handle.
    let rc = unsafe { ffi::rti_chunk_push(c, RtiSample { ts: 100, value: 1.0 }) };
    assert_eq!(rc, RTI_OK);
    // SAFETY: c is a live chunk handle.
    let rc = unsafe { ffi::rti_chunk_push(c, RtiSample { ts: 101, value: 2.0 }) };
    assert_eq!(rc, RTI_OK);
    // full chunk -> RTI_ERR_FULL (backpressure)
    // SAFETY: c is a live chunk handle.
    let rc = unsafe { ffi::rti_chunk_push(c, RtiSample { ts: 102, value: 3.0 }) };
    assert_eq!(rc, RTI_ERR_FULL);
    let mut wm = 0u64;
    // SAFETY: c is live; wm is a valid writable u64.
    let rc = unsafe { ffi::rti_chunk_seal(c, &mut wm) };
    assert_eq!(rc, RTI_OK);
    assert!(wm >= wm0 + 2, "seal must advance the durable watermark ({wm0} -> {wm})");
    // after sealing there is room again
    // SAFETY: c is a live chunk handle.
    let rc = unsafe { ffi::rti_chunk_push(c, RtiSample { ts: 103, value: 4.0 }) };
    assert_eq!(rc, RTI_OK);
    // SAFETY: c came from rti_chunk_new and is closed exactly once.
    unsafe { ffi::rti_chunk_close(c) };
    // chunk_len == 0 -> null handle
    // SAFETY: h is live; zero chunk_len is the documented error path.
    let c = unsafe { ffi::rti_chunk_new(h, 9, 0, 1_000) };
    assert!(c.is_null());

    // sealed points are readable back through the safe API
    db.flush().unwrap();
    let s = rti_vla::latest(&db, 9).unwrap().expect("sealed points exist");
    assert_eq!(s, Sample::new(101, 2.0));

    // close is null-tolerant
    // SAFETY: null close is explicitly allowed.
    unsafe {
        ffi::rti_window_close(std::ptr::null_mut());
        ffi::rti_chunk_close(std::ptr::null_mut());
        ffi::rti_db_close(std::ptr::null_mut());
    }
    // SAFETY: h came from db_handle and is closed exactly once.
    unsafe { ffi::rti_db_close(h) };
}
