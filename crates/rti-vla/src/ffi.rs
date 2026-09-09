//! Frozen C ABI for VLA runtimes (SPEC §4, stable subset, semver-frozen from v0.7.0).
//!
//! `#[repr(C)]` over opaque handles; the single integration contract for VLA runtimes
//! in any language (Python/PyO3 bindings are deferred to v0.8).
//!
//! ## Handle lifecycle
//!
//! A `Db` is always opened by Rust host code (`rti_db::Db::open`); the host hands it to
//! C consumers via [`db_handle`], which boxes an `Arc<Db>` into an opaque [`RtiDb`]
//! pointer. Window/chunk handles are created by `rti_window_open` / `rti_chunk_new` and
//! released by the matching `rti_window_close` / `rti_chunk_close` / [`rti_db_close`]
//! (all null-tolerant). Every handle must be closed exactly once; passing a pointer not
//! obtained from this module is undefined behavior.
//!
//! ## Status codes
//!
//! All functions returning `i32` use: `>= 0` success (`1` = sample produced / present,
//! `0` = none / end of stream), `< 0` error ([`RTI_ERR_NULL`], [`RTI_ERR_FULL`],
//! [`RTI_ERR_INVALID`], [`RTI_ERR_INTERNAL`]).

use std::sync::Arc;
use std::time::Duration;

use rti_core::Sample;
use rti_db::Db;

use crate::{latest, window, ChunkBuffer, TimeSpan};

/// Success: operation completed, no sample produced (end of stream / empty series).
pub const RTI_OK: i32 = 0;
/// Success: a sample was produced (written to the out-parameter).
pub const RTI_SAMPLE: i32 = 1;
/// Error: a required pointer argument was null.
pub const RTI_ERR_NULL: i32 = -1;
/// Error: chunk buffer full (backpressure; seal the chunk, then retry).
pub const RTI_ERR_FULL: i32 = -2;
/// Error: invalid argument (e.g. degenerate window span, zero series length).
pub const RTI_ERR_INVALID: i32 = -3;
/// Error: internal engine error.
pub const RTI_ERR_INTERNAL: i32 = -4;

/// C-facing sample layout (`#[repr(C)]`, 16 bytes: nanosecond ts + f64 value).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RtiSample {
    /// Nanosecond timestamp.
    pub ts: i64,
    /// Sample value.
    pub value: f64,
}

impl From<Sample> for RtiSample {
    fn from(s: Sample) -> Self {
        Self { ts: s.ts, value: s.value }
    }
}

impl From<RtiSample> for Sample {
    fn from(s: RtiSample) -> Self {
        Sample::new(s.ts, s.value)
    }
}

/// Opaque database handle (owns one `Arc<Db>` refcount).
pub struct RtiDb {
    inner: Arc<Db>,
}

/// Opaque window iterator handle.
pub struct RtiWindow {
    inner: crate::WindowIter,
}

/// Opaque chunk-buffer handle.
pub struct RtiChunk {
    inner: ChunkBuffer,
}

/// Hand an `Arc<Db>` to C consumers as an opaque handle (safe Rust-side constructor;
/// the returned pointer must eventually be released with [`rti_db_close`]).
pub fn db_handle(db: Arc<Db>) -> *mut RtiDb {
    Box::into_raw(Box::new(RtiDb { inner: db }))
}

/// Release a database handle obtained from [`db_handle`]. Null-tolerant.
///
/// # Safety
///
/// `db` must be null or a pointer returned by [`db_handle`] that has not been closed yet.
#[no_mangle]
pub unsafe extern "C" fn rti_db_close(db: *mut RtiDb) {
    if !db.is_null() {
        // SAFETY: per the function contract `db` came from Box::into_raw in db_handle
        // and is closed exactly once, so reconstituting the Box is sound.
        drop(unsafe { Box::from_raw(db) });
    }
}

/// S1 reflex path: latest value for a series.
///
/// Returns [`RTI_SAMPLE`] and writes `*out` when the series has a point, [`RTI_OK`]
/// when it is empty, or a negative error code. Steady-state: no heap allocation.
///
/// # Safety
///
/// `db` must be a live handle from [`db_handle`]; `out` must be valid for writing one
/// [`RtiSample`].
#[no_mangle]
pub unsafe extern "C" fn rti_latest(db: *const RtiDb, series: u32, out: *mut RtiSample) -> i32 {
    if db.is_null() || out.is_null() {
        return RTI_ERR_NULL;
    }
    // SAFETY: per the function contract `db` is a live RtiDb handle; we only borrow it.
    let db = &unsafe { &*db }.inner;
    // SAFETY: per the function contract `out` points to writable memory for one RtiSample.
    let out = unsafe { &mut *out };
    match latest(db, series) {
        Ok(Some(s)) => {
            *out = s.into();
            RTI_SAMPLE
        }
        Ok(None) => RTI_OK,
        Err(_) => RTI_ERR_INTERNAL,
    }
}

/// S2 episodic context: open an ordered merging window over `series[0..series_len]`
/// for the half-open span `[start, end)`. Returns an opaque iterator handle, or null
/// on error (null arguments, `series_len == 0`, or `start >= end`).
///
/// The handle must be released with [`rti_window_close`].
///
/// # Safety
///
/// `db` must be a live handle from [`db_handle`]; `series` must be valid for reading
/// `series_len` consecutive `u32` values.
#[no_mangle]
pub unsafe extern "C" fn rti_window_open(
    db: *const RtiDb,
    series: *const u32,
    series_len: usize,
    start: i64,
    end: i64,
) -> *mut RtiWindow {
    if db.is_null() || series.is_null() || series_len == 0 {
        return std::ptr::null_mut();
    }
    // SAFETY: per the function contract `db` is a live RtiDb handle; we only borrow it.
    let db = &unsafe { &*db }.inner;
    // SAFETY: per the function contract `series` is readable for `series_len` elements.
    let series = unsafe { std::slice::from_raw_parts(series, series_len) };
    match window(db, series, TimeSpan::new(start, end)) {
        Ok(it) => Box::into_raw(Box::new(RtiWindow { inner: it })),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Pull the next sample from a window: [`RTI_SAMPLE`] + `*out` written, [`RTI_OK`] at
/// end of stream, negative on error.
///
/// # Safety
///
/// `w` must be a live handle from [`rti_window_open`]; `out` must be valid for writing
/// one [`RtiSample`].
#[no_mangle]
pub unsafe extern "C" fn rti_window_next(w: *mut RtiWindow, out: *mut RtiSample) -> i32 {
    if w.is_null() || out.is_null() {
        return RTI_ERR_NULL;
    }
    // SAFETY: per the function contract `w` is a live RtiWindow handle; we have unique
    // access for the duration of the call (handles are not thread-safe by contract).
    let w = unsafe { &mut *w };
    // SAFETY: per the function contract `out` points to writable memory for one RtiSample.
    let out = unsafe { &mut *out };
    match w.inner.next() {
        Some(s) => {
            *out = s.into();
            RTI_SAMPLE
        }
        None => RTI_OK,
    }
}

/// Release a window handle obtained from [`rti_window_open`]. Null-tolerant.
///
/// # Safety
///
/// `w` must be null or a pointer returned by [`rti_window_open`] that has not been
/// closed yet.
#[no_mangle]
pub unsafe extern "C" fn rti_window_close(w: *mut RtiWindow) {
    if !w.is_null() {
        // SAFETY: per the function contract `w` came from Box::into_raw in
        // rti_window_open and is closed exactly once.
        drop(unsafe { Box::from_raw(w) });
    }
}

/// Create a chunk buffer over `db` for `series` holding `chunk_len` action slots
/// aligned to a control period of `period_ns` nanoseconds. Returns null on error
/// (null db, `chunk_len == 0`). The handle must be released with [`rti_chunk_close`].
///
/// # Safety
///
/// `db` must be a live handle from [`db_handle`].
#[no_mangle]
pub unsafe extern "C" fn rti_chunk_new(
    db: *const RtiDb,
    series: u32,
    chunk_len: u32,
    period_ns: u64,
) -> *mut RtiChunk {
    if db.is_null() || chunk_len == 0 {
        return std::ptr::null_mut();
    }
    // SAFETY: per the function contract `db` is a live RtiDb handle; we only clone the
    // Arc inside it (the caller keeps ownership of the handle).
    let db = Arc::clone(&unsafe { &*db }.inner);
    let cb = ChunkBuffer::new(db, series, chunk_len, Duration::from_nanos(period_ns));
    Box::into_raw(Box::new(RtiChunk { inner: cb }))
}

/// Stage one action sample; never blocks. [`RTI_OK`] on success,
/// [`RTI_ERR_FULL`] when the chunk is full (seal it, then retry).
///
/// # Safety
///
/// `c` must be a live handle from [`rti_chunk_new`].
#[no_mangle]
pub unsafe extern "C" fn rti_chunk_push(c: *mut RtiChunk, sample: RtiSample) -> i32 {
    if c.is_null() {
        return RTI_ERR_NULL;
    }
    // SAFETY: per the function contract `c` is a live RtiChunk handle; we have unique
    // access for the duration of the call (handles are not thread-safe by contract).
    let c = unsafe { &mut *c };
    match c.inner.push(sample.into()) {
        Ok(()) => RTI_OK,
        Err(rti_core::Error::SeriesFull) => RTI_ERR_FULL,
        Err(_) => RTI_ERR_INTERNAL,
    }
}

/// Flush the staged chunk as one durable batch and return the durable watermark via
/// `*out_watermark`. [`RTI_OK`] on success, negative on error.
///
/// # Safety
///
/// `c` must be a live handle from [`rti_chunk_new`]; `out_watermark` must be valid for
/// writing one `u64`.
#[no_mangle]
pub unsafe extern "C" fn rti_chunk_seal(c: *mut RtiChunk, out_watermark: *mut u64) -> i32 {
    if c.is_null() || out_watermark.is_null() {
        return RTI_ERR_NULL;
    }
    // SAFETY: per the function contract `c` is a live RtiChunk handle; unique access
    // for the duration of the call (handles are not thread-safe by contract).
    let c = unsafe { &mut *c };
    // SAFETY: per the function contract `out_watermark` points to a writable u64.
    let out = unsafe { &mut *out_watermark };
    match c.inner.seal_chunk() {
        Ok(wm) => {
            *out = wm;
            RTI_OK
        }
        Err(_) => RTI_ERR_INTERNAL,
    }
}

/// Release a chunk-buffer handle obtained from [`rti_chunk_new`]. Null-tolerant.
/// Staged-but-unsealed samples are dropped (they were never written to the db).
///
/// # Safety
///
/// `c` must be null or a pointer returned by [`rti_chunk_new`] that has not been
/// closed yet.
#[no_mangle]
pub unsafe extern "C" fn rti_chunk_close(c: *mut RtiChunk) {
    if !c.is_null() {
        // SAFETY: per the function contract `c` came from Box::into_raw in
        // rti_chunk_new and is closed exactly once.
        drop(unsafe { Box::from_raw(c) });
    }
}
