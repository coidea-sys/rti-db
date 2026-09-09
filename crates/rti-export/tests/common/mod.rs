//! Shared test helpers: in-memory engine setup and backpressure-aware put.

#![allow(dead_code)]

use rti_core::{Config, Error, Sample};
use rti_db::Db;

/// `put` with retry on SPSC-ring backpressure (same pattern as rti-db tests).
pub fn put_retry(db: &Db, series: u32, s: Sample) {
    loop {
        match db.put(series, s) {
            Ok(()) => return,
            Err(Error::SeriesFull) => std::thread::yield_now(),
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

/// Pure in-memory engine (Deterministic profile) with room for `memtable_max` samples
/// (large enough that LRU eviction never fires during the test).
pub fn mem_db(memtable_max: usize) -> Db {
    let mut cfg = Config::deterministic();
    cfg.memtable_max = memtable_max;
    Db::open(cfg).unwrap()
}
