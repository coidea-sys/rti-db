//! v0.3 热路径分配计数验证（feature `alloc-count`，独立测试二进制）。
//!
//! 用全局计数分配器验证：**Deterministic 档稳态 put 热路径 0 堆分配增长**。
//! 独立二进制保证单测试进程，计数不受其他测试干扰。

#![cfg(feature = "alloc-count")]

use rti_core::{Config, Error, Sample};
use rti_db::{alloc_count, CountingAllocator, Db};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn put_retry(db: &Db, series: u32, s: Sample) {
    loop {
        match db.put(series, s) {
            Ok(()) => return,
            Err(Error::SeriesFull) => std::thread::yield_now(), // 背压重试
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

#[test]
fn deterministic_put_steady_state_zero_alloc_growth() {
    let mut cfg = Config::deterministic();
    // 容量足够大：预热 + 测量窗口内不触发 LRU 丢弃、序列 Vec 不再扩容。
    cfg.memtable_max = 1 << 20;
    let db = Db::open(cfg).unwrap();

    const SERIES: i64 = 4;
    // 预热 600k：ring 流水 / ingest 批缓冲 / 序列 Vec 容量全部进入稳态
    // （每序列 150k 样本 → Vec 容量 262144，测量只再加 50k）。
    for i in 0..600_000i64 {
        put_retry(&db, (i % SERIES) as u32, Sample::new(i, 1.0));
    }
    db.flush().unwrap();

    let before = alloc_count();
    for j in 0..200_000i64 {
        put_retry(&db, (j % SERIES) as u32, Sample::new(600_000 + j, 2.0));
    }
    db.flush().unwrap();
    let after = alloc_count();

    assert_eq!(
        after, before,
        "稳态 put 热路径必须 0 堆分配增长（before={before}, after={after}）"
    );

    // 数据完好性抽查（顺带覆盖 LRU 未误触发）
    assert_eq!(db.lru_evictions(), 0, "大容量下不应发生丢弃");
    let got: Vec<Sample> = db.scan(3, 799_990, 800_000, None, None).unwrap().collect();
    assert!(!got.is_empty());
}
