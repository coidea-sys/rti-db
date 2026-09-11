//! Regression: a durable write after a big buffered batch, followed by seal + reopen,
//! must be the O(1) `latest()` head (SPEC v0.8 §3).
use rti_db::Db;
use rti_core::{Config, SyncPolicy, Sample};
use std::time::Duration;

#[test]
fn reopen_latest_includes_durable_tail() {
    let dir = std::env::temp_dir().join(format!("repro-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = Config {
        data_dir: Some(dir.clone()),
        wal_sync: SyncPolicy::Group { interval_us: 500 },
        ..Config::default()
    };
    const BASE: i64 = 1_700_000_000_000_000_000;
    {
        let db = Db::open(cfg.clone()).unwrap();
        for i in 0..10_000i64 {
            db.put(1, Sample::new(BASE + i * 1000, i as f64)).unwrap();
        }
        db.flush().unwrap();
        // strictly newer than every buffered sample
        db.put_durable(1, Sample::new(BASE + 10_000_000, 1.5), Duration::from_millis(50)).unwrap();
        assert_eq!(db.latest(1).unwrap(), Some(Sample { ts: BASE + 10_000_000, value: 1.5 }));
        db.seal().unwrap();
        db.compact().unwrap();
    }
    let db2 = Db::open(cfg).unwrap();
    assert_eq!(
        db2.latest(1).unwrap(),
        Some(Sample { ts: BASE + 10_000_000, value: 1.5 }),
        "latest after reopen must be the durable tail sample"
    );
    assert_eq!(db2.scan(1, 0, i64::MAX, None, None).unwrap().count(), 10_001);
    let _ = std::fs::remove_dir_all(&dir);
}
