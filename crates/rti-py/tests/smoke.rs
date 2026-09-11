//! Smoke tests: exercise the PyO3 surface in-process (auto-initialize dev feature).
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[test]
fn python_end_to_end() -> PyResult<()> {
    Python::attach(|py| {
        let m = PyModule::new(py, "rti_db")?;
        rti_db::rti_db_init(&m)?;
        let code = cr#"
import tempfile
BASE = 1_700_000_000_000_000_000
d = tempfile.mkdtemp()
db = rti_db.Db(d, sync="group", group_interval_us=500)
for i in range(1000):
    db.put(1, BASE + i * 1000, float(i))
db.flush()
assert db.latest(1) == (BASE + 999 * 1000, 999.0), db.latest(1)
rows = db.scan(1, 0, 2**62)
assert len(rows) == 1000, len(rows)
avg = db.scan(1, 0, 2**62, agg="avg")
assert len(avg) == 1 and abs(avg[0][1] - 499.5) < 1e-9, avg
wm = db.put_durable(1, BASE + 1_000_000, 1000.0)
assert wm >= 1
assert db.latest(1) == (BASE + 1_000_000, 1000.0)
db.seal()
db.put(1, BASE + 2_000_000, 1001.0)
db.seal()
db.compact_series(1)
stats = db.compaction_stats()
assert stats["output_segments"] <= stats["input_segments"]
assert db.latest(1) == (BASE + 2_000_000, 1001.0)
assert db.memtable_len == 0
assert db.segment_count >= 1
# reopen: durability across restart
db2 = rti_db.Db(d, sync="group")
assert db2.latest(1) == (BASE + 2_000_000, 1001.0), db2.latest(1)
assert len(db2.scan(1, 0, 2**62)) == 1002
print("rti_db python end-to-end OK")
"#;
        let globals = PyDict::new(py);
        globals.set_item("rti_db", m)?;
        py.run(code, Some(&globals), None)
    })
}

#[test]
fn python_deterministic_in_memory() -> PyResult<()> {
    Python::attach(|py| {
        let m = PyModule::new(py, "rti_db")?;
        rti_db::rti_db_init(&m)?;
        let code = cr#"
db = rti_db.Db(profile="deterministic", memtable_max=100)
db.put(7, 10, 1.5)
db.flush()
assert db.latest(7) == (10, 1.5)
assert db.scan(7, 0, 100) == [(10, 1.5)]
print("rti_db deterministic OK")
"#;
        let globals = PyDict::new(py);
        globals.set_item("rti_db", m)?;
        py.run(code, Some(&globals), None)
    })
}
