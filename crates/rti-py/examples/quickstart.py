import os
import tempfile
import time

import rti_db

d = tempfile.mkdtemp(prefix="rtipy-")
db = rti_db.Db(os.path.join(d, "data"), sync="group", group_interval_us=1000)
print("打开:", d)

N = 10_000
base = 1_700_000_000_000_000_000

t0 = time.perf_counter_ns()
for i in range(N):
    db.put(1, base + i * 1_000_000, 20.0 + (i % 100) * 0.1)
t1 = time.perf_counter_ns()
print(f"put x{N}: 平均 {(t1 - t0) / 1e3 / N:.2f} us/点, "
      f"吞吐 {N / ((t1 - t0) / 1e9):,.0f} pts/s")

wm = db.put_durable(1, base + N * 1_000_000, 42.5, timeout_ms=50)
print("put_durable 水位:", wm)

t0 = time.perf_counter_ns()
for _ in range(1000):
    r = db.latest(1)
t1 = time.perf_counter_ns()
print(f"latest(): {(t1 - t0) / 1e3 / 1000:.3f} us/次 -> {r}")

t0 = time.perf_counter_ns()
n = sum(1 for _ in db.scan(1, 0, 2**62))
t1 = time.perf_counter_ns()
print(f"scan 全量 {n} 点: {(t1 - t0) / 1e6:.2f} ms")

print("agg(avg):", db.scan(1, 0, 2**62, agg="avg"))

db.flush()
print("compact 合并段数:", db.compact())
print("全部通过")
