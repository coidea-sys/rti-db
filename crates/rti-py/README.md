# rti-py — rti-db 的 Python 绑定

真实 rti-db 引擎（Rust）的 PyO3 绑定。阻塞 I/O 操作（open/flush/seal/compact/durable 等待/scan 收集）释放 GIL；`put`/`latest` 为纳秒级、持有 GIL。

## 安装

```bash
pip install rtidb        # PyPI 分发名（rti-py 与已占用的 pypi.org/project/rti 冲突）
import rti_db            # 导入名仍是 rti_db
```

源码构建（需要 Rust 工具链）：

```bash
pip install maturin
cd crates/rti-py
maturin build --release          # 产出 target/wheels/rtidb-*.whl
pip install target/wheels/rtidb-*.whl
```

> 已发布：rtidb 0.8.0 @ PyPI（macOS arm64；其他平台从源码构建同款命令）。
> macOS 若遇 "mis-aligned LINKEDIT string pool"（ld_prime 链接器 bug），
> 用 `scripts/fix-macho-linkedit.py` 修补后再安装。

## 快速上手

```python
import rti_db

db = rti_db.Db("/var/lib/rti", sync="group", group_interval_us=1000)

# 写入：O(1) 无锁入队（~0.2 µs，含绑定开销）
db.put(1, 1_700_000_000_000_000_000, 42.0)

# 持久写入：返回后 kill -9 也不丢
wm = db.put_durable(1, 1_700_000_000_000_001_000, 43.0, timeout_ms=50)

# O(1) 读最新值
ts, v = db.latest(1)

# 范围扫描 / 谓词 / 聚合
for ts, v in db.scan(1, 0, 2**62):
    ...
avg = db.scan(1, t0, t1, agg="avg")

db.flush()
db.compact()
```

`Db()` 参数：`data_dir`（省略 = 纯内存 Deterministic）、`sync`（"group"/"always"/"none"）、`profile`（"balanced"/"deterministic"）、`memtable_max`、`pool_bytes`。

## 本机实测（Apple Silicon，Python 3.12，含绑定开销）

| 操作 | 实测 |
|---|---|
| `put` | 0.21 µs/点 · 4.67M pts/s |
| `latest()` | 0.17 µs/次 |
| `put_durable` | 水位语义正确（返回 ≥ 本记录序号） |
| `scan` 全量 | 10k 点 6.7 ms |

## 已知构建问题（macOS + Xcode ld_prime）

部分 macOS 工具链上，rustc 经 ld_prime 链接的 cdylib 会触发 dyld 的
"mis-aligned LINKEDIT string pool" 拒绝加载（符号表字符串池 4 字节对齐而非 8）。
这是链接器 bug，与本 crate 代码无关。`scripts/fix-macho-linkedit.py`
可对已构建的 `.so/.dylib` 做二进制修补（8 字节对齐 + 重签），本仓库的
开发机即用该脚本验证通过。
