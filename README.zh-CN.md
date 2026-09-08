# rti-db

**用 Rust 编写的硬实时嵌入式时序数据引擎** —— 实时智能的数据面。

[English README](README.md)

[![CI](https://github.com/coidea-sys/rti-db/actions/workflows/ci.yml/badge.svg)](https://github.com/coidea-sys/rti-db/actions/workflows/ci.yml)
[![版本](https://img.shields.io/badge/version-0.6.0-blue)](https://github.com/coidea-sys/rti-db)
[![测试](https://img.shields.io/badge/tests-132%20passing-brightgreen)](#测试与可复现性)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-lightgrey)](LICENSE-MIT)

```text
传感器/事件流（100kHz 级）
        │
        ▼  无锁 SPSC 入队（~0.5µs，热路径零分配）
┌──────────────┐   批量    ┌───────────┐   seal   ┌────────────────┐
│  Ingest 层    │ ───────► │  WAL 层   │ ───────► │  Segment 层    │
│ SPSC/MPMC    │          │ 组提交+CRC│          │ 列存+压缩+ZM   │
│ 环形缓冲      │          │ 检查点截断 │          │ （只读列式段）  │
└──────────────┘          └───────────┘          └────────────────┘
        │                                                │
        ▼                                                ▼
  MemTable（内存池分配）◄──── 查询：谓词下推/zone map/零拷贝迭代
        │                                                │
        ▼                                                ▼
  快通道特征读取（µs 级）                     冷层（S3）/ Raft 副本
```

## 为什么做 rti-db？（定位）

通用数据库用确定性换取通用性：GC 停顿、分配器抖动、后台合并、不可预测的尾时延——这是"什么都能做"的代价。而实时智能（工业控制、自动驾驶、航天 GNC、机器人）的数据面**付不起这个代价**。

**rti-db 做相反的取舍：放弃通用性，换取确定性。** 它不是 SQL 数据库，不是 KV 存储，不是分析型数仓。它是传感器与决策回路之间缺失的那一层：写路径 O(1)、零分配、无锁、最坏情况时延可论证的存储引擎。

设计遵循 TRIZ 分离原理：不在"快"与"聪明"之间折中，而是**分离**——确定性快通道（无锁摄入、已验证控制回路）与智能慢通道（列存段、冷分层、副本），两者之间是显式的持久性契约。

## 有什么不同？（特色）

- **结构性有界的尾时延。** 无 GC、稳态热路径零堆分配（有全局分配计数器测试证明）、无锁竞争、写路径无后台合并。p999 的有界是构造出来的，不是调参调出来的。
- **持久性是显式的 API 选择。** `put()` = 最低延迟、尽力持久；`put_durable()` = 返回即抗 `kill -9`（等待 MemTable 应用 + WAL append + OS flush，返回 durable 水位）。类似 Kafka 的 acks=0/1/all，但逐次调用显式可选。
- **崩溃恢复 ~23ms。** WAL 检查点机制维持不变式"WAL 恰好保护当前 MemTable"——恢复只重放一个尾巴，而非整段日志。
- **真压缩 1.58×**（端到端落盘口径：delta-of-delta 时间戳 + XOR 浮点 + WAL 截断），同工作负载下反超 ClickHouse 的 1.22×。
- **内存安全 + 可审计的 unsafe 面。** 除三个小 crate（`rti-mem`、`rti-buffer`、`rti-wal-uring`）外全部 `#![forbid(unsafe_code)]`，每处 unsafe 配 `// SAFETY:` 注释。C/C++ 系统约 70% 的高危 CVE 源于内存错误——Rust 在编译期消除这一类。
- **no_std 子集。** `rti-core`、`rti-mem`、`rti-buffer` 可编译为 no_std（core+alloc）——同一引擎可从 MCU 安全岛跑到云端服务器。
- **io_uring 流水化 WAL**（Linux，feature 门控，优雅降级）、**单分片 Raft 副本**（快照、成员变更、预投票——全部用虚拟时钟确定性测试）、**S3 冷层**（纯 std HTTP 客户端，零外部依赖）、**TSN 时间对齐**（面向确定性网络）。
- **一切皆可确定性复现。** Raft 协议测试运行在虚拟时钟 + 内存传输上——选主、故障切换、日志一致性、网络分区场景 100% 可复现（连跑 30 次全绿）。

## 基准评测（v0.6，实测——非引用）

![写入延迟](docs/images/bench-write-latency.png)

![吞吐量](docs/images/bench-throughput.png)

![存储与恢复](docs/images/bench-storage-recovery.png)

![v0.5 → v0.6 自我改进](docs/images/bench-v05-v06.png)

完整方法论、原始 JSON 与一键复现评测台（`bench/run_all.sh`，约 35 分钟）见 `bench/`。同机、同工作负载（100 万点 / 8 序列）、同持久化档位、HDR 直方图、3 轮取中位。对手均为默认调优。

| 指标 | rti-db | SQLite (WAL) | Redis 7.2 | ClickHouse 24.8 |
|---|---|---|---|---|
| 单点写 p50 | **0.47 µs**（组提交档） | 7.5 µs | 25.4 µs | 1,940 µs |
| 单点写 p999 | **0.91 µs**（组提交档） | 15.3 **ms** | 116 µs | 27.6 ms |
| 批量写入 | **904 万 pts/s** | 250 万 | 25 万 | 104 万 |
| 全量扫描 | **3,300 万 pts/s** | 200 万 | ~100 万 | 210 万 |
| 落盘（100 万点） | **10.1 MB（1.58×）** | 27.0 MB | 59.8 MB（AOF） | 13.1 MB（1.22×） |
| 崩溃恢复 | **23.3 ms** | 13.7 ms | 1,094 ms | n/a（不可变 parts） |
| 零丢失写路径 | `put_durable` | 有 | 有（aof-always） | 有 |

**诚实地说说 rti-db 输在哪**：服务端通用聚合与 SQL（ClickHouse 主场）、多模型查询（Postgres/TimescaleDB 主场）；异步 `put()` 在崩溃时存在固有丢失窗口（以 ring 容量为界，已文档化，由 `put_durable` 闭合）。没有任何系统在所有指标上碾压所有数据库——rti-db 在它选择的指标上赢得是结构性的。

## 架构与策略（设计哲学）

12 个 crate，按同一套分离原理分层：

| Crate | 职责 |
|---|---|
| `rti-core` | 类型、错误、TSN 时间对齐（`no_std`） |
| `rti-mem` | 确定性内存池——尾时延的总开关（`no_std`） |
| `rti-buffer` | 无锁 SPSC/MPMC 环形缓冲，cache-line 分离（`no_std`） |
| `rti-wal` / `rti-wal-uring` | WAL（组提交+检查点）；io_uring 流水后端 |
| `rti-store` | 列存段、delta-of-delta+XOR 压缩、zone map、冷层（S3/本地） |
| `rti-query` | 谓词下推、向量化解码、零拷贝迭代器 |
| `rti-raft` | 单分片 Raft：选主、复制、快照、成员变更、预投票 |
| `rti-net` | 最小 TCP 行协议 |
| `rti-db` | 门面：`Db::open / put / put_durable / scan` |
| `rti-edge` | RTI-Edge 集成：三档预设（安全岛/认知层/规划层） |

策略一句话：**不在在位者的主场（通用 SQL、分析）硬碰硬——占领它们集体缺席的硬实时数据面，用结构性（而非调参）优势立足。**

## 快速上手

```toml
[dependencies]
rti-db = { git = "https://github.com/coidea-sys/rti-db" }
```

```rust
use rti_db::{Db, Config, SyncPolicy, Profile, Sample};
use std::time::Duration;

let mut cfg = Config::default();
cfg.profile = Profile::Balanced;
cfg.wal_sync = SyncPolicy::Group { interval_us: 1000 };
let mut db = Db::open(cfg)?;

// 快通道：~0.5µs 入队
db.put(1, Sample { ts: 1_700_000_000_000_000_000, value: 42.0 })?;

// 持久通道：返回即抗 kill -9
let watermark = db.put_durable(1, Sample { ts: 1_700_000_000_000_001_000, value: 43.0 },
                               Duration::from_millis(50))?;

for s in db.scan(1, 0, i64::MAX, None, None)? { /* 零拷贝迭代 */ }
```

运行三层边缘演示（安全岛 / 认知层 / 规划层）：

```bash
cargo run --release --example edge_demo
```

Feature 开关：`io-uring`（流水化 WAL 后端）、`s3`（S3 冷层）、`alloc-count`（分配审计）、`std`（默认；关闭后核心三 crate 进入 no_std）。

## 测试与可复现性

- **121 个测试全绿**（`cargo test --workspace`），全 feature 132 个
- Raft 协议确定性测试（虚拟时钟 + 内存传输）
- 稳态零分配证明测试
- 评测台：`bench/run_all.sh`（全量约 35 分钟，`--smoke` 2 分钟）——本 README 每个数字都可复现

## 路线图

- **v0.1–v0.6（已完成）**：核心引擎 → io_uring+块解码+TSN 对齐 → 确定性档+no_std+镜像 → Raft+冷层 → 流水 WAL+S3+边缘集成 → 持久性语义+WAL 检查点（本版本）
- **v0.7**：多分片 Raft、快速冲突回溯、segment 压实
- **v0.8**：形式化 WCET 分析工具链、更广 no_std 覆盖、TSN 硬件时间戳

## 许可证

双许可：[MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE)，任选其一。

## 贡献

欢迎 Issue 与 PR。提交前请运行 `cargo test --workspace --all-features`；涉及性能的改动必须附 `bench/run_all.sh` 的前后对比数字。
---
