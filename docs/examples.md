# 旗舰示例（Flagship Examples）

四个可运行示例，把 rti-db 的差异化能力变成可测量、可复现的演示。所有数字为实机实测（共享沙箱主机，独占 RT 环境会更好）。

Four runnable examples that turn rti-db's differentiators into measurable, reproducible demonstrations. All numbers are measured on a shared sandbox host (a dedicated RT setup scores better).

| # | 示例 | 能力 | 运行 |
|---|---|---|---|
| 1 | `embodied_memory` | O(1) `latest()` 反射读 + 列存窗口推理（具身智能工作记忆） | `cargo run -p rti-db --release --example embodied_memory` |
| 2 | `flight_recorder` | `put_durable` 零丢失契约（真实 SIGKILL 验证）+ 确定性回放 | `cargo run -p rti-db --release --example flight_recorder` |
| 3 | `multishard_failover` | 多分片 Raft：独立选举/隔离/分区换主/愈合收敛 | `cargo run -p rti-raft --release --example multishard_failover` |
| 4 | `compaction_value` | segment 压实：合并因子 -92%、扫描 +46%、重传幂等 | `cargo run -p rti-db --release --example compaction_value` |

---

## 1. 具身智能工作记忆（embodied_memory）

物理世界以 1–100 kHz 流动，基础模型以 10–100 ms 推理。示例同时跑两条回路：

- **S1 反射回路**：10 万次 O(1) `latest()`（零分配、无锁读路径），逐次计时
- **S2 推理回路**：200 次多路最近窗口聚合（谓词下推 + 单遍 O(1) 额外内存）

实测输出：

```text
摄入 200000 点（8 路交错）：1.96M pts/s
[S1 反射回路] 100000 次 O(1) latest() 读：
  p50=451ns  p99=570ns  p999=2699ns  max=15190ns  吞吐=1.1M reads/s
[S2 推理回路] 200 次窗口聚合（8 路 × 最近窗口）：
  平均 26.1µs/次（1600 个聚合结果）
```

**为什么重要**：100 kHz 控制拍 = 10µs 预算，反射读 p999=2.7µs 占用 <0.1% 预算。这是"物理世界的 KV cache"：O(1) 取最新状态 + 列存取情景窗口，快慢两车道结构性分离。

## 2. 飞行记录器：kill -9 零丢失（flight_recorder）

`put_durable()` 返回后该点已扛住进程被杀。示例**真的这么做**：子进程逐点 durable 写入并汇报确认序号，父进程在确认第 10,000 点后直接 SIGKILL，然后重开数据库逐点校验：

```text
子进程在确认第 10000 点后被 SIGKILL（退出状态：unix_wait_status(9)）
崩溃恢复：1.3 ms（WAL 检查点只重放当前 MemTable 尾巴）
零丢失校验：10002 个点全部在位且字节正确 ✓
确定性回放：两次全窗口扫描摘要 0x3beb316c07f42198 == 0x3beb316c07f42198 ✓
```

**为什么重要**：安全认证（航空/工业/交通）要求"模型看到了什么、何时看到、决定了什么"可微秒级复现。已确认数据零丢失 + 毫秒级恢复 + 字节级确定性回放 = 事故复现与审计的基础。

## 3. 多分片 Raft 故障转移（multishard_failover）

两个分片（SENSOR=10 / ACTUATOR=20）共享 3 台物理机，虚拟时钟驱动、完全确定性：

```text
T+ 220ms  选举完成：SENSOR Leader=节点1  ACTUATOR Leader=节点3（分片领导天然分散）
T+ 360ms  各写 5 条：SENSOR 提交水位 [5,5,5]  ACTUATOR [5,5,5]  日志一致 ✓
          分片隔离：SENSOR 日志不含 ACTUATOR 数据 ✓
T+ 360ms  网络分区：SENSOR Leader(节点1)被切走
T+ 580ms  SENSOR 换主成功：新 Leader=节点2；期间 ACTUATOR Leader 仍为节点3（无感知）✓
T+ 980ms  分区愈合：SENSOR 提交水位 [8,8,8]  ACTUATOR [8,8,8]
          收敛校验：两个分片所有节点已提交日志逐条一致 ✓
```

**为什么重要**：分片是独立的一致性域——单分片故障爆炸半径=该分片。全程虚拟时钟，输出逐次可复现：分布式正确性可以像单元测试一样验证。

## 4. segment 压实价值（compaction_value）

时序负载必有重传（补发/重试/迟到）。rti-db 按 (series, ts) **幂等去重**（首写生效），重传不污染数据，但重复帧物理堆积。写入 30 万点 → 重发 10 万点 ×4 轮 → `compact()`：

```text
压实前：段数=104  扫描 300000 点耗时 17.5ms（17.2M pts/s）  重传幂等（首写值）✓
compact()：合并 104 个段，耗时 134.3ms
压实后：段数=8   扫描 300000 点耗时 11.9ms（25.1M pts/s）
可见数据一致性：压实前后扫描首点完全相同 ✓
段数 104 → 8（合并因子 -92%），全量扫描 17.2M → 25.1M pts/s（+46%）
```

**为什么重要**：压实是崩溃安全的尾段合并（按设计保留重复帧，磁盘去重交给保留策略）。它保证的是长期运行下**扫描速度不衰减**——LSM 类引擎的必修课，且正确性可断言验证。
