# rti-db — 硬实时嵌入式数据引擎（v0.4）

RTI-Edge 实时智能平台的数据引擎：面向工业/车载/机器人场景的
**硬实时、确定性、零拷贝**嵌入式时序/事件存储。

设计目标（诚实声明）：不在所有指标上胜过所有数据库，而是在
**尾时延（p99/p999）、时延抖动、最坏情况有界性（WCET）、内存安全**
四个维度上做到通用数据库无法承诺的水平，同时保持可比的中位吞吐。

## 架构

```
                          put(series, sample)
                                 │  ~O(1)，无堆分配
                                 ▼
                   ┌─────────────────────────┐
   写入线程 ──────▶│  SPSC ring (rti-buffer)  │  capacity = 2^n
                   │  head/tail 分 cache line │  满 → Error::SeriesFull（背压）
                   └────────────┬────────────┘
                                │ 批量出队（≤1024/批）
                                ▼
                   ┌─────────────────────────┐
   ingest 线程 ───▶│  WAL append (rti-wal)    │  定长 28B 帧 + CRC32
                   │  组提交 SyncPolicy::Group │  崩溃恢复：CRC 损坏处截断
                   └────────────┬────────────┘
                                ▼
                   ┌─────────────────────────┐
                   │  MemTable (rti-store)    │  序列缓冲存于 SlabPool（O(1) 槽位）
                   │  满 → seal               │
                   └────────────┬────────────┘
                                ▼
                   ┌─────────────────────────┐
                   │  Segment（不可变列式）    │  ts: delta-of-delta + varint
                   │  + zone map + CRC32     │  val: XOR float（Gorilla 简化）
                   └─────────────────────────┘

   scan(series, t0, t1, pred, agg):
        flush → zone map 跳过无关 segment
             → 谓词下推到 decode 循环（不满足的点不物化）
             → 合并 memtable + segments，结果缓冲对象池复用（稳态 0 malloc）

   rti-net：TCP 行协议  put <series> <ts> <value> / scan <series> <t0> <t1> [agg]
```

crate 依赖方向（无环）：

```
rti-core ◀── rti-mem ◀── rti-store ◀── rti-db ◀── rti-net
rti-core ◀── rti-buffer ─┘            ▲
rti-core ◀── rti-wal ────────────────┤
rti-core ◀── rti-query ◀─────────────┘
rti-wal ◀⇢ rti-wal-uring（optional，feature `io-uring`，Linux only）
rti-core ◀── rti-raft（副本协议；日志条目 = rti-wal Record 批次）
```

## 快速上手

```bash
export PATH=$HOME/.cargo/bin:$PATH
cargo check --workspace          # 0 error / 0 warning
cargo test --workspace           # 94 个测试全绿（80 历史 + 14 新增）
cargo test -p rti-store --features s3           # +12：SigV4 已知向量 + mock S3 往返
cargo test -p rti-db --features s3              # +1：S3 归档端到端（mock server）
cargo test -p rti-wal --features io-uring       # 真实 io_uring 后端测试
cargo test -p rti-db --features alloc-count     # +1：稳态 0 堆分配增长验证
bash scripts/check-no-std.sh     # no_std 冒烟（rti-core/rti-mem/rti-buffer）
cargo run --release --example bench -p rti-db   # 吞吐/时延/压缩比/解码对比
```

## v0.6 新特性 — 持久性语义分档、WAL checkpoint、组提交优化（Wave 6）

v0.6 修复权威评测（见《rti-db 权威基准评测报告》§四/§五）暴露的三个短板：
**Group 档 kill -9 丢失 13.2% 在途点、100 万点落盘 38.1MB（0.42× 膨胀）、
Group 档批量吞吐仅 368k pts/s**。

### 1. 写入语义两档：put = 低延迟尽力持久，put_durable = 崩溃不丢

- `Db::put(series, sample)`：语义不变——无锁 SPSC 入队即返回（p50 亚微秒）。
  崩溃窗口 = ring 中未出队的点 + 组提交窗口。适用于可丢失最近 N 秒的
  监控/遥测场景。
- `Db::put_durable(series, sample, timeout) -> Result<u64>`（新增）：入队后
  阻塞等待 ingest 线程完成「应用 MemTable + WAL append 并 flush 到 OS」，
  返回 durable 水位序号。是否含 fsync 由当前 `SyncPolicy` 决定
  （`Always` 每条刷盘；`Group` 机器掉电窗口 ≤ interval；`None` 不刷盘）。
  **进程崩溃（kill -9）语义**：ingest 每批结束把 WAL 缓冲 flush 到 OS
  页缓存（`flush_os`，不 fsync），因此 `put_durable` 返回即保证已确认点
  在进程崩溃后不丢——新测试 `put_durable_survives_simulated_crash`
  （跳过 Drop 模拟 kill -9 后重开，500/500 点零丢失）。
  超时返回 `Error::Timeout`，但**数据不丢**（仍在管线中，随后持久化，
  可用水位复查）。
- `Db::durable_watermark() -> u64`（新增）：当前已持久化水位
  （已被 ingest 应用的入队序号），单调递增。正确性基础：占位计数与
  入队在同一临界区完成，水位票据与 ring 顺序严格一致。

### 2. WAL checkpoint：segment seal 后截断 WAL 前缀

- MemTable seal 为 segment（临时文件 + fsync + 原子 rename + 目录 fsync，
  v0.6 补齐）成功后，WAL 中已被 segment 覆盖的前缀被截断：保留 seal 点后
  进入新 MemTable 的尾巴记录（`Wal::checkpoint_keep`），整体写入
  `wal.tmp`、fsync、原子 rename、目录 fsync。rename 前崩溃 → 旧 WAL
  完好（与 segment 重复，恢复按 ts 去重）；rename 后崩溃 → 新 WAL +
  已落盘 segment。两方向均无丢失。
- 恢复重放期间**禁止** checkpoint（重放未读完）；重启恢复从截断后的
  WAL 起点重放，天然兼容。不变式：WAL 恰好保护当前 MemTable 的内容，
  稳态落盘 ≈ segment + ≤ 1 个 MemTable 的 WAL 尾巴。

### 3. 组提交吞吐优化

- ingest 线程单次唤醒尽量 drain ring（动态批，上限 `BATCH_MAX = 8192`），
  WAL 整批一次编码一次 `write_all`；
- fsync 频率由 `SyncPolicy::Group` 的 interval 门控（`sync_if_due`），
  **不再每批无条件 fsync**（v0.5 每 1024 条一次 fsync 是 368k pts/s
  的主因）；
- `SyncPolicy::Always` 语义不变（逐条 append + fsync）。

### v0.5 → v0.6 实测对比（同机同评测台，`bench-harness` 全量复测）

| 指标 | v0.5 | v0.6 | 变化 |
|---|---|---|---|
| Group(1ms) 崩溃丢失点（50 万点 kill -9） | 65,824（13.2%） | TBD | TBD |
| 100 万点落盘 | 38.1MB（0.42× 膨胀） | TBD | TBD |
| Group(1ms) 批量吞吐 | 368k pts/s | TBD | TBD |
| Group(1ms) put p999 | 17.9µs | TBD | TBD |

（复测原始数据：`bench-results-v06/rtidb.json`、`bench-results-v06/recovery.json`；
v0.5 基线：`bench-results/`。）

## v0.5 新特性（Wave 4 Stream A：rti-raft 增强）

在 v0.4 核心四件事之上补齐三个生产级机制，全部保持
MemoryTransport + 虚拟时钟的确定性测试与 `ReplicatedWal` 接口不变：

1. **快照（Snapshot）与日志压缩**
   - `Snapshot { last_included_index, last_included_term, state }`；
     `Node::take_snapshot(compact_upto, state)` 丢弃已提交前缀
     （越界/重复压缩返回 `Err`），`Node::install_snapshot()` 本地安装
     （陈旧快照幂等空操作）。
   - 日志索引绝对化：`log[0]` 对应 `snap_index + 1`；Follower 落后
     超过 Leader 日志起点（`next_index <= snap_index`）时，Leader 改发
     **InstallSnapshot RPC** 而非 AppendEntries；Follower 安装后保留
     任期连续的后缀，commit/applied 水位抬到快照点。
2. **单步成员变更**：`Node::add_peer(id)` / `remove_peer(id)` /
   `membership()`。一次只变一个（pending 期间拒绝新变更）；新配置
   编码为日志条目（哨兵 series = `u32::MAX`，经日志复制**提交后**
   生效，对 `ReplicatedWal::take_durable` 的用户数据流透明）；被
   移除节点降级为非投票成员（不选举、不投票、不计入多数派）。
3. **预投票（PreVote）**：超时后先以 `term+1` 预投票（不抬任期），
   获多数派预投票才正式参选；收方仅在「候选人在配置内 + 日志不
   落后 + 自己 Leader 租约过期（≥ election_min）」时投预投票——
   分区节点因此无法抬高集群任期。为防同刻超时节点互相拒绝预投票
   而活锁，选举超时哈希混入预竞选轮数（确定性不受影响）。
4. 测试（全部确定性）：快照安装后 Follower 追平、压缩后旧索引触发
   InstallSnapshot（附 take_snapshot 边界）、add_peer 后 4 节点选主
   与提交、remove_peer 后旧节点被多数派排除、PreVote 下分区节点
   任期零扰动、ReplicatedWal 哨兵过滤。简化取舍：配置在提交点生效
   （非 Raft 论文的追加即生效），单步变更下二者安全性等价；
   无快速冲突回溯；日志与快照均在内存。

## v0.5 新特性 — Stream B：io_uring 在途批流水（Wave 4）

rti-wal-uring 从 v0.2 的「提交 SQE 后阻塞等全部完成」升级为**多批在途
流水**（`rti_wal_uring::UringPipeline`，unsafe 胶水仍隔离在该 crate，
rti-wal 保持 `#![forbid(unsafe_code)]`）：

```
 push(batch)                reap 循环（CQE）              flush/sync
 ┌──────────┐  SQE   ┌──────────────┐  user_data=槽位   ┌──────────────┐
 │ 自有槽位  │──────▶│ 内核在途 ≤64  │──────▶ 释放槽位   │ 只等本批令牌  │
 │ 缓冲拷贝  │ 不等   │ 批完成水位推进│      批完成标志   │ + FSYNC 边界  │
 └──────────┘ 返回   └──────────────┘                   └──────────────┘
```

- **不阻塞提交**：`push` 拷贝数据进 pipeline 自有槽位、提交写 SQE 后
  立即返回批令牌（`BatchToken`）；槽位在 CQE reap 前绝不复用，
  `Drop` 全量 drain（安全模型不变式）。
- **CQE reap 循环**：非阻塞 drain 就绪完成事件，核验每批结果字节数
  （短写/负 errno 报错），推进连续完成水位。
- **flush 只等本批**：`flush(token)` 只等待 ≤ token 的批完成，不 drain
  全环；`sync()` = flush(最新批) + `IORING_OP_FSYNC`（组提交边界）。
- **在途深度可配**：`PipelineConfig::max_in_flight`，默认 **64**。
- **背压策略可配**：`BackpressurePolicy::Error`（深度满立即返回
  `Error::Backpressure`，fail-fast 且整批未提交可重试，硬实时推荐）
  或 `Block`（阻塞 reap 至有槽位，吞吐优先），默认 `Block`。
- rti-wal 侧对应新后端 `IoUringPipelinedWalWriter`
  （feature `io-uring`，backend_name = `"io_uring_pipeline"`）；
  **`WalWriter::auto` 行为不变**（仍选 v0.2 同步后端），流水后端显式
  打开，v0.2 既有测试语义不受影响。

bench（`cargo run --release -p rti-wal --features io-uring --example wal_bench`，
沙箱内核 6.6 真实 ring，N=500K 条 28B 帧，3 轮取优）：

```
== wal bench: uring-pipeline vs std write ==
backend             thrput ops/s      group ops/s   group p99 (us)
std                     11973402            50411           3406.3
uring-sync              12438292            50288           2766.0
uring-pipeline          12922761            49137           2695.0
```

诚实解读：提交路径（throughput 场景）pipeline 较 std **+8%**；
group-commit 场景被沙箱 fsync 时延（~1.3 ms/次）支配，三后端吞吐
相近属预期，pipeline 的持久化边界 p99 仍最优（**-21% vs std**）——
它不等待无关批、组边界只等本组。流水架构的真实收益在慢存储/高
并发批场景更大（提交不阻塞 = 下一批编码与上一批写盘重叠）。

新增测试（rti-wal-uring 7 + rti-wal feature io-uring 4，共 11）：
流水顺序性（批数 ≫ 深度，写回读逐字节一致）、超槽位批拆分、
Error 背压确定性触发且不丢数据（pending 保留可重试）、Block 背压
内联回收、Drop 全量 drain、旧令牌 flush 立即返回、续写偏移、
流水组提交与 v0.2 语义一致。无新增依赖。

## v0.5 新特性 — Stream C：真实 S3 冷层（Wave 4）

`S3ColdTier`（feature `s3`）从 v0.4 的签名骨架补全为完整实现：
put/get/list 基于既有 SigV4 签名（AWS 官方文档已知答案向量逐字节钉死），
HTTP/1.1 传输为纯 std `TcpStream` 自实现（约 150 行，含 Content-Length/chunked
解析与 10s 超时）——评估后放弃 `ureq`（TLS 依赖链过重，违背纯 std 铁律），
**零新增第三方依赖**。endpoint/region/bucket/凭证从环境变量与 `S3Config` 读取；
无凭证或 https endpoint 优雅报错。

测试（`--features s3`）：内嵌 mock S3 server（std TcpListener 解析 HTTP、校验
签名头、内存存对象）完成 put/get/list 往返、拒签 403、无凭证 Err，以及
`archive_older_than` → mock S3 → scan 逐点一致 → 重启 catalog 读回再一致
的端到端（rti-db +1）。

已知限制：仅支持明文 HTTP endpoint（生产可挂 TLS 终结代理或内网 MinIO）；
单段单次上传无 multipart、无自动重试（归档幂等，失败重调即可）。

## v0.5 新特性 — RTI-Edge 集成（Wave 5）

新 crate **rti-edge**（`#![forbid(unsafe_code)]`，零新增依赖）：把 rti-db
包装为 RTI-Edge 平台的数据面组件。`EdgeConfig { profile, data_dir,
memtable_max, mirror, raft, cold_tier, tsn_align_ns }` +
`EdgeNode::open / ingest / scan / archive_older_than / health`；`ingest`
可选先经 `TsAligner` 网格对齐，`health()` 返回统一视图
`EdgeHealth { profile, mirror_stats, raft_role, segment_count, alloc_ok }`
（`alloc_ok` 在 feature `alloc-count` 下为「基线后分配零增长」实测，
否则恒 true 并如实注明）。

RTI-L3 三层 × 三档预设：

```
 RTI-L3 层            预设构造器                 组成
 ──────────────────────────────────────────────────────────────
 ┌─────────────┐  EdgeConfig::safety_island()  Deterministic（纯内存、
 │  安全岛      │  ── 功能安全/硬实时控制环      LRU、稳态零分配热路径）
 │ (safety)    │                              + UDP 镜像（best-effort）
 ├─────────────┤  EdgeConfig::cognition()      Balanced（WAL+segment）
 │  认知层      │  ── 感知融合/副本冗余          + 3 节点内存 Raft：
 │ (cognition) │                              ingest 经多数派 durable
 ├─────────────┤  EdgeConfig::planning()       Balanced + 冷分层
 │  规划层      │  ── 长时程分析/历史回溯        （archive_older_than
 │ (planning)  │                              + scan 透明读回）
 └─────────────┘
```

- 演示：`cargo run -p rti-edge --example edge_demo`（三档一体化：
  镜像 1000/1000 接收、Raft durable 水位逐条推进、归档后 scan
  逐点一致，打印三份 health）。
- 测试：三档预设 open→ingest→scan→health 往返；safety_island 档
  故意配置 `data_dir` 也全程不落盘（目录从未创建）+ 镜像接收 >99%；
  feature `alloc-count` 下稳态 1000 次 ingest 分配零增长。
- 范围（诚实声明）：Raft 为**单进程内存集群仿真**（MemoryTransport
  + 虚拟时钟，验证数据面集成语义；跨进程部署需换 TcpTransport 并
  由部署方驱动时钟）；挂 Raft 的 `EdgeNode` 因 `Rc` 而非 `Send`。

## v0.4 新特性（Wave 3）

1. **rti-raft（新 crate，`#![forbid(unsafe_code)]`）：单分片简化 Raft**
   - 核心四件事：选主（SplitMix64 确定性种子"随机"超时）、心跳、
     日志复制（AppendEntries 含冲突截断）、多数派提交（仅当前任期
     条目按计数推进，遵循 Raft 论文）。
   - **逻辑时钟注入**（`Node::tick(now)`）：测试用虚拟时钟做到完全
     确定性。`Transport` trait（`send`/`recv`，允许丢包）+ 两个实现：
     `MemoryTransport`（共享 `MemoryNetwork`，支持双向网络分区）与
     `TcpTransport`（短连接、线格式 `len+from+payload`）。
   - **`ReplicatedWal`**：与 rti-wal 的集成点——日志条目 = WAL
     `Record` 批次；`append_batch` 提议，`take_durable` 只交付
     **多数派确认**的记录（此前不得视为崩溃安全）。
   - 测试（MemoryTransport + 虚拟时钟）：3 节点选主收敛、Leader
     宕机重选、多数派提交后 Leader 宕机不丢已提交条目、分区少数派
     不可提交（愈合后少数派条目被覆盖、已提交历史不变）。
   - 范围外（诚实声明）：无快照/成员变更/预投票/快速冲突回溯；
     日志在内存。
2. **rti-store 冷分层**
   - `ColdTier` trait（`put_segment`/`get_segment`/`list`，`Send+Sync`）。
   - `LocalFsColdTier`：完整实现（目录模拟对象存储，tmp+rename
     原子写，防路径穿越）。
   - `S3ColdTier`（feature `s3`）：**完整实现（v0.5）**——纯
     safe Rust 自实现 SHA-256/HMAC（**零新增依赖**），签名密钥派生
     与 `sign()` 完整可用并以 RFC 4231 / AWS 文档向量测试；put/get/
     list 返回注明原因的 `Err`（需真实凭证 + HTTP transport，README
     与 rustdoc 均注明）。
   - **`Db::archive_older_than(ts)`**：zone.max_ts < ts 的本地 segment
     上传冷层 → 删本地文件 → 追加 `archive.catalog`（含 zone map，
     崩溃残行静默跳过，重启以本地 .seg 为准去重）。`scan` **透明
     读回**：命中归档段时从冷层取回并缓存；谓词 zone-map 整段跳过
     对归档段同样生效（不触发读回）。`Db::set_cold_tier` /
     `archived_segment_count` 配套。

## v0.3 新特性（Wave 2）

1. **rti-db：确定性配置档 `Profile::Deterministic`**
   - `Config` 新增 `profile: Profile` 与 `mirror: Option<Mirror>`；
     `data_dir` 改为 `Option<PathBuf>`（SPEC-evolution 明确许可的
     破坏性变更：Deterministic 下可为 `None`，Balanced 下必须 `Some`）。
   - Deterministic 语义：强制 `SyncPolicy::None`；**纯内存运行**
     （不建目录、不开 WAL、不写 segment，即使 `data_dir = Some` 也
     绝不触碰文件系统，有 1M put 测试钉死）；MemTable 满按 LRU
     （写入触达时钟）丢弃整条最老序列并计数（`Db::lru_evictions()`），
     被丢弃缓冲进 spare 池复用；`seal()` 为 no-op。
   - **UDP 镜像 `Mirror`**：`put` 入队成功的同时以非阻塞 UDP 发送
     20 字节小端数据报（`series u32 | ts i64 | value bits u64`）。
     best-effort：失败仅计数（`Db::mirror_stats()` →
     `MirrorStats { sent, failed }`），不阻塞、不重试、不影响返回值
     ——不进热路径延迟预算。
   - **`alloc_count()` 热路径分配验证**（feature `alloc-count`，
     仅测试用）：`rti-mem` 提供 `CountingAllocator`（委托
     `std::alloc::System` + Relaxed 计数），集成测试注册为
     `#[global_allocator]`，验证 Deterministic 稳态 put
     **0 堆分配增长**（20 万次 put，before == after 精确相等）。
2. **no_std 裁剪（rti-core / rti-mem / rti-buffer）**
   - 三 crate 改为 `#![cfg_attr(not(feature = "std"), no_std)]`，
     `std` 为默认 feature；no_std 下仅依赖 `core` + `alloc`，
     API 完全一致。rti-core 的 `Error::Io` 变体与 `Config`
     （含 `PathBuf`）仅在 `std` 下存在（SPEC-evolution 许可）。
   - rti-buffer 新增 **`SpscRingN<T, const N: usize>`**：存储内联
     （`[MaybeUninit<T>; N]`，零堆分配，可驻留静态区/栈上）、
     单线程 `&mut self` 句柄、无原子操作的 SPSC ring，
     供 no_std 嵌入式场景使用。
   - `scripts/check-no-std.sh`：一键冒烟
     `cargo check -p rti-core/-mem/-buffer --no-default-features`。

## v0.2 新特性（Wave 1）

1. **rti-wal：io_uring 后端**（feature `io-uring`，默认关闭，Linux only）
   - 新 `WalWriter` trait（`append` / `append_batch` / `sync_now` /
     `offset` / `flush_policy`）；v0.1 的 std 实现保留为 `StdWalWriter`，
     `Wal` 公共 API 逐字节不变（内部委托）。
   - `IoUringWalWriter`：帧攒批后**一次系统调用提交整批 SQE** +
     组提交边界单次 `IORING_OP_FSYNC`。
   - `WalWriter::auto(path, sync)` 自动选后端：feature on + 内核允许 →
     io_uring，否则优雅回退 Std（永不 panic）。`RTI_WAL_FORCE_STD=1`
     可强制回退（逃生门）。
2. **rti-store：8 路展开块解码**
   - `decode_ts_block::<N>(&[u8], &mut [i64; N])` /
     `decode_val_block::<N>(&[u8], &mut [f64; N])`：一次调用解码一整块，
     safe Rust、定数 8 路展开、**不用 nightly `std::simd`**；与流式
     解码器逐点 / bit-exact 一致（有测试钉死）。
   - 诚实说明：delta-of-delta 与 XOR 均有串行数据依赖，无法真正
     SIMD；收益来自摊薄逐点调用的分支/边界检查（实测 ts 1.39x、
     val 1.19x，见 bench）。
3. **rti-core：TSN 时间对齐 `TsAligner`**
   - `TsAligner::new(align_ns)`：`align(ts)` 向下取整到网格
     （`rem_euclid`，负时间戳按数学 floor）、`jitter(ts)` 恒在
     `[0, align_ns)`。
   - `PtpProfile { grandmaster_offset_ns, path_delay_ns }`：网格对齐前
     先做 PTP 校正（`ts - offset - delay`，**saturating** 不回绕）。

```rust
use rti_core::{Config, Sample, SyncPolicy};
use rti_db::Db;
use rti_query::{Agg, Pred};

let db = Db::open(Config {
    data_dir: "rti-data".into(),
    memtable_max: 1 << 16,
    wal_sync: SyncPolicy::Group { interval_us: 1_000 },
    pool_bytes: 1 << 20,
})?;
db.put(7, Sample::new(1_700_000_000_000_000_000, 21.5))?;
db.flush()?; // 读己之写边界 / 持久化边界
for s in db.scan(7, 0, i64::MAX, Some(Pred::Gt(20.0)), Some(Agg::Avg))? {
    println!("{:?}", s);
}
```

TCP 行协议（`rti-net`）：

```text
put  7 1700000000000000000 21.5   → OK
scan 7 0 9999999999999999999 avg  → 0 21.5 / END
```

## 性能设计落实（SPEC §4）

1. **写路径**：`put` 仅做一次 SPSC 入队（缓存对端游标，绝大多数情况
   只有一次原子读 + 一次 Release 写）；ingest 线程批量 WAL append，
   `SyncPolicy::Group` 下按间隔 + 批边界组提交，p999 可控。
2. **读路径**：segment zone map（min/max ts、min/max val）先整段跳过；
   `Pred` 以闭包下推到 decode 循环，不满足谓词的点不解压物化；
   段解码迭代器逐点流式产出、零分配。
3. **时间戳压缩**：delta-of-delta + zigzag varint（Gorilla 简化版）。
4. **内存**：MemTable 序列缓冲存于 `rti-mem::SlabPool`（O(1) 槽位、
   无系统调用）；`BumpArena` 提供 O(1) reset 的 epoch 分配器；
   查询结果缓冲经对象池复用，稳态热循环 0 次 malloc。
5. **bench**：`examples/bench.rs`（std::time，离线可跑）输出 put 吞吐、
   p50/p99/p999 写入时延、scan 吞吐（1M 点）、压缩比；v0.2 新增
   标量流式解码 vs 8 路展开块解码吞吐对比（ts / val 各一组）。

## 依赖决策

本环境 crates.io 可用，因此按 SPEC §1 使用了白名单中的
`crc32fast`（WAL/segment CRC，含纯 Rust fallback 实现于该 crate 内部）。
`memmap2` 未使用：segment 读取采用**预读缓冲**（一次 `fs::read`），
对小 segment 更简单且无生命周期负担。`criterion` 未使用：bench 用
`std::time` 写在 `examples/bench.rs`，离线可重复运行。
除此之外全部为纯 std 实现，离线可 `cargo check`/`cargo test`（依赖已
vendor 于 Cargo.lock）。

**v0.2 新增依赖**（SPEC-evolution §3 白名单放宽许可）：

| 依赖 | 用途 | 理由 |
|---|---|---|
| `io-uring` 0.7 | rti-wal-uring 的 SQE/CQE API | SPEC-evolution Wave 1 明确点名；`io_uring_setup/enter` 无 libc 稳定封装，该 crate 是事实标准的最小绑定 |
| `libc`（传递） | io-uring 的 syscall 封装 | 传递依赖，不可选 |
| `bitflags` 2（传递） | io-uring 的 opcode 标志位 | 传递依赖，不可选 |

`io-uring` 仅被 `rti-wal-uring`（`cfg(target_os = "linux")`）依赖，
而 `rti-wal-uring` 是 `rti-wal` 的 **optional** 依赖（feature
`io-uring`，默认关闭）：默认构建（含非 Linux）不拉取、不编译它。

**v0.5 无新增第三方依赖**（Stream C）：S3 冷层 HTTP 传输为纯 std
`TcpStream` 自实现（约 150 行）。曾评估 SPEC-wave45 点名的候选
`ureq`：其默认 TLS 链（rustls + ring/aws-lc-rs 或 native-tls +
openssl）会引入数十个传递依赖与 C 构建步骤，违背「优先纯 std、
离线可构建」铁律；自实现 HTTP/1.1 客户端的代价仅为「不支持
TLS」（HTTP-only endpoint，README 与 rustdoc 均已声明），对
内网 MinIO / 代理终结 TLS 的部署足够。

**v0.3/v0.4 无新增第三方依赖**：no_std 裁剪只用 `core` + `alloc`；
`CountingAllocator` 基于 `std::alloc::System`；镜像与 TcpTransport
用 `std::net`；S3 SigV4 签名为纯 safe 自实现 SHA-256/HMAC（避免
引入 hmac/sha2/aws-sdk 依赖链）。rti-db 新增对 rti-mem 的**工作区
内**依赖（复用其 unsafe 豁免与计数分配器）；rti-raft 仅依赖
rti-core + rti-wal（工作区内）。

## unsafe 政策

- `#![forbid(unsafe_code)]`：rti-core / rti-wal / rti-store / rti-query / rti-net / rti-db / rti-raft。
- `#![allow(unsafe_code)]`：rti-mem、rti-buffer；每处 unsafe 均有
  `// SAFETY:` 注释说明不变量（槽位初始化状态、SPSC/MPMC 协议的
  Acquire/Release 配对、对齐与边界论证）。
- **v0.2 新增豁免：`rti-wal-uring`**（仅 `src/ring.rs` 一个文件）。
  必要性：`io-uring` crate 的 SQE 提交（`SubmissionQueue::push`）是
  `unsafe fn`，rti-wal 本体为 `forbid(unsafe_code)` 无法直接调用；
  按 SPEC-evolution §5「feature-gate 或独立 crate」把最小 unsafe
  胶水隔离为独立 crate，沿用同一纪律（单文件豁免 + 每处 SAFETY）。
  核心安全不变量：所有公开方法**同步阻塞**（`submit_and_wait` 等齐
  CQE 并核验字节数后才返回），内核不会引用已失效的缓冲区或 fd。

## 设计权衡（诚实清单）

- **异步持久化**：`put` 入队即返回，持久化由 ingest 线程完成；
  `flush()` 提供显式的读己之写/持久化边界。换来的是写路径 ~O(1) 与
  可控 p999，代价是崩溃窗口 = 组提交间隔（`SyncPolicy::Group`）。
- **`put` 的互斥锁**：为支持多写者线程共享 `&Db`（rti-net 每连接一线程），
  SPSC 生产端包了一层 `Mutex`；单写者（推荐部署：单 ingest 线程 + 分片）
  时锁无竞争，开销与纯无锁同量级。多写者高竞争场景应改用 MPMC
  （rti-buffer 已提供 `MpmcRing`）。
- **scan 合并代价**：memtable 与各 segment 结果在查询时合并排序
  （归并前各自有序，近似 O(n)）；生产系统会用 LSM 层级压实减少段数。
- **单序列单 segment 文件**：seal 时每条序列写一个文件，简单可靠；
  高序列基数场景应改为多序列共享段 + 段内索引。
- **谓词下推粒度**：段级 zone map + decode 循环内过滤；未实现块级
  （block-in-segment）跳过，是后续优化点。
- **聚合返回形式**：`agg = Some(_)` 时 `scan` 返回恰好一个样本
  （`ts = t0, value = 聚合值`），保持与 SPEC §3 迭代器签名一致。
- **`rti_query::scan` 的泛型化**：为避免 `rti-query ↔ rti-db` 循环依赖，
  扫描目标抽象为 `ScanSource` trait；门面层保留了与 SPEC §3 逐字一致的
  `rti_db::scan(&Db, ...)` 自由函数与 `Db::scan` 方法。
- **WAL 恢复回放**：恢复时全量回放进 MemTable（超大 WAL 会慢）；
  生产系统应做 checkpoint + WAL 截断。
- **io_uring 后端为同步阻塞提交**（v0.2）：每批 SQE 提交后即
  `submit_and_wait`，换取最小的 unsafe 表面积与缓冲区生命周期论证；
  尚未做「在途批 + 流水提交」（submit 不等待、下一批填充时收上一批
  CQE），那才是 io_uring 相对 `write(2)` 的完整收益，留作后续优化。
  块解码函数同理：串行依赖下只能摊薄调用开销，无法真 SIMD。
- **Deterministic 档 = 放弃持久化**（v0.3）：崩溃即数据全失，
  适用于「实时档 + 镜像冗余」部署；LRU 丢弃粒度为**整条序列**
  （不是逐点），时钟按写入触达更新（scan 不算触达）。
- **镜像是单向 best-effort**：UDP 丢包、乱序、重复都不补偿；
  内核 rmem 耗尽即丢（测试中 loopback 突发即观测到），镜像端
  只能用于冗余/审计，不能作为一致性来源。
- **v0.3 顺手修复的存量热路径分配**：rti-store `MemTable::insert`
  的 `.ok_or(Error::Corrupt(...into()))` 是惰性求值的反模式
  （ eagerly 构造 String，每次 insert 分配一次），已改 `ok_or_else`——
  这正是 `alloc_count()` 测试抓出来的。

## 已知限制（骨架边界）

- 单写者 ingest 线程；rti-raft 是**教学级单分片实现**：无快照、
  无成员变更、无预投票、日志在内存、冲突回溯逐格回退；
  `TcpTransport` 为短连接（无连接池）——可作副本协议的正确性
  基座，不是生产 RPC 层。
- 冷分层读回缓存**只增不减**（归档段首次命中后驻留内存）；无
  缓存逐出策略。归档粒度为整段（zone.max_ts < ts）。
- `SpscRingN` 为单线程句柄（`&mut self`）：跨线程拆分需调用方自行
  保证 SPSC 协议（后续可提供 `split` 端点包装）。
- Deterministic 档 scan 结果随 LRU 丢弃而截断（只能查到未被淘汰的
  最近窗口），这是「有界内存」的必然语义。
- segment 无层级压实（compaction），段数随 seal 次数线性增长。
- `pool_bytes` 目前作为预算配置项存在，ring/slab 容量由常量与
  `memtable_max` 推导；后续可统一纳入预算核算。
- 错误处理以 `Error` 枚举透传，ingest 线程致命错误会使后续
  `put`/`scan` 返回错误（fail-fast）。
