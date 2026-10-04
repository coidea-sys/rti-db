# rti-db 交互式演示套件

三个层次，一个比一个"真"。所有数字都是实测，合成值会明确标注。

| 演示 | 是什么 | 怎么跑 |
|---|---|---|
| [index.html](index.html) | 三个真实修复的动画回放：时间戳碰撞 flaky 测试 · macOS 平台门控 · 门面再导出 | 浏览器直接打开 |
| [robot-brain.html](robot-brain.html) | 架构仿真：cart-pole 机器人的神经信号流经忠实建模的引擎（ring 背压/组提交/seal/ts 去重，Gorilla 编码真实现算压缩率）。合成延迟如实标注 | 浏览器直接打开 |
| [robot-live.html](robot-live.html) + [robot-walk.html](robot-walk.html) | **零仿真**：真实 cart-pole / LIPM 双足行走在本地 Rust 进程内跑，控制状态的每个读、传感样本的每个写都经过真实 rti-db（WAL 组提交、1 kHz 网格时间戳） | 见下方"运行 LIVE 演示" |

## 运行 LIVE 演示

```bash
cd demo/server
cargo build --release          # 纯 std，零新依赖
sh run.sh                      # cart-pole @ http://127.0.0.1:8791（监督进程：kill -9 自动重启 + WAL 重放）
# 行走版（第二个终端）：
while true; do ./target/release/walker 2>>walker.log; sleep 0.3; done   # @ http://127.0.0.1:8792
```

然后浏览器打开 `robot-live.html` / `robot-walk.html`。

## 能玩什么（全部真实测量）

- **S1/S2 双速大脑**：S1 反射 = `Db::latest()`（本机实测 p50 0.042 µs / p999 1.08 µs）；S2 慢思考 = 真实 `scan()` 窗口推理（实测计时）。断开 S1（控制退化为 200 ms 慢查询），行走版在随机推力下摔倒，cart-pole 版同理
- **kill -9**：真实 `exit(9)`（不走 Drop），supervisor 重启后新进程打开同一 Db 触发真实 WAL 重放；恢复时间为浏览器实测停机时长；零丢失用真实 `Agg::Count` / 全窗口 digest 前后校验（实测二次 kill 后 digest 逐字节一致）
- **写突发 / 回放竞争**：×10 写入压测与并发全量回放客户端（实测 ~1000 次 scan/s、最差 49 ms）——分析读真实挤压写平面，`put()` 背压拒绝被如实计数
- **挑战模式**（robot-live）：16 通道 ~37k pts/s 多速率写入 + 三重 torture + 中途随机 kill -9，episode 结束由引擎 scan 出判决书。实测：15 s / 539,374 点 / 额定与压测丢数均为 0 / PASS

## 已发表基线对照（判决书并排展示）

- ros2 工具链 1 kHz 饱和丢 **8.6–16.9%**（ros2probe，arXiv 2606.10746）；全量录制丢 75%+
- rosbag2 大分片每 10 分钟**静默丢 1 分钟**（issue #2108）
- rosbag2 崩溃标准答案 = 丢当前打开的 bag chunk；消息丢失可观测性 2026 年 5 月才有

## 实现说明（诚实记录）

- 判决语义：每通道"期望点数（扣除如实计量的中断时间）vs 实测 scan 点数"逐一对齐；丢失的唯一来源是 `put()` 真实返回 `SeriesFull`（网格时间戳 + 追赶补齐，写入侧永不伪造缺口）
- episode 元数据存在引擎内（标记通道 999）——kill 杀不掉存在 DB 里的状态，重启后从 Db 恢复 episode 窗口
- 控制环读 Db 必须过滤本进程启动前的样本，并在摔倒重置后留传感器重建期（`put()`→`latest()` 无 read-your-writes 保证——两个真实 bug 的教训）
- 行走物理选型：弹簧-质量四足 bound 是研究级稳定化问题（开发中放弃，见 commit 33d6d2e）；LIPM（Kajita 线性倒立摆）是教科书行走模型，线性、可验证，数据平面故事不变
