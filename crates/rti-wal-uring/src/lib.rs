//! rti-wal-uring：rti-wal 的 io_uring 提交后端（Linux only）。
//!
//! 存在的理由（v0.2 决策，README「依赖决策」有记录）：
//! `io-uring` crate 的 SQE 提交 API（`SubmissionQueue::push`）是
//! `unsafe fn`，而 rti-wal 按 SPEC §1 是 `#![forbid(unsafe_code)]`。
//! 因此把**最小必需**的 unsafe 提交胶水隔离到这个独立 crate
//! （与 rti-mem/rti-buffer 相同的纪律：单文件 `#![allow(unsafe_code)]`
//! + 每处 `// SAFETY:` 注释），rti-wal 本体保持 100% safe。
//!
//! 本 crate 的公开 API 全部是 safe 的：
//! - v0.2 同步阻塞语义：[`UringFile::write_at`] 提交一批写 SQE 并
//!   **等待全部完成**后才返回，不存在悬挂的在途 SQE；
//! - v0.5 在途批流水：[`UringPipeline`] 提交后不等待，CQE reap 循环
//!   增量回收完成事件；SQE 引用的批缓冲由 pipeline 自有、reap 前绝不
//!   复用、`Drop` 时全量 drain（安全模型见 pipe.rs 模块文档）。

#[cfg(target_os = "linux")]
mod pipe;
#[cfg(target_os = "linux")]
mod ring;

#[cfg(target_os = "linux")]
pub use pipe::{BackpressurePolicy, BatchToken, PipeError, PipelineConfig, UringPipeline};
#[cfg(target_os = "linux")]
pub use ring::UringFile;

/// 探测当前内核/沙箱是否允许创建 io_uring 实例。
///
/// 非 Linux 平台恒为 `false`；Linux 上尝试创建一个最小 ring
/// 再立即销毁（`io_uring_setup` 被 seccomp 禁用或内核过旧时返回 `false`）。
pub fn probe() -> bool {
    #[cfg(target_os = "linux")]
    {
        io_uring::IoUring::new(2).is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 探测本身不得 panic（沙箱 seccomp / 老内核都要优雅返回）。
    #[test]
    fn probe_never_panics() {
        let _ = probe();
    }

    #[test]
    fn non_linux_probe_is_false() {
        #[cfg(not(target_os = "linux"))]
        assert!(!probe());
        #[cfg(target_os = "linux")]
        {
            // Linux 上 probe 结果依赖环境，不断言具体值，只保证可调用。
            let _ = probe();
        }
    }
}
