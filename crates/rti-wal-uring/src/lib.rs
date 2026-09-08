//! rti-wal-uring: the io_uring commit backend for rti-wal (Linux only).
//!
//! Rationale (v0.2 decision, recorded in the README 'dependency decisions'):
//! the `io-uring` crate's SQE submission API (`SubmissionQueue::push`) is an
//! `unsafe fn`, while rti-wal is `#![forbid(unsafe_code)]` per SPEC §1.
//! The **minimal necessary** unsafe submission glue is therefore isolated in this separate crate
//! (same discipline as rti-mem/rti-buffer: a single file with `#![allow(unsafe_code)]`
//! + a `// SAFETY:` comment at each site), keeping rti-wal itself 100% safe.
//!
//! This crate's public API is entirely safe:
//! - v0.2 synchronous blocking semantics: [`UringFile::write_at`] submits a batch of write SQEs
//!   and returns only after **all have completed** — no dangling in-flight SQEs;
//! - v0.5 in-flight batch pipeline: [`UringPipeline`] does not wait after submission; a CQE reap
//!   loop incrementally reclaims completion events; batch buffers referenced by SQEs are owned by
//!   the pipeline, never reused before reaping, and fully drained on `Drop` (safety model: see the pipe.rs module docs).

#[cfg(target_os = "linux")]
mod pipe;
#[cfg(target_os = "linux")]
mod ring;

#[cfg(target_os = "linux")]
pub use pipe::{BackpressurePolicy, BatchToken, PipeError, PipelineConfig, UringPipeline};
#[cfg(target_os = "linux")]
pub use ring::UringFile;

/// Probe whether the current kernel/sandbox allows creating an io_uring instance.
///
/// Always `false` on non-Linux platforms; on Linux, tries creating a minimal ring
/// and immediately destroying it (`false` when `io_uring_setup` is disabled by seccomp or the kernel is too old).
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

    /// The probe itself must not panic (seccomp sandboxes / old kernels must return gracefully).
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
            // probe results on Linux depend on the environment; no concrete value is asserted, only callability.
            let _ = probe();
        }
    }
}
