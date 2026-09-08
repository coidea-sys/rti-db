//! io_uring 提交后端的最小 unsafe 胶水（仅本文件豁免 unsafe）。
//!
//! 安全模型：所有公开方法都是**同步阻塞**语义——SQE 提交后
//! `submit_and_wait` 等待全部 CQE 返回、并核验每个写的结果字节数后才
//! return。因此内核不会在函数返回后仍引用调用方的缓冲区或 fd，
//! 这是每处 `unsafe { push }` 成立的核心不变量。

#![allow(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use io_uring::{opcode, types, IoUring};

/// 单个已打开文件上的 io_uring 同步写/刷盘提交器。
///
/// - 批量写：[`UringFile::write_at`] 一次把多段缓冲按连续偏移提交为一组
///   SQE（一个 `io_uring_enter` 系统调用提交整批），等待全部完成；
/// - 组提交刷盘：[`UringFile::fsync`] 提交 `IORING_OP_FSYNC` 并等待完成。
pub struct UringFile {
    ring: IoUring,
    /// 保持 fd 存活；ring 中的 SQE 按裸 fd 引用它。
    file: File,
    /// SQ 容量（`IoUring::new` 的 entries），批量写按此分块。
    depth: usize,
}

impl UringFile {
    /// 打开（不存在则创建）`path` 并创建一个 `queue_depth` 深的 ring。
    ///
    /// `queue_depth` 会向上取整到 2 的幂（io_uring 要求）。
    /// 内核/沙箱不允许 io_uring（`io_uring_setup` 返回 EPERM/ENOSYS）
    /// 时返回 `Err`，调用方应回退到 std 后端（见 rti-wal 的
    /// `WalWriter::auto`）。
    pub fn open(path: impl AsRef<Path>, queue_depth: u32) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        Self::from_file(file, queue_depth)
    }

    /// 用已打开的 `file` 创建提交器（ring 创建失败同样返回 `Err`）。
    pub fn from_file(file: File, queue_depth: u32) -> io::Result<Self> {
        let depth = queue_depth.max(1).next_power_of_two();
        let ring = IoUring::new(depth)?;
        Ok(Self { ring, file, depth: depth as usize })
    }

    /// 把 `bufs` 各段按 `offset` 起的连续偏移批量提交写，等待全部完成。
    ///
    /// 返回写入的总字节数。任何一段写失败或短写（结果 < 请求长度）
    /// 都会使整批以 `Err` 告终——WAL 帧很小且走页缓存，短写只可能
    /// 来自 ENOSPC 等致命情况，按 fail-fast 处理。
    pub fn write_at(&mut self, bufs: &[&[u8]], offset: u64) -> io::Result<usize> {
        for b in bufs {
            if b.len() > u32::MAX as usize {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "single buffer > u32::MAX"));
            }
        }
        let fd = types::Fd(self.file.as_raw_fd());
        let mut total = 0usize;
        let mut off = offset;
        // 按 SQ 容量分块，避免 PushError（队列满）。
        for chunk in bufs.chunks(self.depth) {
            let mut pushed = 0usize;
            {
                let mut sq = self.ring.submission();
                for b in chunk {
                    if b.is_empty() {
                        continue;
                    }
                    let op = opcode::Write::new(fd, b.as_ptr(), b.len() as u32)
                        .offset(off)
                        .build()
                        // user_data 携带请求长度，完成时核验短写。
                        .user_data(b.len() as u64);
                    // SAFETY: SQE 按裸指针引用 `b`、按裸 fd 引用 `self.file`。
                    // 两者都活到本函数返回之后；而本函数在返回前
                    // `submit_and_wait` 阻塞等待该 SQE 完成并核验 CQE，
                    // 故内核持有这些引用的窗口严格在借用有效期内。
                    unsafe { sq.push(&op) }.map_err(|_| {
                        io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ full")
                    })?;
                    off += b.len() as u64;
                    pushed += 1;
                }
            }
            if pushed == 0 {
                continue;
            }
            self.ring.submit_and_wait(pushed)?;
            let mut completed = 0usize;
            for cqe in self.ring.completion() {
                let res = cqe.result();
                if res < 0 {
                    return Err(io::Error::from_raw_os_error(-res));
                }
                if res as u64 != cqe.user_data() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "io_uring short write",
                    ));
                }
                total += res as usize;
                completed += 1;
            }
            if completed != pushed {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "io_uring lost completion",
                ));
            }
        }
        Ok(total)
    }

    /// 提交 `IORING_OP_FSYNC` 并等待完成（组提交的组边界）。
    pub fn fsync(&mut self) -> io::Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let op = opcode::Fsync::new(fd).build().user_data(0);
        {
            let mut sq = self.ring.submission();
            // SAFETY: FSYNC 只引用 fd；`self.file` 活到函数返回之后，
            // 且返回前已阻塞等待该 SQE 完成。
            unsafe { sq.push(&op) }
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ full"))?;
        }
        self.ring.submit_and_wait(1)?;
        let mut completed = false;
        for cqe in self.ring.completion() {
            let res = cqe.result();
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }
            completed = true;
        }
        if completed {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "io_uring lost fsync completion"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpfile(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-wal-uring-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d.join("uring.log")
    }

    /// 批量写 + fsync + std 读回校验。io_uring 不可用时优雅跳过。
    #[test]
    fn write_fsync_readback() {
        let p = tmpfile("roundtrip");
        let mut f = match UringFile::open(&p, 8) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("io_uring unavailable ({e}), skipping on-ring assertion");
                return;
            }
        };
        let a = b"hello ".as_slice();
        let b = b"io_uring".as_slice();
        let n = f.write_at(&[a, b], 0).unwrap();
        assert_eq!(n, a.len() + b.len());
        f.fsync().unwrap();
        // 第二批改偏移追加，验证显式偏移语义
        let c = b"!".as_slice();
        assert_eq!(f.write_at(&[c], n as u64).unwrap(), 1);
        f.fsync().unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got, b"hello io_uring!");
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 批大小超过 SQ 深度时必须正确分块提交。
    #[test]
    fn batch_larger_than_queue_depth_is_chunked() {
        let p = tmpfile("chunked");
        let mut f = match UringFile::open(&p, 4) {
            Ok(f) => f,
            Err(_) => return, // 环境不支持时优雅跳过
        };
        let bufs: Vec<Vec<u8>> = (0..37u8).map(|i| vec![i; 16]).collect();
        let refs: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
        let n = f.write_at(&refs, 1024).unwrap(); // 起始偏移非 0
        assert_eq!(n, 37 * 16);
        f.fsync().unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got.len(), 1024 + 37 * 16);
        for (i, b) in bufs.iter().enumerate() {
            assert_eq!(&got[1024 + i * 16..1024 + (i + 1) * 16], b.as_slice());
        }
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// 空缓冲批 = 无操作；打开即失败（非 Linux/无权限）时 Err 不 panic。
    #[test]
    fn empty_batch_and_graceful_open() {
        let p = tmpfile("empty");
        match UringFile::open(&p, 2) {
            Ok(mut f) => {
                assert_eq!(f.write_at(&[], 0).unwrap(), 0);
                assert_eq!(f.write_at(&[b"".as_slice(), b"x".as_slice()], 0).unwrap(), 1);
                f.fsync().unwrap();
                assert_eq!(std::fs::read(&p).unwrap(), b"x");
            }
            Err(_) => { /* 优雅降级路径由 rti-wal 测试覆盖 */ }
        }
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }
}
