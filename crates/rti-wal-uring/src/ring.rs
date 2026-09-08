//! Minimal unsafe glue for the io_uring commit backend (only this file is exempt from the unsafe ban).
//!
//! Safety model: every public method has **synchronous blocking** semantics — after SQE submission,
//! `submit_and_wait` waits for all CQEs and verifies each write's result byte count before returning.
//! The kernel therefore never references the caller's buffers or fd after the function returns —
//! the core invariant behind every `unsafe { push }`.

#![allow(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use io_uring::{opcode, types, IoUring};

/// Synchronous io_uring write/fsync submitter on a single open file.
///
/// - batch write: [`UringFile::write_at`] submits multiple buffers as one group of SQEs at continuous
///   offsets starting from `offset` (the whole batch in a single `io_uring_enter` syscall), waiting for all to complete;
/// - group-commit flush: [`UringFile::fsync`] submits `IORING_OP_FSYNC` and waits for completion.
pub struct UringFile {
    ring: IoUring,
    /// Keeps the fd alive; SQEs in the ring reference it by raw fd.
    file: File,
    /// SQ capacity (entries of `IoUring::new`); batch writes are chunked by this.
    depth: usize,
}

impl UringFile {
    /// Open (creating if missing) `path` and create a ring `queue_depth` deep.
    ///
    /// `queue_depth` is rounded up to a power of two (required by io_uring).
    /// Returns `Err` when the kernel/sandbox disallows io_uring (`io_uring_setup` returns EPERM/ENOSYS);
    /// the caller should fall back to the std backend (see rti-wal's
    /// `WalWriter::auto`).
    pub fn open(path: impl AsRef<Path>, queue_depth: u32) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).write(true).truncate(false).open(path)?;
        Self::from_file(file, queue_depth)
    }

    /// Create a submitter from an already-open `file` (ring creation failure also returns `Err`).
    pub fn from_file(file: File, queue_depth: u32) -> io::Result<Self> {
        let depth = queue_depth.max(1).next_power_of_two();
        let ring = IoUring::new(depth)?;
        Ok(Self { ring, file, depth: depth as usize })
    }

    /// Submit each buffer of `bufs` as a batch of writes at continuous offsets from `offset`, waiting for all to complete.
    ///
    /// Returns the total bytes written. Any failed or short write (result < requested length)
    /// fails the whole batch with `Err` — WAL frames are small and go through the page cache, so a short
    /// write can only come from something fatal like ENOSPC; treated fail-fast.
    pub fn write_at(&mut self, bufs: &[&[u8]], offset: u64) -> io::Result<usize> {
        for b in bufs {
            if b.len() > u32::MAX as usize {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "single buffer > u32::MAX"));
            }
        }
        let fd = types::Fd(self.file.as_raw_fd());
        let mut total = 0usize;
        let mut off = offset;
        // chunk by SQ capacity to avoid PushError (queue full).
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
                        // user_data carries the requested length; short writes are verified at completion.
                        .user_data(b.len() as u64);
                    // SAFETY: the SQE references `b` by raw pointer and `self.file` by raw fd.
                    // Both outlive this function's return; and before returning, this function
                    // blocks in `submit_and_wait` for that SQE's completion and verifies the CQE,
                    // so the window where the kernel holds these references is strictly within the borrow lifetimes.
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
                return Err(io::Error::other("io_uring lost completion"));
            }
        }
        Ok(total)
    }

    /// Submit `IORING_OP_FSYNC` and wait for completion (group boundary of group commit).
    pub fn fsync(&mut self) -> io::Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let op = opcode::Fsync::new(fd).build().user_data(0);
        {
            let mut sq = self.ring.submission();
            // SAFETY: FSYNC references only the fd; `self.file` outlives the function return,
            // and this SQE's completion has been blockingly awaited before returning.
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
            Err(io::Error::other("io_uring lost fsync completion"))
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

    /// Batch write + fsync + std read-back verification. Skipped gracefully when io_uring is unavailable.
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
        // the second batch appends at a changed offset, verifying explicit-offset semantics
        let c = b"!".as_slice();
        assert_eq!(f.write_at(&[c], n as u64).unwrap(), 1);
        f.fsync().unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got, b"hello io_uring!");
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// Batches larger than the SQ depth must be chunked and submitted correctly.
    #[test]
    fn batch_larger_than_queue_depth_is_chunked() {
        let p = tmpfile("chunked");
        let mut f = match UringFile::open(&p, 4) {
            Ok(f) => f,
            Err(_) => return, // skip gracefully when the environment does not support it
        };
        let bufs: Vec<Vec<u8>> = (0..37u8).map(|i| vec![i; 16]).collect();
        let refs: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
        let n = f.write_at(&refs, 1024).unwrap(); // nonzero start offset
        assert_eq!(n, 37 * 16);
        f.fsync().unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got.len(), 1024 + 37 * 16);
        for (i, b) in bufs.iter().enumerate() {
            assert_eq!(&got[1024 + i * 16..1024 + (i + 1) * 16], b.as_slice());
        }
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }

    /// An empty buffer batch is a no-op; when opening itself fails (non-Linux/no permission), Err without panic.
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
            Err(_) => { /* the graceful degradation path is covered by rti-wal tests */ }
        }
        std::fs::remove_dir_all(p.parent().unwrap()).ok();
    }
}
