//! rti-buffer：无锁环形缓冲。
//!
//! - [`SpscRing`]：单生产者单消费者，容量为 2 的幂，
//!   head/tail 分处不同 cache line，避免 false sharing；
//!   可 [`SpscRing::split`] 为跨线程的 producer/consumer。
//! - [`MpmcRing`]：Vyukov 有界 MPMC 队列（sequence 数组 + CAS）。
//!
//! 这是 SPEC 允许使用 unsafe 的两个 crate 之一；每处 unsafe 均附
//! `// SAFETY:` 注释。所有缓冲构造时一次分配，热路径零 malloc。
//!
//! ## no_std（v0.3）
//!
//! 默认 feature `std` 关闭时本 crate 为 `no_std`（`core` + `alloc`）：
//! [`SpscRing`] / [`MpmcRing`] 的堆分配版本 API 完全一致；另提供
//! [`SpscRingN`]——存储内联（`[MaybeUninit<T>; N]`，可驻留静态区/
//! 栈上）、无堆分配的 const-generic SPSC ring。

#![allow(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

/// 按 64 字节 cache line 对齐的包装，隔离 head/tail 等热计数器。
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct CacheLine<T>(pub T);

mod mpmc;
mod spsc;
mod spsc_n;

pub use mpmc::MpmcRing;
pub use spsc::{SpscConsumer, SpscProducer, SpscRing};
pub use spsc_n::SpscRingN;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spsc_fifo_and_full() {
        let mut r = SpscRing::with_capacity(4);
        assert!(r.is_empty());
        for i in 0..4 {
            r.push(i).unwrap();
        }
        assert_eq!(r.len(), 4);
        assert_eq!(r.push(99), Err(99)); // 满，元素被退回
        assert_eq!(r.pop(), Some(0));
        assert_eq!(r.pop(), Some(1));
        r.push(10).unwrap();
        assert_eq!(r.pop(), Some(2));
        assert_eq!(r.pop(), Some(3));
        assert_eq!(r.pop(), Some(10));
        assert_eq!(r.pop(), None);
    }

    /// SPEC §5 点名：ring 回绕测试。
    #[test]
    fn spsc_wraparound() {
        let cap = 8;
        let mut r = SpscRing::with_capacity(cap);
        // 反复写满再读空，游标多次回绕 storage
        for round in 0..1000u64 {
            for i in 0..cap as u64 {
                r.push(round * cap as u64 + i).unwrap();
            }
            assert_eq!(r.len(), cap);
            for i in 0..cap as u64 {
                assert_eq!(r.pop(), Some(round * cap as u64 + i));
            }
            assert!(r.is_empty());
        }
    }

    #[test]
    fn spsc_split_cross_thread() {
        let r = SpscRing::<u64>::with_capacity(1024);
        let (p, c) = r.split();
        let n = 100_000u64;
        let t = std::thread::spawn(move || {
            let mut sum = 0u64;
            let mut got = 0u64;
            while got < n {
                if let Some(v) = c.pop() {
                    sum += v;
                    got += 1;
                } else {
                    std::thread::yield_now();
                }
            }
            sum
        });
        for i in 0..n {
            while p.push(i).is_err() {
                std::thread::yield_now();
            }
        }
        let sum = t.join().unwrap();
        assert_eq!(sum, n * (n - 1) / 2);
    }

    #[test]
    fn mpmc_fifo_and_full() {
        let q = MpmcRing::with_capacity(4);
        for i in 0..4 {
            q.push(i).unwrap();
        }
        assert_eq!(q.push(9), Err(9));
        assert_eq!(q.len(), 4);
        assert_eq!(q.pop(), Some(0));
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn mpmc_concurrent_stress() {
        use std::sync::Arc;
        let q = Arc::new(MpmcRing::<u64>::with_capacity(256));
        let producers = 4;
        let per = 25_000u64;
        let total = producers * per;
        let mut handles = Vec::new();
        for _ in 0..producers {
            let q = Arc::clone(&q);
            handles.push(std::thread::spawn(move || {
                for i in 0..per {
                    while q.push(i).is_err() {
                        std::thread::yield_now();
                    }
                }
            }));
        }
        let mut consumers = Vec::new();
        for _ in 0..4 {
            let q = Arc::clone(&q);
            consumers.push(std::thread::spawn(move || {
                let mut cnt = 0u64;
                let mut sum = 0u64;
                while cnt < total / 4 {
                    if let Some(v) = q.pop() {
                        sum += v;
                        cnt += 1;
                    } else {
                        std::thread::yield_now();
                    }
                }
                sum
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let sum: u64 = consumers.into_iter().map(|h| h.join().unwrap()).sum();
        // 每个生产者产生 0..per，共 producers 份
        assert_eq!(sum, producers * (per * (per - 1) / 2));
        assert_eq!(q.len(), 0);
    }
}
