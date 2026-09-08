//! rti-buffer: lock-free ring buffers.
//!
//! - [`SpscRing`]: single-producer single-consumer, power-of-two capacity;
//!   head/tail live on separate cache lines to avoid false sharing;
//!   can be [`SpscRing::split`] into cross-thread producer/consumer endpoints.
//! - [`MpmcRing`]: Vyukov bounded MPMC queue (sequence array + CAS).
//!
//! This is one of the two crates where the SPEC permits unsafe code; every unsafe
//! block carries a `// SAFETY:` comment. All buffers are allocated once at construction; zero malloc on hot paths.
//!
//! ## no_std (v0.3)
//!
//! With the default `std` feature disabled this crate is `no_std` (`core` + `alloc`):
//! the heap-allocated [`SpscRing`] / [`MpmcRing`] APIs are identical; this crate also
//! provides [`SpscRingN`] — a const-generic SPSC ring with inline storage
//! (`[MaybeUninit<T>; N]`, can live in static memory or on the stack) and zero heap allocation.

#![allow(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

/// Wrapper aligned to a 64-byte cache line, isolating hot counters such as head/tail.
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
        assert_eq!(r.push(99), Err(99)); // full; the element is handed back
        assert_eq!(r.pop(), Some(0));
        assert_eq!(r.pop(), Some(1));
        r.push(10).unwrap();
        assert_eq!(r.pop(), Some(2));
        assert_eq!(r.pop(), Some(3));
        assert_eq!(r.pop(), Some(10));
        assert_eq!(r.pop(), None);
    }

    /// Named by SPEC §5: ring wrap-around test.
    #[test]
    fn spsc_wraparound() {
        let cap = 8;
        let mut r = SpscRing::with_capacity(cap);
        // repeatedly fill and drain so the cursors wrap around the storage many times
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
        // each producer produces 0..per, `producers` copies in total
        assert_eq!(sum, producers * (per * (per - 1) / 2));
        assert_eq!(q.len(), 0);
    }
}
