//! Bounded MPMC queue: Vyukov's algorithm (per-cell sequence array + CAS).

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};

use crate::CacheLine;

/// Queue cell: a sequence number marks the slot state (writable/readable).
struct Cell<T> {
    seq: AtomicUsize,
    data: MaybeUninit<T>,
}

/// Bounded multi-producer multi-consumer ring with power-of-two capacity.
///
/// `push`/`pop` are lock-free CAS loops that return immediately when full/empty (bounded backpressure);
/// no heap allocation and no syscalls on hot paths.
pub struct MpmcRing<T> {
    buf: Box<[Cell<T>]>,
    mask: usize,
    /// Enqueue position (contended by producers).
    enqueue_pos: CacheLine<AtomicUsize>,
    /// Dequeue position (contended by consumers).
    dequeue_pos: CacheLine<AtomicUsize>,
}

impl<T> MpmcRing<T> {
    /// Create a queue with capacity at least `n` (actual capacity is a power of two).
    pub fn with_capacity(n: usize) -> Self {
        let cap = n.max(1).next_power_of_two();
        let mut buf = Vec::with_capacity(cap);
        for i in 0..cap {
            buf.push(Cell { seq: AtomicUsize::new(i), data: MaybeUninit::uninit() });
        }
        Self {
            buf: buf.into_boxed_slice(),
            mask: cap - 1,
            enqueue_pos: CacheLine(AtomicUsize::new(0)),
            dequeue_pos: CacheLine(AtomicUsize::new(0)),
        }
    }

    /// Write one element; handed back unchanged when the queue is full (backpressure).
    pub fn push(&self, v: T) -> Result<(), T> {
        let mut pos = self.enqueue_pos.0.load(Ordering::Relaxed);
        loop {
            let cell = &self.buf[pos & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let dif = seq.wrapping_sub(pos) as isize;
            if dif == 0 {
                match self.enqueue_pos.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: a successful CAS means this thread exclusively owns the slot for one round
                        // (seq == pos means the slot is writable); after the write, the Release seq store
                        // below publishes it to consumers.
                        unsafe { cell.data.as_ptr().cast_mut().write(v) };
                        cell.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(p) => pos = p,
                }
            } else if dif < 0 {
                return Err(v); // full
            } else {
                pos = self.enqueue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Read one element; returns `None` when empty.
    pub fn pop(&self) -> Option<T> {
        let mut pos = self.dequeue_pos.0.load(Ordering::Relaxed);
        loop {
            let cell = &self.buf[pos & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let dif = seq.wrapping_sub(pos.wrapping_add(1)) as isize;
            if dif == 0 {
                match self.dequeue_pos.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: a successful CAS means this thread exclusively owns the slot for one round
                        // (seq == pos+1 means the slot holds published data);
                        // reading it out takes ownership.
                        let v = unsafe { cell.data.as_ptr().read() };
                        cell.seq.store(pos.wrapping_add(self.mask + 1), Ordering::Release);
                        return Some(v);
                    }
                    Err(p) => pos = p,
                }
            } else if dif < 0 {
                return None; // empty
            } else {
                pos = self.dequeue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Approximate number of elements (a lower-bound estimate under concurrency).
    pub fn len(&self) -> usize {
        let e = self.enqueue_pos.0.load(Ordering::Acquire);
        let d = self.dequeue_pos.0.load(Ordering::Acquire);
        e.saturating_sub(d)
    }

    /// Whether the queue is (approximately) empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Actual capacity (power of two).
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Drop for MpmcRing<T> {
    fn drop(&mut self) {
        // single-threaded context; drop the remaining elements in order
        let mut d = *self.dequeue_pos.0.get_mut();
        let e = *self.enqueue_pos.0.get_mut();
        while d != e {
            // SAFETY: slots in [dequeue, enqueue) hold initialized T;
            // this is the last access (Drop).
            unsafe { self.buf[d & self.mask].data.as_mut_ptr().drop_in_place() };
            d = d.wrapping_add(1);
        }
    }
}

// SAFETY: the classic Vyukov bounded MPMC protocol: slot state is synchronized via
// per-cell seq with Acquire/Release, and position advancement uses CAS for uniqueness — safe under arbitrary multi-threaded concurrency.
unsafe impl<T: Send> Send for MpmcRing<T> {}
// SAFETY: same as above.
unsafe impl<T: Send> Sync for MpmcRing<T> {}
