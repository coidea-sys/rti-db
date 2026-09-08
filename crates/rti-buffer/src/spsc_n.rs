//! const-generic SPSC ring (v0.3, no_std friendly): storage is inline in the struct.
//!
//! Differences from the heap-allocated [`crate::SpscRing`]:
//! - storage is `[MaybeUninit<T>; N]`, **zero heap allocation** — it can live in static
//!   memory, on the stack, or in any caller-provided memory (under no_std the caller provides the capacity);
//! - single-threaded handle (`&mut self` methods) with plain `usize` cursors and no atomics;
//!   ideal for naturally-SPSC scenarios such as embedded single-core / interrupt-vs-main-loop designs.

use core::mem::MaybeUninit;

/// SPSC ring with inline storage; capacity `N` must be a power of two and >= 2.
///
/// `push`/`pop`/`len` are all O(1), with no syscalls and no heap allocation.
pub struct SpscRingN<T, const N: usize> {
    buf: [MaybeUninit<T>; N],
    /// Consumer cursor (monotonically increasing, wrapping via mask).
    head: usize,
    /// Producer cursor.
    tail: usize,
}

impl<T, const N: usize> SpscRingN<T, N> {
    /// Create an empty ring.
    ///
    /// # Panics
    /// Panics when `N < 2` or `N` is not a power of two (one-time check at construction).
    pub fn new() -> Self {
        assert!(N >= 2 && N.is_power_of_two(), "SpscRingN: N must be a power of two >= 2");
        Self {
            // from_fn constructs element by element; MaybeUninit needs no initialization.
            buf: core::array::from_fn(|_| MaybeUninit::uninit()),
            head: 0,
            tail: 0,
        }
    }

    /// Capacity (= N).
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Current number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head)
    }

    /// Whether the ring is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the ring is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= N
    }

    /// Write one element; handed back unchanged when the ring is full (backpressure).
    #[inline]
    pub fn push(&mut self, v: T) -> Result<(), T> {
        if self.is_full() {
            return Err(v);
        }
        let idx = self.tail & (N - 1);
        // SAFETY: the fullness check guarantees slot idx currently holds no unread data;
        // single-threaded &mut self guarantees no concurrent access; idx < N always holds (mask).
        unsafe { self.buf[idx].as_mut_ptr().write(v) };
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }

    /// Read one element; returns `None` when empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let idx = self.head & (N - 1);
        // SAFETY: the emptiness check guarantees slot idx holds written-but-unconsumed data;
        // single-threaded &mut self guarantees no concurrent access; reading it out takes ownership.
        let v = unsafe { self.buf[idx].as_ptr().read() };
        self.head = self.head.wrapping_add(1);
        Some(v)
    }
}

impl<T, const N: usize> Default for SpscRingN<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> Drop for SpscRingN<T, N> {
    fn drop(&mut self) {
        // drop the elements still left in the ring
        let mut h = self.head;
        while h != self.tail {
            // SAFETY: slots in [head, tail) all hold initialized T; this is the last access
            // (Drop): reading each one out drops it.
            unsafe { self.buf[h & (N - 1)].as_mut_ptr().drop_in_place() };
            h = h.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn push_pop_fifo_and_full_backpressure() {
        let mut r = SpscRingN::<u32, 8>::new();
        assert_eq!(r.capacity(), 8);
        for i in 0..8 {
            r.push(i).unwrap();
        }
        assert!(r.is_full());
        assert_eq!(r.push(99), Err(99), "a full ring must push the element back (backpressure)");
        for i in 0..8 {
            assert_eq!(r.pop(), Some(i), "must be FIFO");
        }
        assert!(r.is_empty());
        assert_eq!(r.pop(), None);
    }

    /// Wrap-around test: cursors stay correct after many fill-drain rounds.
    #[test]
    fn wraparound_preserves_order() {
        let mut r = SpscRingN::<u64, 4>::new();
        let mut next_in = 0u64;
        let mut next_out = 0u64;
        for _ in 0..10_000 {
            // mixed fill/drain at random depths
            while r.push(next_in).is_ok() {
                next_in += 1;
            }
            assert_eq!(r.pop(), Some(next_out));
            next_out += 1;
        }
        assert_eq!(r.len(), (next_in - next_out) as usize);
        while let Some(v) = r.pop() {
            assert_eq!(v, next_out);
            next_out += 1;
        }
        assert_eq!(next_in, next_out);
    }

    /// Drop must drop leftover elements exactly once.
    #[test]
    fn drop_runs_element_destructors_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        #[derive(Debug)]
        struct Guard(Arc<AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        {
            let mut r = SpscRingN::<Guard, 8>::new();
            for _ in 0..5 {
                r.push(Guard(Arc::clone(&drops))).unwrap();
            }
            r.pop(); // consume 1 (dropped immediately)
        }
        assert_eq!(drops.load(Ordering::SeqCst), 5, "each of the 5 elements is dropped exactly once");
    }

    #[test]
    fn len_tracks_operations() {
        let mut r = SpscRingN::<u8, 16>::default();
        assert_eq!(r.len(), 0);
        r.push(1).unwrap();
        r.push(2).unwrap();
        assert_eq!(r.len(), 2);
        r.pop();
        assert_eq!(r.len(), 1);
        assert!(!r.is_empty() && !r.is_full());
    }
}
