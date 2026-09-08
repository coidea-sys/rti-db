//! Lock-free SPSC ring buffer: power-of-two capacity, head/tail on separate cache lines.
//!
//! Single-threaded usage calls [`SpscRing::push`]/[`SpscRing::pop`] directly;
//! cross-thread usage [`SpscRing::split`]s into [`SpscProducer`]/[`SpscConsumer`],
//! with each endpoint caching the peer cursor so most operations need only one atomic load.

use core::cell::Cell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::CacheLine;

/// Shared core: fixed-size storage + cache-line-separated head/tail cursors (monotonically increasing, wrapping via mask).
struct RingCore<T> {
    buf: Box<[MaybeUninit<T>]>,
    mask: usize,
    /// Consumer cursor (next read position).
    head: CacheLine<AtomicUsize>,
    /// Producer cursor (next write position).
    tail: CacheLine<AtomicUsize>,
}

impl<T> RingCore<T> {
    fn new(n: usize) -> Self {
        let cap = n.max(1).next_power_of_two();
        let mut buf = Vec::with_capacity(cap);
        // SAFETY: MaybeUninit<T> needs no initialization; slots are read only within the
        // written range delimited by tail/head, guaranteed by the atomic protocol below.
        unsafe { buf.set_len(cap) };
        Self {
            buf: buf.into_boxed_slice(),
            mask: cap - 1,
            head: CacheLine(AtomicUsize::new(0)),
            tail: CacheLine(AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    fn len(&self) -> usize {
        let t = self.tail.0.load(Ordering::Acquire);
        let h = self.head.0.load(Ordering::Acquire);
        t.wrapping_sub(h)
    }

    /// Producer-side primitive: `cached_head` is the locally cached peer cursor; returns the new cached value.
    #[inline]
    fn push(&self, v: T, tail: usize, cached_head: &mut usize) -> Result<(), T> {
        if tail.wrapping_sub(*cached_head) >= self.capacity() {
            // cache is stale; reload the real head
            *cached_head = self.head.0.load(Ordering::Acquire);
            if tail.wrapping_sub(*cached_head) >= self.capacity() {
                return Err(v);
            }
        }
        let idx = tail & self.mask;
        // SAFETY: the SPSC protocol guarantees slot idx currently holds no unread data (fullness checked),
        // and only the producer writes that slot; buf.len() == capacity, idx < capacity.
        unsafe { self.buf[idx].as_ptr().cast_mut().write(v) };
        // Release publishes the data, so a consumer that reads tail is guaranteed to see it.
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Consumer-side primitive: `cached_tail` is the locally cached peer cursor; returns the new cached value.
    #[inline]
    fn pop(&self, head: usize, cached_tail: &mut usize) -> Option<T> {
        if head == *cached_tail {
            *cached_tail = self.tail.0.load(Ordering::Acquire);
            if head == *cached_tail {
                return None;
            }
        }
        let idx = head & self.mask;
        // SAFETY: the SPSC protocol guarantees slot idx holds written-but-unconsumed data
        // (emptiness checked, tail published with Release); only the consumer reads that slot.
        let v = unsafe { self.buf[idx].as_ptr().read() };
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(v)
    }
}

impl<T> Drop for RingCore<T> {
    fn drop(&mut self) {
        // drop the elements still left in the ring
        let mut h = *self.head.0.get_mut();
        let t = *self.tail.0.get_mut();
        while h != t {
            // SAFETY: slots in [head, tail) all hold initialized T; this is the last access
            // (Drop): reading each one out drops it.
            unsafe { self.buf[h & self.mask].as_mut_ptr().drop_in_place() };
            h = h.wrapping_add(1);
        }
    }
}

// SAFETY: RingCore's protocol is strictly SPSC — the producer only writes tail/slots,
// the consumer only writes head; the two ends synchronize via paired Acquire/Release, so there is no data race.
unsafe impl<T: Send> Send for RingCore<T> {}
// SAFETY: same as above; at any moment only one producer and one consumer endpoint access it.
unsafe impl<T: Send> Sync for RingCore<T> {}

/// Single-producer single-consumer ring buffer (single-threaded handle).
///
/// Capacity rounds up to a power of two; `push`/`pop`/`len` are all O(1) with no syscalls.
pub struct SpscRing<T> {
    core: Arc<RingCore<T>>,
    tail: usize,
    head: usize,
    cached_head: usize,
    cached_tail: usize,
}

impl<T> SpscRing<T> {
    /// Create a ring with capacity at least `n` (actual capacity is a power of two).
    pub fn with_capacity(n: usize) -> Self {
        Self {
            core: Arc::new(RingCore::new(n)),
            tail: 0,
            head: 0,
            cached_head: 0,
            cached_tail: 0,
        }
    }

    /// Write one element; handed back unchanged when the ring is full (backpressure).
    #[inline]
    pub fn push(&mut self, v: T) -> Result<(), T> {
        self.core.push(v, self.tail, &mut self.cached_head)?;
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }

    /// Read one element; returns `None` when empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let v = self.core.pop(self.head, &mut self.cached_tail)?;
        self.head = self.head.wrapping_add(1);
        Some(v)
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

    /// Actual capacity (power of two).
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// Split into cross-thread producer/consumer endpoints (the single-threaded handle must not be used afterwards).
    pub fn split(self) -> (SpscProducer<T>, SpscConsumer<T>) {
        let p = SpscProducer {
            core: Arc::clone(&self.core),
            tail: Cell::new(self.tail),
            cached_head: Cell::new(self.cached_head),
        };
        let c = SpscConsumer {
            core: Arc::clone(&self.core),
            head: Cell::new(self.head),
            cached_tail: Cell::new(self.cached_tail),
        };
        (p, c)
    }
}

/// SPSC producer endpoint (movable across threads, but only one may exist at a time).
///
/// Uses `Cell` internally for cursor caching, so `push` takes only `&self` — convenient
/// for the immutable signature of the `Db::put(&self)` facade; `Cell` makes it naturally
/// `!Sync`, preventing two threads from producing concurrently at the type level.
pub struct SpscProducer<T> {
    core: Arc<RingCore<T>>,
    tail: Cell<usize>,
    cached_head: Cell<usize>,
}

impl<T> SpscProducer<T> {
    /// Write one element; handed back unchanged when the ring is full (backpressure).
    #[inline]
    pub fn push(&self, v: T) -> Result<(), T> {
        let mut cached = self.cached_head.get();
        self.core.push(v, self.tail.get(), &mut cached)?;
        self.cached_head.set(cached);
        self.tail.set(self.tail.get().wrapping_add(1));
        Ok(())
    }

    /// Remaining writable space (conservative value).
    #[inline]
    pub fn free(&self) -> usize {
        self.core.capacity() - self.core.len()
    }
}

/// SPSC consumer endpoint (movable across threads, but only one may exist at a time).
pub struct SpscConsumer<T> {
    core: Arc<RingCore<T>>,
    head: Cell<usize>,
    cached_tail: Cell<usize>,
}

impl<T> SpscConsumer<T> {
    /// Read one element; returns `None` when empty.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mut cached = self.cached_tail.get();
        let v = self.core.pop(self.head.get(), &mut cached)?;
        self.cached_tail.set(cached);
        self.head.set(self.head.get().wrapping_add(1));
        Some(v)
    }
}

// SAFETY: Cell makes an endpoint !Sync, so one endpoint is never shared by two threads;
// with one producer and one consumer endpoint, RingCore's SPSC protocol holds (T is movable across threads).
unsafe impl<T: Send> Send for SpscProducer<T> {}
// SAFETY: same as above.
unsafe impl<T: Send> Send for SpscConsumer<T> {}
