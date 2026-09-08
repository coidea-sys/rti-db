//! rti-mem: deterministic memory pools.
//!
//! - [`BumpArena`]: pre-allocated fixed-size arena; `alloc` is just pointer bumping,
//!   never frees individually, and `reset()` reclaims everything in O(1).
//! - [`SlabPool<T>`]: fixed-size object pool; `alloc`/`free` are both O(1) with no syscalls.
//!
//! This is one of the two crates where the SPEC permits unsafe code; every unsafe
//! block carries a `// SAFETY:` comment explaining the invariants.
//!
//! ## no_std (v0.3)
//!
//! With the default `std` feature disabled this crate is `no_std` (`core` + `alloc`);
//! the [`BumpArena`] / [`SlabPool`] APIs are identical.

#![allow(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod arena;
mod slab;

pub use arena::BumpArena;
pub use slab::SlabPool;

/// Counting global allocator (feature `alloc-count`, test-only, v0.3).
///
/// Usage (registered as the global allocator in an integration-test binary):
///
/// ```ignore
/// #[global_allocator]
/// static A: rti_mem::alloc_count::CountingAllocator =
///     rti_mem::alloc_count::CountingAllocator;
/// ```
///
/// Afterwards [`alloc_count::alloc_count`] returns the cumulative number of process
/// allocations, used to verify zero heap-allocation growth on the steady-state hot path. Counting itself is a single
/// `Relaxed` atomic increment and does not change allocation semantics (it delegates directly to `std::alloc::System`).
#[cfg(feature = "alloc-count")]
pub mod alloc_count {
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::alloc::{GlobalAlloc, Layout, System};

    /// Counting allocator: each `alloc` increments the count by 1 (`dealloc` is not counted).
    pub struct CountingAllocator;

    static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

    // SAFETY: every method delegates verbatim to `System` (which itself satisfies all
    // GlobalAlloc invariants); the only addition is a count with no ordering requirements.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            // SAFETY: layout is passed through unchanged, matching the caller's contract.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: ptr/layout come from this allocator's alloc (guaranteed by the caller),
            // delegated verbatim to System.
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    /// Cumulative number of heap allocations in the process (after this allocator is registered globally).
    pub fn alloc_count() -> u64 {
        ALLOC_COUNT.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::alloc::Layout;

    #[test]
    fn arena_alloc_bump_and_reset() {
        let mut a = BumpArena::new(1024);
        let p1 = a.alloc(Layout::new::<u64>()).unwrap();
        let p2 = a.alloc(Layout::new::<u64>()).unwrap();
        assert_ne!(p1, p2);
        assert_eq!(a.used(), 16);
        a.reset();
        assert_eq!(a.used(), 0);
        // after reset, allocation can start over from the beginning
        let p3 = a.alloc(Layout::new::<u64>()).unwrap();
        assert_eq!(p3, p1);
    }

    #[test]
    fn arena_respects_alignment_and_exhaustion() {
        let mut a = BumpArena::new(64);
        let layout = Layout::from_size_align(16, 16).unwrap();
        let p = a.alloc(layout).unwrap();
        assert_eq!(p.as_ptr() as usize % 16, 0);
        // 64 bytes cannot hold five 16-byte blocks
        assert!(a.alloc(layout).is_some());
        assert!(a.alloc(layout).is_some());
        assert!(a.alloc(layout).is_some());
        assert!(a.alloc(layout).is_none());
    }

    #[test]
    fn slab_alloc_free_reuse() {
        let mut pool = SlabPool::<u64>::with_capacity(2);
        let s1 = pool.alloc(1).unwrap();
        let s2 = pool.alloc(2).unwrap();
        assert!(pool.alloc(3).is_none()); // full
        *pool.get_mut(s1).unwrap() = 10;
        assert_eq!(*pool.get(s1).unwrap(), 10);
        pool.free(s1).unwrap();
        let s3 = pool.alloc(3).unwrap(); // reuse the freed slot
        assert_eq!(s3, s1);
        assert_eq!(*pool.get(s3).unwrap(), 3);
        assert_eq!(*pool.get(s2).unwrap(), 2);
    }

    #[test]
    fn slab_double_free_and_invalid_access_rejected() {
        let mut pool = SlabPool::<u32>::with_capacity(1);
        let s = pool.alloc(7).unwrap();
        pool.free(s).unwrap();
        assert!(pool.free(s).is_err()); // double free is rejected
        assert!(pool.get(99).is_none()); // out-of-bounds handle
    }
}
