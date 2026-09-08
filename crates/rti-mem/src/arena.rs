//! Pointer-bump arena: pre-allocated, never frees individually, `reset()` is O(1).

use alloc::vec::Vec;
use core::alloc::Layout;
use core::ptr::NonNull;


/// Pre-allocated fixed-size bump arena.
///
/// - `alloc` is a single aligned addition plus a bounds check: O(1), no syscalls;
/// - individual frees are unsupported; `reset()` zeroes the offset, reclaiming all memory in O(1).
///
/// Suited for batch-allocating temporary buffers per epoch (e.g. one segment encoding).
pub struct BumpArena {
    // Hold the allocation so it is freed on Drop; it never changes after allocation, so the raw pointer stays stable.
    buf: Vec<u8>,
    start: NonNull<u8>,
    offset: usize,
}

impl BumpArena {
    /// Create an arena with a capacity of `cap` bytes (allocated once; never mallocs again).
    pub fn new(cap: usize) -> Self {
        let mut buf = Vec::with_capacity(cap);
        // SAFETY: `with_capacity(cap)` has reserved `cap` bytes of contiguous memory;
        // the Vec is only read afterwards and never grows, so the raw pointer stays valid for its entire lifetime.
        let start = unsafe { NonNull::new_unchecked(buf.as_mut_ptr()) };
        Self { buf, start, offset: 0 }
    }

    /// Allocate a block per `layout`; returns `None` when out of space (deterministic refusal, never grows).
    pub fn alloc(&mut self, layout: Layout) -> Option<NonNull<u8>> {
        let base = self.start.as_ptr() as usize;
        let aligned = (base + self.offset + layout.align() - 1) & !(layout.align() - 1);
        let new_offset = aligned - base + layout.size();
        if new_offset > self.buf.capacity() {
            return None;
        }
        self.offset = new_offset;
        // SAFETY: `aligned` lies within `[base, base + capacity)` (bounds check above),
        // satisfies `layout.align()` alignment, and the memory belongs to the still-live `self.buf`.
        Some(unsafe { NonNull::new_unchecked(aligned as *mut u8) })
    }

    /// Reclaim all memory in O(1): only zeroes the offset, never touches allocated bytes.
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    /// Number of bytes used.
    pub fn used(&self) -> usize {
        self.offset
    }

    /// Remaining usable bytes (conservative estimate excluding alignment padding).
    pub fn remaining(&self) -> usize {
        self.buf.capacity() - self.offset
    }

    /// Total capacity.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

// SAFETY: the arena exclusively owns its buffer and all methods take `&mut self`;
// callers must guarantee the returned raw pointers are never used out of bounds or overlapping.
unsafe impl Send for BumpArena {}
