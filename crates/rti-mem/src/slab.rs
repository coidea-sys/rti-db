//! Fixed-size object pool: `alloc`/`free` are both O(1) with no syscalls.

use core::mem::MaybeUninit;

use alloc::vec;
use alloc::vec::Vec;

/// Fixed-size object slab pool.
///
/// Pre-allocates `cap` slots and the free stack once at construction; on the hot path
/// `alloc`/`free` are just stack pop/push — no syscalls, no heap allocation.
/// Handles are slot indices; `get`/`get_mut` callers must ensure the handle is valid (invalid returns `None`).
pub struct SlabPool<T> {
    slots: Vec<MaybeUninit<T>>,
    /// Stack of free slot indices, fully reserved at construction.
    free: Vec<usize>,
    /// Occupancy flags, used to reject double frees.
    occupied: Vec<bool>,
}

impl<T> SlabPool<T> {
    /// Pre-allocate a pool of `cap` slots.
    pub fn with_capacity(cap: usize) -> Self {
        let mut slots = Vec::with_capacity(cap);
        // SAFETY: MaybeUninit<T> needs no initialization; after setting len to cap every
        // slot is valid uninitialized storage, written only on alloc and read/dropped
        // only while occupied[i] is true.
        unsafe { slots.set_len(cap) };
        let mut free = Vec::with_capacity(cap);
        // Push in reverse order so the first alloc gets slot 0 (easier testing and debugging).
        for i in (0..cap).rev() {
            free.push(i);
        }
        Self { slots, free, occupied: vec![false; cap] }
    }

    /// Allocate a slot in O(1), store `v`, and return the slot handle; returns `None` when the pool is full.
    pub fn alloc(&mut self, v: T) -> Option<usize> {
        let idx = self.free.pop()?;
        // SAFETY: idx comes from the free stack, so it is < slots.len() and currently
        // unoccupied (occupied[idx] == false); writing to an uninitialized slot is sound.
        unsafe { self.slots[idx].as_mut_ptr().write(v) };
        self.occupied[idx] = true;
        Some(idx)
    }

    /// Free a slot in O(1). Returns `Err` on an invalid handle or an already-freed slot (double free).
    pub fn free(&mut self, idx: usize) -> Result<(), FreeError> {
        if idx >= self.slots.len() {
            return Err(FreeError::InvalidHandle);
        }
        if !self.occupied[idx] {
            return Err(FreeError::DoubleFree);
        }
        // SAFETY: occupied[idx] is true, so the slot holds an initialized T; here it is
        // dropped and the slot is marked free.
        unsafe { self.slots[idx].as_mut_ptr().drop_in_place() };
        self.occupied[idx] = false;
        self.free.push(idx);
        Ok(())
    }

    /// Borrow a slot; returns `None` for an invalid or unoccupied handle.
    pub fn get(&self, idx: usize) -> Option<&T> {
        if idx >= self.slots.len() || !self.occupied[idx] {
            return None;
        }
        // SAFETY: occupied[idx] is true, so the slot is initialized and can be borrowed safely.
        Some(unsafe { &*self.slots[idx].as_ptr() })
    }

    /// Borrow a slot mutably; returns `None` for an invalid or unoccupied handle.
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
        if idx >= self.slots.len() || !self.occupied[idx] {
            return None;
        }
        // SAFETY: occupied[idx] is true, so the slot is initialized; &mut self guarantees exclusivity.
        Some(unsafe { &mut *self.slots[idx].as_mut_ptr() })
    }

    /// Number of allocated slots.
    pub fn len(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total number of slots.
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }
}

impl<T> Drop for SlabPool<T> {
    fn drop(&mut self) {
        for i in 0..self.slots.len() {
            if self.occupied[i] {
                // SAFETY: occupied[i] is true, so the slot holds an initialized T, and this is
                // the last access (Drop).
                unsafe { self.slots[i].as_mut_ptr().drop_in_place() };
            }
        }
    }
}

/// Why `free` failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreeError {
    /// Handle out of bounds.
    InvalidHandle,
    /// Slot already freed.
    DoubleFree,
}

// SAFETY: SlabPool owns all its slots; the API prevents data races via the &/&mut
// SAFETY: SlabPool owns all its slots; the API prevents data races via the &/&mut borrowing rules, giving the same Send/Sync conditions as Vec<T>.
unsafe impl<T: Send> Send for SlabPool<T> {}
// SAFETY: same as above; shared references only expose `&T`, hence `T: Sync` is required.
unsafe impl<T: Sync> Sync for SlabPool<T> {}
