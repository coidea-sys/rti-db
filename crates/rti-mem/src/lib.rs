//! rti-mem：确定性内存池。
//!
//! - [`BumpArena`]：预分配定长 arena，`alloc` 只是指针碰撞（bump），
//!   永不逐个 free，`reset()` O(1) 整体回收。
//! - [`SlabPool<T>`]：固定大小对象池，`alloc`/`free` 均 O(1) 且无系统调用。
//!
//! 这是 SPEC 允许使用 unsafe 的两个 crate 之一；每处 unsafe 均附
//! `// SAFETY:` 注释说明不变量。
//!
//! ## no_std（v0.3）
//!
//! 默认 feature `std` 关闭时本 crate 为 `no_std`（`core` + `alloc`），
//! [`BumpArena`] / [`SlabPool`] API 完全一致。

#![allow(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod arena;
mod slab;

pub use arena::BumpArena;
pub use slab::SlabPool;

/// 计数全局分配器（feature `alloc-count`，仅测试用，v0.3）。
///
/// 用法（集成测试二进制中注册为全局分配器）：
///
/// ```ignore
/// #[global_allocator]
/// static A: rti_mem::alloc_count::CountingAllocator =
///     rti_mem::alloc_count::CountingAllocator;
/// ```
///
/// 之后 [`alloc_count::alloc_count`] 返回进程累计分配次数，
/// 用于验证热路径稳态 0 堆分配增长。计数本身是一次 `Relaxed`
/// 原子加，不改变分配语义（直接委托 `std::alloc::System`）。
#[cfg(feature = "alloc-count")]
pub mod alloc_count {
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::alloc::{GlobalAlloc, Layout, System};

    /// 计数分配器：每次 `alloc` 使计数 +1（`dealloc` 不计）。
    pub struct CountingAllocator;

    static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

    // SAFETY: 所有方法原样委托 `System`（其本身满足 GlobalAlloc
    // 全部不变量）；附加操作仅为无内存序要求的计数。
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            // SAFETY: layout 原样传递，与调用方的约定一致。
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: ptr/layout 来自本分配器的 alloc（调用方保证），
            // 原样委托 System。
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    /// 进程累计堆分配次数（本分配器注册为全局分配器后）。
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
        // reset 后可以从头再分配
        let p3 = a.alloc(Layout::new::<u64>()).unwrap();
        assert_eq!(p3, p1);
    }

    #[test]
    fn arena_respects_alignment_and_exhaustion() {
        let mut a = BumpArena::new(64);
        let layout = Layout::from_size_align(16, 16).unwrap();
        let p = a.alloc(layout).unwrap();
        assert_eq!(p.as_ptr() as usize % 16, 0);
        // 64 字节放不下 5 个 16 字节块
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
        assert!(pool.alloc(3).is_none()); // 满
        *pool.get_mut(s1).unwrap() = 10;
        assert_eq!(*pool.get(s1).unwrap(), 10);
        pool.free(s1).unwrap();
        let s3 = pool.alloc(3).unwrap(); // 复用被释放的槽位
        assert_eq!(s3, s1);
        assert_eq!(*pool.get(s3).unwrap(), 3);
        assert_eq!(*pool.get(s2).unwrap(), 2);
    }

    #[test]
    fn slab_double_free_and_invalid_access_rejected() {
        let mut pool = SlabPool::<u32>::with_capacity(1);
        let s = pool.alloc(7).unwrap();
        pool.free(s).unwrap();
        assert!(pool.free(s).is_err()); // 双重释放被拒绝
        assert!(pool.get(99).is_none()); // 越界句柄
    }
}
