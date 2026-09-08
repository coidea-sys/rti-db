//! 指针碰撞式 arena：预分配、永不逐个 free、`reset()` O(1)。

use alloc::vec::Vec;
use core::alloc::Layout;
use core::ptr::NonNull;


/// 预分配定长 bump arena。
///
/// - `alloc` 只做一次对齐加法与边界检查，O(1)，无系统调用；
/// - 不支持单独释放；`reset()` 将偏移归零，O(1) 回收全部内存。
///
/// 适用于每个 epoch（如一次 segment 编码）批量分配临时缓冲的场景。
pub struct BumpArena {
    // 持有分配以保证 Drop 时释放；分配后不再变动，故裸指针稳定。
    buf: Vec<u8>,
    start: NonNull<u8>,
    offset: usize,
}

impl BumpArena {
    /// 创建容量为 `cap` 字节的 arena（一次性分配，之后不再 malloc）。
    pub fn new(cap: usize) -> Self {
        let mut buf = Vec::with_capacity(cap);
        // SAFETY: `with_capacity(cap)` 已保留 `cap` 字节的连续内存；
        // 该 Vec 之后只读不写、不会扩容，因此裸指针在整个生命周期内有效。
        let start = unsafe { NonNull::new_unchecked(buf.as_mut_ptr()) };
        Self { buf, start, offset: 0 }
    }

    /// 按 `layout` 分配一块内存，空间不足返回 `None`（确定性拒绝，不扩容）。
    pub fn alloc(&mut self, layout: Layout) -> Option<NonNull<u8>> {
        let base = self.start.as_ptr() as usize;
        let aligned = (base + self.offset + layout.align() - 1) & !(layout.align() - 1);
        let new_offset = aligned - base + layout.size();
        if new_offset > self.buf.capacity() {
            return None;
        }
        self.offset = new_offset;
        // SAFETY: `aligned` 位于 `[base, base + capacity)` 内（上方边界检查），
        // 且满足 `layout.align()` 对齐；内存来自仍存活的 `self.buf`。
        Some(unsafe { NonNull::new_unchecked(aligned as *mut u8) })
    }

    /// O(1) 回收全部内存：仅将偏移归零，不触碰已分配字节。
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    /// 已使用字节数。
    pub fn used(&self) -> usize {
        self.offset
    }

    /// 剩余可用字节数（不含对齐填充的保守估计）。
    pub fn remaining(&self) -> usize {
        self.buf.capacity() - self.offset
    }

    /// 总容量。
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

// SAFETY: arena 独占其缓冲区，方法均需 `&mut self`；
// 返回的裸指针由调用方保证不越界、不重叠使用。
unsafe impl Send for BumpArena {}
