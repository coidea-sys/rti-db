//! const-generic SPSC ring（v0.3，no_std 友好）：存储内联在结构体内。
//!
//! 与堆分配版 [`crate::SpscRing`] 的区别：
//! - 存储为 `[MaybeUninit<T>; N]`，**零堆分配**——可放在静态区、
//!   栈上或调用方提供的任何内存中（no_std 下容量由调用方承担）；
//! - 单线程句柄（`&mut self` 方法），游标为普通 `usize`，无原子操作；
//!   适合嵌入式单核/中断-主循环这类天然 SPSC 场景。

use core::mem::MaybeUninit;

/// 存储内联的 SPSC ring，容量 `N` 必须是 2 的幂且 ≥ 2。
///
/// `push`/`pop`/`len` 均为 O(1)、无系统调用、无堆分配。
pub struct SpscRingN<T, const N: usize> {
    buf: [MaybeUninit<T>; N],
    /// 消费者游标（单调递增，回绕靠掩码）。
    head: usize,
    /// 生产者游标。
    tail: usize,
}

impl<T, const N: usize> SpscRingN<T, N> {
    /// 创建空 ring。
    ///
    /// # Panics
    /// `N < 2` 或 `N` 不是 2 的幂时 panic（构造期一次性检查）。
    pub fn new() -> Self {
        assert!(N >= 2 && N.is_power_of_two(), "SpscRingN: N 必须是 >=2 的 2 的幂");
        Self {
            // from_fn 逐项构造，MaybeUninit 无需初始化。
            buf: core::array::from_fn(|_| MaybeUninit::uninit()),
            head: 0,
            tail: 0,
        }
    }

    /// 容量（= N）。
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// 当前元素个数。
    #[inline]
    pub fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head)
    }

    /// 是否为空。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 是否已满。
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= N
    }

    /// 写入一个元素；ring 满则原样退回（背压）。
    #[inline]
    pub fn push(&mut self, v: T) -> Result<(), T> {
        if self.is_full() {
            return Err(v);
        }
        let idx = self.tail & (N - 1);
        // SAFETY: 满检查保证槽位 idx 当前无未读数据；单线程 &mut self
        // 保证无并发访问；idx < N 恒成立（掩码）。
        unsafe { self.buf[idx].as_mut_ptr().write(v) };
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }

    /// 读出一个元素；空返回 `None`。
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let idx = self.head & (N - 1);
        // SAFETY: 空检查保证槽位 idx 存有已写入且未消费的数据；
        // 单线程 &mut self 保证无并发访问；读出即取走所有权。
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
        // 析构仍留在 ring 中的元素
        let mut h = self.head;
        while h != self.tail {
            // SAFETY: [head, tail) 区间内的槽位均存有已初始化的 T；
            // 此处为最后一次访问（Drop），逐个读走即析构。
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
        assert_eq!(r.push(99), Err(99), "满必须背压退回");
        for i in 0..8 {
            assert_eq!(r.pop(), Some(i), "必须 FIFO");
        }
        assert!(r.is_empty());
        assert_eq!(r.pop(), None);
    }

    /// 回绕测试：多轮填满-掏空后游标回绕仍正确。
    #[test]
    fn wraparound_preserves_order() {
        let mut r = SpscRingN::<u64, 4>::new();
        let mut next_in = 0u64;
        let mut next_out = 0u64;
        for _ in 0..10_000 {
            // 随机深度的填/掏混合
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

    /// Drop 必须恰好析构遗留元素一次。
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
            r.pop(); // 消费 1 个（立即析构）
        }
        assert_eq!(drops.load(Ordering::SeqCst), 5, "5 个元素各析构一次");
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
