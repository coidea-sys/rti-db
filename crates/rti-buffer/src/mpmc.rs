//! 有界 MPMC 队列：Vyukov 算法（per-cell sequence 数组 + CAS）。

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};

use crate::CacheLine;

/// 队列单元：sequence 号标记槽位状态（可写/可读）。
struct Cell<T> {
    seq: AtomicUsize,
    data: MaybeUninit<T>,
}

/// 有界多生产者多消费者 ring，容量为 2 的幂。
///
/// `push`/`pop` 为无锁 CAS 循环，满/空时立即返回（有界背压），
/// 热路径无堆分配、无系统调用。
pub struct MpmcRing<T> {
    buf: Box<[Cell<T>]>,
    mask: usize,
    /// 入队位置（生产者竞争）。
    enqueue_pos: CacheLine<AtomicUsize>,
    /// 出队位置（消费者竞争）。
    dequeue_pos: CacheLine<AtomicUsize>,
}

impl<T> MpmcRing<T> {
    /// 创建容量至少为 `n` 的队列（实际容量为 2 的幂）。
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

    /// 写入一个元素；队列满则原样退回（背压）。
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
                        // SAFETY: CAS 成功意味着本线程独占该槽位一轮
                        // （seq == pos 表示槽位可写），写入后由下方
                        // Release 的 seq 存储发布给消费者。
                        unsafe { cell.data.as_ptr().cast_mut().write(v) };
                        cell.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(p) => pos = p,
                }
            } else if dif < 0 {
                return Err(v); // 满
            } else {
                pos = self.enqueue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    /// 读出一个元素；空返回 `None`。
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
                        // SAFETY: CAS 成功意味着本线程独占该槽位一轮
                        // （seq == pos+1 表示槽位有已发布数据），
                        // 读出即取走所有权。
                        let v = unsafe { cell.data.as_ptr().read() };
                        cell.seq.store(pos.wrapping_add(self.mask + 1), Ordering::Release);
                        return Some(v);
                    }
                    Err(p) => pos = p,
                }
            } else if dif < 0 {
                return None; // 空
            } else {
                pos = self.dequeue_pos.0.load(Ordering::Relaxed);
            }
        }
    }

    /// 当前元素个数的近似值（并发下为下界估计）。
    pub fn len(&self) -> usize {
        let e = self.enqueue_pos.0.load(Ordering::Acquire);
        let d = self.dequeue_pos.0.load(Ordering::Acquire);
        e.saturating_sub(d)
    }

    /// 是否（近似）为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 实际容量（2 的幂）。
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Drop for MpmcRing<T> {
    fn drop(&mut self) {
        // 单线程上下文，直接顺序析构剩余元素
        let mut d = *self.dequeue_pos.0.get_mut();
        let e = *self.enqueue_pos.0.get_mut();
        while d != e {
            // SAFETY: [dequeue, enqueue) 区间内的槽位存有已初始化的 T；
            // 此处为最后一次访问（Drop）。
            unsafe { self.buf[d & self.mask].data.as_mut_ptr().drop_in_place() };
            d = d.wrapping_add(1);
        }
    }
}

// SAFETY: 经典 Vyukov 有界 MPMC 协议：槽位状态由 per-cell seq 以
// Acquire/Release 同步，位置推进用 CAS 保证唯一性，任意多线程并发安全。
unsafe impl<T: Send> Send for MpmcRing<T> {}
// SAFETY: 同上。
unsafe impl<T: Send> Sync for MpmcRing<T> {}
