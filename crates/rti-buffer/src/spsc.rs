//! 无锁 SPSC ring buffer：容量为 2 的幂，head/tail 分处独立 cache line。
//!
//! 单线程用法直接 [`SpscRing::push`]/[`SpscRing::pop`]；
//! 跨线程用法则 [`SpscRing::split`] 为 [`SpscProducer`]/[`SpscConsumer`]，
//! 两端各自缓存对端游标，绝大多数操作只需一次原子读。

use core::cell::Cell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::CacheLine;

/// 共享内核：定长存储 + 分 cache line 的头尾游标（单调递增，回绕靠掩码）。
struct RingCore<T> {
    buf: Box<[MaybeUninit<T>]>,
    mask: usize,
    /// 消费者游标（下次读取位置）。
    head: CacheLine<AtomicUsize>,
    /// 生产者游标（下次写入位置）。
    tail: CacheLine<AtomicUsize>,
}

impl<T> RingCore<T> {
    fn new(n: usize) -> Self {
        let cap = n.max(1).next_power_of_two();
        let mut buf = Vec::with_capacity(cap);
        // SAFETY: MaybeUninit<T> 无需初始化；槽位仅在 tail/head 界定的
        // 已写入区间内被读取，由下方的原子协议保证。
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

    /// 生产端原语：`cached_head` 为本地缓存的对端游标，返回新缓存值。
    #[inline]
    fn push(&self, v: T, tail: usize, cached_head: &mut usize) -> Result<(), T> {
        if tail.wrapping_sub(*cached_head) >= self.capacity() {
            // 缓存过期，重新加载真实 head
            *cached_head = self.head.0.load(Ordering::Acquire);
            if tail.wrapping_sub(*cached_head) >= self.capacity() {
                return Err(v);
            }
        }
        let idx = tail & self.mask;
        // SAFETY: SPSC 协议保证槽位 idx 当前无未读数据（已做满检查），
        // 且只有生产者会写该槽位；buf 长度即 capacity，idx < capacity。
        unsafe { self.buf[idx].as_ptr().cast_mut().write(v) };
        // Release 发布数据，使消费者读到 tail 后必然读到数据。
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// 消费端原语：`cached_tail` 为本地缓存的对端游标，返回新缓存值。
    #[inline]
    fn pop(&self, head: usize, cached_tail: &mut usize) -> Option<T> {
        if head == *cached_tail {
            *cached_tail = self.tail.0.load(Ordering::Acquire);
            if head == *cached_tail {
                return None;
            }
        }
        let idx = head & self.mask;
        // SAFETY: SPSC 协议保证槽位 idx 存有已写入且未消费的数据
        // （已做空检查，tail 以 Release 发布），只有消费者会读该槽位。
        let v = unsafe { self.buf[idx].as_ptr().read() };
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(v)
    }
}

impl<T> Drop for RingCore<T> {
    fn drop(&mut self) {
        // 析构仍留在 ring 中的元素
        let mut h = *self.head.0.get_mut();
        let t = *self.tail.0.get_mut();
        while h != t {
            // SAFETY: [head, tail) 区间内的槽位均存有已初始化的 T；
            // 此处为最后一次访问（Drop），逐个读走即析构。
            unsafe { self.buf[h & self.mask].as_mut_ptr().drop_in_place() };
            h = h.wrapping_add(1);
        }
    }
}

// SAFETY: RingCore 的协议是严格 SPSC——生产端只写 tail/槽位、
// 消费端只写 head；两端经 Acquire/Release 配对同步，无数据竞争。
unsafe impl<T: Send> Send for RingCore<T> {}
// SAFETY: 同上；同一时刻生产/消费各只有一个端点访问。
unsafe impl<T: Send> Sync for RingCore<T> {}

/// 单生产者单消费者 ring buffer（单线程句柄）。
///
/// 容量向上取整为 2 的幂；`push`/`pop`/`len` 均为 O(1) 无系统调用。
pub struct SpscRing<T> {
    core: Arc<RingCore<T>>,
    tail: usize,
    head: usize,
    cached_head: usize,
    cached_tail: usize,
}

impl<T> SpscRing<T> {
    /// 创建容量至少为 `n` 的 ring（实际容量为 2 的幂）。
    pub fn with_capacity(n: usize) -> Self {
        Self {
            core: Arc::new(RingCore::new(n)),
            tail: 0,
            head: 0,
            cached_head: 0,
            cached_tail: 0,
        }
    }

    /// 写入一个元素；ring 满则原样退回（背压）。
    #[inline]
    pub fn push(&mut self, v: T) -> Result<(), T> {
        self.core.push(v, self.tail, &mut self.cached_head)?;
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }

    /// 读出一个元素；空返回 `None`。
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let v = self.core.pop(self.head, &mut self.cached_tail)?;
        self.head = self.head.wrapping_add(1);
        Some(v)
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

    /// 实际容量（2 的幂）。
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// 拆分为跨线程的生产/消费端点（单线程句柄不可再用）。
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

/// SPSC 生产端点（可跨线程移动，但同一时刻只能有一个）。
///
/// 内部用 `Cell` 做游标缓存，故 `push` 只需 `&self`，便于门面
/// `Db::put(&self)` 的不可变签名；`Cell` 使其天然 `!Sync`，
/// 从类型上防止两个线程同时生产。
pub struct SpscProducer<T> {
    core: Arc<RingCore<T>>,
    tail: Cell<usize>,
    cached_head: Cell<usize>,
}

impl<T> SpscProducer<T> {
    /// 写入一个元素；ring 满则原样退回（背压）。
    #[inline]
    pub fn push(&self, v: T) -> Result<(), T> {
        let mut cached = self.cached_head.get();
        self.core.push(v, self.tail.get(), &mut cached)?;
        self.cached_head.set(cached);
        self.tail.set(self.tail.get().wrapping_add(1));
        Ok(())
    }

    /// 剩余可写空间（保守值）。
    #[inline]
    pub fn free(&self) -> usize {
        self.core.capacity() - self.core.len()
    }
}

/// SPSC 消费端点（可跨线程移动，但同一时刻只能有一个）。
pub struct SpscConsumer<T> {
    core: Arc<RingCore<T>>,
    head: Cell<usize>,
    cached_tail: Cell<usize>,
}

impl<T> SpscConsumer<T> {
    /// 读出一个元素；空返回 `None`。
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let mut cached = self.cached_tail.get();
        let v = self.core.pop(self.head.get(), &mut cached)?;
        self.cached_tail.set(cached);
        self.head.set(self.head.get().wrapping_add(1));
        Some(v)
    }
}

// SAFETY: Cell 使端点 !Sync，同一端点不会被两个线程共享；
// 生产/消费各一个端点时满足 RingCore 的 SPSC 协议（T 可跨线程移动）。
unsafe impl<T: Send> Send for SpscProducer<T> {}
// SAFETY: 同上。
unsafe impl<T: Send> Send for SpscConsumer<T> {}
