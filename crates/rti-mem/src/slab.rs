//! 固定大小对象池：`alloc`/`free` 均 O(1)、无系统调用。

use core::mem::MaybeUninit;

use alloc::vec;
use alloc::vec::Vec;

/// 固定大小对象 slab 池。
///
/// 构造时一次性预分配 `cap` 个槽位与空闲栈；热路径上的
/// `alloc`/`free` 只是栈 pop/push，无系统调用、无堆分配。
/// 句柄为槽位下标，`get`/`get_mut` 需自行保证句柄有效（无效返回 `None`）。
pub struct SlabPool<T> {
    slots: Vec<MaybeUninit<T>>,
    /// 空闲槽位下标栈，构造时预留全部容量。
    free: Vec<usize>,
    /// 占用标记，用于拒绝双重释放。
    occupied: Vec<bool>,
}

impl<T> SlabPool<T> {
    /// 预分配 `cap` 个槽位的池。
    pub fn with_capacity(cap: usize) -> Self {
        let mut slots = Vec::with_capacity(cap);
        // SAFETY: MaybeUninit<T> 不需要初始化；len 设为 cap 后
        // 每个槽位均为合法的未初始化存储，仅在 alloc 时写入、
        // 仅在 occupied[i] 为真时读取/析构。
        unsafe { slots.set_len(cap) };
        let mut free = Vec::with_capacity(cap);
        // 逆序压栈，使首个 alloc 拿到槽位 0（便于测试与调试）。
        for i in (0..cap).rev() {
            free.push(i);
        }
        Self { slots, free, occupied: vec![false; cap] }
    }

    /// O(1) 分配一个槽位并写入 `v`，返回槽位句柄；池满返回 `None`。
    pub fn alloc(&mut self, v: T) -> Option<usize> {
        let idx = self.free.pop()?;
        // SAFETY: idx 来自空闲栈，必然 < slots.len() 且当前未被占用
        // （occupied[idx] == false），写入未初始化槽位是合法的。
        unsafe { self.slots[idx].as_mut_ptr().write(v) };
        self.occupied[idx] = true;
        Some(idx)
    }

    /// O(1) 释放槽位。句柄无效或已释放（双重释放）返回 `Err`。
    pub fn free(&mut self, idx: usize) -> Result<(), FreeError> {
        if idx >= self.slots.len() {
            return Err(FreeError::InvalidHandle);
        }
        if !self.occupied[idx] {
            return Err(FreeError::DoubleFree);
        }
        // SAFETY: occupied[idx] 为真，槽位中存有已初始化的 T，
        // 此处将其析构并把槽位标记为空闲。
        unsafe { self.slots[idx].as_mut_ptr().drop_in_place() };
        self.occupied[idx] = false;
        self.free.push(idx);
        Ok(())
    }

    /// 读取槽位引用；句柄无效或未占用返回 `None`。
    pub fn get(&self, idx: usize) -> Option<&T> {
        if idx >= self.slots.len() || !self.occupied[idx] {
            return None;
        }
        // SAFETY: occupied[idx] 为真，槽位已初始化，可安全借用。
        Some(unsafe { &*self.slots[idx].as_ptr() })
    }

    /// 读取槽位可变引用；句柄无效或未占用返回 `None`。
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
        if idx >= self.slots.len() || !self.occupied[idx] {
            return None;
        }
        // SAFETY: occupied[idx] 为真，槽位已初始化；&mut self 保证独占。
        Some(unsafe { &mut *self.slots[idx].as_mut_ptr() })
    }

    /// 已分配槽位数。
    pub fn len(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 总槽位数。
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }
}

impl<T> Drop for SlabPool<T> {
    fn drop(&mut self) {
        for i in 0..self.slots.len() {
            if self.occupied[i] {
                // SAFETY: occupied[i] 为真，槽位存有已初始化的 T，
                // 且此处为最后一次访问（Drop）。
                unsafe { self.slots[i].as_mut_ptr().drop_in_place() };
            }
        }
    }
}

/// `free` 失败原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreeError {
    /// 句柄越界。
    InvalidHandle,
    /// 槽位已被释放。
    DoubleFree,
}

// SAFETY: SlabPool 拥有全部槽位，API 通过 &/&mut 借用规则防止数据竞争，
// 与 Vec<T> 具有相同的 Send/Sync 条件。
unsafe impl<T: Send> Send for SlabPool<T> {}
// SAFETY: 同上，共享引用只暴露 &T，故要求 T: Sync。
unsafe impl<T: Sync> Sync for SlabPool<T> {}
