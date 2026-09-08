//! MemTable：内存中的可写表，满则 seal 为 segment。
//!
//! 每序列的样本缓冲（`Vec<Sample>`，创建时预保留容量）存放在
//! [`rti_mem::SlabPool`] 中——序列槽位 O(1) 分配/回收，无系统调用；
//! 稳态写入路径只发生 `Vec::push`（摊还 O(1)，容量预保留）。

use std::collections::BTreeMap;

use rti_core::{Error, Result, Sample, SeriesId, Timestamp};
use rti_mem::SlabPool;

/// 每个序列缓冲的初始保留容量。
const SERIES_BUF_CAP: usize = 256;

/// 索引条目：slab 槽位 + 最近使用序号（LRU 时钟）。
#[derive(Clone, Copy, Debug)]
struct SeriesEntry {
    slot: usize,
    last_used: u64,
}

/// 内存表：`SeriesId → 按 ts 排序的 Sample 序列`。
///
/// 达到 `max_samples` 后 `insert` 返回 [`Error::SeriesFull`]，
/// 调用方应 `take()` 取走数据 seal 落盘并复用本表。
///
/// v0.3：新增 [`MemTable::insert_lru`]——满时不报错，而是按 LRU
/// （写入触达时钟）丢弃最老序列并计数，供 rti-db 确定性档使用；
/// 被丢弃序列的缓冲经 spare 池复用，稳态无堆分配。
pub struct MemTable {
    /// series → 索引条目。
    index: BTreeMap<SeriesId, SeriesEntry>,
    /// 序列样本缓冲池（预分配槽位）。
    pool: SlabPool<Vec<Sample>>,
    max_samples: usize,
    len: usize,
    /// LRU 逻辑时钟（每次插入触达 +1）。
    tick: u64,
    /// 被 LRU 丢弃的序列数（累计）。
    evicted: u64,
    /// 丢弃序列留下的缓冲（复用，避免稳态反复 malloc/free）。
    spare: Vec<Vec<Sample>>,
}

impl MemTable {
    /// 创建容量为 `max_samples` 个采样点、最多 `max_series` 条序列的表。
    pub fn new(max_samples: usize, max_series: usize) -> Self {
        Self {
            index: BTreeMap::new(),
            pool: SlabPool::with_capacity(max_series.max(1)),
            max_samples,
            len: 0,
            tick: 0,
            evicted: 0,
            spare: Vec::new(),
        }
    }

    /// 取一个序列缓冲（spare 复用优先，否则新建）。
    fn take_buf(&mut self) -> Vec<Sample> {
        self.spare.pop().unwrap_or_else(|| Vec::with_capacity(SERIES_BUF_CAP))
    }

    /// 插入一个采样点（O(1) 摊还）。
    ///
    /// 假定同一序列的 ts 大体递增（seal 时会排序兜底）。
    /// 表满返回 [`Error::SeriesFull`]；序列数超出池容量同样返回满。
    pub fn insert(&mut self, series: SeriesId, sample: Sample) -> Result<()> {
        if self.len >= self.max_samples {
            return Err(Error::SeriesFull);
        }
        self.tick += 1;
        let slot = match self.index.get_mut(&series) {
            Some(e) => {
                e.last_used = self.tick;
                e.slot
            }
            None => {
                let buf = self.take_buf();
                let s = self.pool.alloc(buf).ok_or(Error::SeriesFull)?;
                self.index.insert(series, SeriesEntry { slot: s, last_used: self.tick });
                s
            }
        };
        let buf = self
            .pool
            .get_mut(slot)
            .ok_or_else(|| Error::Corrupt("memtable slot lost".into()))?;
        buf.push(sample);
        self.len += 1;
        Ok(())
    }

    /// 插入一个采样点；表满时按 LRU 丢弃最老序列（计数）再插入。
    ///
    /// 返回是否发生了丢弃。永不返回 [`Error::SeriesFull`]
    /// （`max_samples == 0` 的退化配置除外）——这是 rti-db 确定性档
    /// 「满则丢弃最老序列」语义的实现。
    ///
    /// 丢弃粒度为整条序列：被丢弃序列的全部样本从表中移除，
    /// 其缓冲进 spare 池复用。LRU 时钟按**写入**触达更新。
    pub fn insert_lru(&mut self, series: SeriesId, sample: Sample) -> Result<bool> {
        let mut evicted_now = false;
        if self.len >= self.max_samples {
            self.evict_lru()?;
            evicted_now = true;
        }
        // 丢弃已腾出样本空间与槽位；这里复用 insert 的常规路径
        // （此时不可能再 SeriesFull，除非 max_samples == 0）。
        self.insert(series, sample)?;
        Ok(evicted_now)
    }

    /// 丢弃 last_used 最小的序列（O(#series) 有界扫描）。
    fn evict_lru(&mut self) -> Result<()> {
        let (&victim, _) = self
            .index
            .iter()
            .min_by_key(|(_, e)| e.last_used)
            .ok_or_else(|| Error::Corrupt("lru evict on empty memtable".into()))?;
        let entry = self
            .index
            .remove(&victim)
            .ok_or_else(|| Error::Corrupt("memtable slot lost".into()))?;
        if let Some(buf) = self.pool.get_mut(entry.slot) {
            let mut buf = std::mem::take(buf);
            self.len -= buf.len();
            buf.clear();
            self.spare.push(buf);
        }
        let _ = self.pool.free(entry.slot);
        self.evicted += 1;
        Ok(())
    }

    /// 被 LRU 丢弃的序列总数（v0.3）。
    pub fn lru_evictions(&self) -> u64 {
        self.evicted
    }

    /// 当前采样点总数。
    pub fn len(&self) -> usize {
        self.len
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 是否已满（应 seal）。
    pub fn is_full(&self) -> bool {
        self.len >= self.max_samples
    }

    /// 表中的序列 id 列表。
    pub fn series_ids(&self) -> Vec<SeriesId> {
        self.index.keys().copied().collect()
    }

    /// 迭代某序列 `[t0, t1]` 闭区间内的样本（二分定位 + 切片，零拷贝）。
    pub fn range(&self, series: SeriesId, t0: Timestamp, t1: Timestamp) -> impl Iterator<Item = Sample> + '_ {
        let slice = self
            .index
            .get(&series)
            .and_then(|&e| self.pool.get(e.slot))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let lo = slice.partition_point(|s| s.ts < t0);
        let hi = slice.partition_point(|s| s.ts <= t1);
        slice[lo..hi.max(lo)].iter().copied()
    }

    /// 取走全部数据（用于 seal）：返回按序列分组的有序样本，表复位为空。
    pub fn take(&mut self) -> BTreeMap<SeriesId, Vec<Sample>> {
        let mut out = BTreeMap::new();
        let index = std::mem::take(&mut self.index);
        for (series, entry) in index {
            if let Some(mut buf) = self.pool.get_mut(entry.slot).map(|b| std::mem::take(b)) {
                buf.sort_by_key(|s| s.ts);
                buf.dedup_by_key(|s| s.ts);
                out.insert(series, buf);
            }
            // 槽位已清空（Vec 被 take 走），释放复用
            let _ = self.pool.free(entry.slot);
        }
        self.len = 0;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_range_query() {
        let mut mt = MemTable::new(100, 4);
        for i in 0..10 {
            mt.insert(1, Sample::new(i * 10, i as f64)).unwrap();
        }
        mt.insert(2, Sample::new(0, 100.0)).unwrap();
        let got: Vec<Sample> = mt.range(1, 20, 50).collect();
        assert_eq!(got.len(), 4); // ts = 20,30,40,50
        assert_eq!(got[0].ts, 20);
        assert_eq!(got[3].value, 5.0);
        assert_eq!(mt.range(9, 0, 100).count(), 0); // 未知序列
    }

    #[test]
    fn full_returns_series_full_and_take_reuses() {
        let mut mt = MemTable::new(4, 2);
        for i in 0..4 {
            mt.insert(1, Sample::new(i, 0.0)).unwrap();
        }
        assert!(mt.is_full());
        assert!(matches!(mt.insert(1, Sample::new(9, 0.0)), Err(Error::SeriesFull)));
        let data = mt.take();
        assert_eq!(data.get(&1).unwrap().len(), 4);
        assert!(mt.is_empty());
        // 复位后可继续写入
        mt.insert(1, Sample::new(100, 1.0)).unwrap();
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn take_sorts_and_dedups() {
        let mut mt = MemTable::new(16, 2);
        mt.insert(1, Sample::new(30, 3.0)).unwrap();
        mt.insert(1, Sample::new(10, 1.0)).unwrap();
        mt.insert(1, Sample::new(20, 2.0)).unwrap();
        mt.insert(1, Sample::new(20, 2.5)).unwrap(); // 重复 ts
        let data = mt.take();
        let v = data.get(&1).unwrap();
        assert_eq!(v.iter().map(|s| s.ts).collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    /// v0.3：LRU 丢弃最久未写的序列，其余序列数据无损，计数正确。
    #[test]
    fn insert_lru_evicts_least_recently_written_series() {
        let mut mt = MemTable::new(4, 4); // 容量 4 样本
        mt.insert_lru(1, Sample::new(1, 1.0)).unwrap(); // tick 1
        mt.insert_lru(2, Sample::new(1, 2.0)).unwrap(); // tick 2
        mt.insert_lru(1, Sample::new(2, 1.5)).unwrap(); // tick 3 → 序列 1 变新
        mt.insert_lru(3, Sample::new(1, 3.0)).unwrap(); // tick 4，表满
        assert!(mt.is_full());
        assert_eq!(mt.lru_evictions(), 0);

        // 第 5 个样本 → 丢弃 last_used 最小的序列 2
        let evicted = mt.insert_lru(3, Sample::new(2, 3.5)).unwrap();
        assert!(evicted);
        assert_eq!(mt.lru_evictions(), 1);
        assert_eq!(mt.len(), 4, "丢弃 1 个样本再插入 1 个，仍为满表 4");
        assert_eq!(mt.range(2, 0, 100).count(), 0, "序列 2 已被整体丢弃");
        assert_eq!(mt.range(1, 0, 100).count(), 2, "序列 1 数据无损");
        assert_eq!(mt.range(3, 0, 100).count(), 2);
    }

    /// v0.3：持续写入永不 SeriesFull；spare 池复用使容量有界。
    #[test]
    fn insert_lru_never_full_and_reuses_buffers() {
        let mut mt = MemTable::new(8, 2); // 小表 + 2 序列槽
        let mut evictions = 0u64;
        for i in 0..1000i64 {
            if mt.insert_lru((i % 2) as u32, Sample::new(i, i as f64)).unwrap() {
                evictions += 1;
            }
        }
        assert!(evictions > 0, "小表必须发生丢弃");
        assert_eq!(mt.lru_evictions(), evictions);
        assert!(mt.len() <= 8);
        // 丢弃-复用循环后表仍可用
        let recent: Vec<Sample> = mt.range(1, 0, 1000).collect();
        assert!(!recent.is_empty());
        assert!(recent.iter().all(|s| s.ts % 2 == 1));
    }

    /// v0.3：take 复位后 LRU 计数保留（累计语义），spare 不清空。
    #[test]
    fn take_preserves_eviction_counter() {
        let mut mt = MemTable::new(2, 2);
        mt.insert_lru(1, Sample::new(1, 0.0)).unwrap();
        mt.insert_lru(1, Sample::new(2, 0.0)).unwrap(); // 满（2 样本）
        mt.insert_lru(2, Sample::new(1, 0.0)).unwrap(); // 丢弃序列 1
        assert_eq!(mt.lru_evictions(), 1);
        let data = mt.take();
        assert_eq!(data.len(), 1);
        assert_eq!(mt.lru_evictions(), 1, "take 不清计数");
        mt.insert_lru(9, Sample::new(1, 0.0)).unwrap(); // spare 复用路径
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn series_slot_pool_exhaustion_is_full() {
        let mut mt = MemTable::new(100, 2); // 池仅 2 槽
        mt.insert(1, Sample::new(0, 0.0)).unwrap();
        mt.insert(2, Sample::new(0, 0.0)).unwrap();
        assert!(matches!(mt.insert(3, Sample::new(0, 0.0)), Err(Error::SeriesFull)));
    }
}
