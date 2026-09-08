//! MemTable: the in-memory writable table, sealed into a segment when full.
//!
//! Per-series sample buffers (`Vec<Sample>`, capacity reserved at creation) live in a
//! [`rti_mem::SlabPool`] — series slots allocate/recycle in O(1) with no syscalls;
//! the steady-state write path only performs `Vec::push` (amortized O(1), capacity pre-reserved).

use std::collections::BTreeMap;

use rti_core::{Error, Result, Sample, SeriesId, Timestamp};
use rti_mem::SlabPool;

/// Initial reserved capacity of each series buffer.
const SERIES_BUF_CAP: usize = 256;

/// Index entry: slab slot + last-used sequence number (LRU clock).
#[derive(Clone, Copy, Debug)]
struct SeriesEntry {
    slot: usize,
    last_used: u64,
}

/// In-memory table: `SeriesId → ts-ordered Sample sequence`.
///
/// Once `max_samples` is reached, `insert` returns [`Error::SeriesFull`];
/// the caller should `take()` the data, seal it to disk, and reuse this table.
///
/// v0.3: adds [`MemTable::insert_lru`] — instead of erroring when full, it evicts the
/// oldest series by LRU (write-touch clock) and counts it, for rti-db's deterministic profile;
/// evicted series buffers are recycled through the spare pool — no heap allocation in steady state.
pub struct MemTable {
    /// series → index entry.
    index: BTreeMap<SeriesId, SeriesEntry>,
    /// Series sample buffer pool (pre-allocated slots).
    pool: SlabPool<Vec<Sample>>,
    max_samples: usize,
    len: usize,
    /// LRU logical clock (+1 on every insert touch).
    tick: u64,
    /// Number of series evicted by LRU (cumulative).
    evicted: u64,
    /// Buffers left behind by evicted series (reused, avoiding steady-state malloc/free churn).
    spare: Vec<Vec<Sample>>,
}

impl MemTable {
    /// Create a table holding `max_samples` samples across at most `max_series` series.
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

    /// Take a series buffer (spare reuse first, otherwise create a new one).
    fn take_buf(&mut self) -> Vec<Sample> {
        self.spare.pop().unwrap_or_else(|| Vec::with_capacity(SERIES_BUF_CAP))
    }

    /// Insert one sample (amortized O(1)).
    ///
    /// Assumes ts of the same series is roughly increasing (sealing sorts as a fallback).
    /// Returns [`Error::SeriesFull`] when the table is full; exceeding the pool's series capacity also returns full.
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

    /// Insert one sample; when the table is full, evict the oldest series by LRU (counted) first.
    ///
    /// Returns whether an eviction happened. Never returns [`Error::SeriesFull`]
    /// (except for the degenerate `max_samples == 0` configuration) — this implements the
    /// deterministic profile's 'evict the oldest series when full' semantics for rti-db.
    ///
    /// Eviction granularity is a whole series: all samples of the evicted series are removed
    /// from the table and its buffer goes to the spare pool for reuse. The LRU clock is updated on **write** touches.
    pub fn insert_lru(&mut self, series: SeriesId, sample: Sample) -> Result<bool> {
        let mut evicted_now = false;
        if self.len >= self.max_samples {
            self.evict_lru()?;
            evicted_now = true;
        }
        // the eviction freed sample space and a slot; reuse insert's regular path here
        // (SeriesFull is impossible at this point, unless max_samples == 0).
        self.insert(series, sample)?;
        Ok(evicted_now)
    }

    /// Evict the series with the smallest last_used (bounded O(#series) scan).
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

    /// Total number of series evicted by LRU (v0.3).
    pub fn lru_evictions(&self) -> u64 {
        self.evicted
    }

    /// Current total number of samples.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the table is full (should seal).
    pub fn is_full(&self) -> bool {
        self.len >= self.max_samples
    }

    /// List of series ids in the table.
    pub fn series_ids(&self) -> Vec<SeriesId> {
        self.index.keys().copied().collect()
    }

    /// Iterate a series' samples in the closed range `[t0, t1]` (binary search + slice, zero-copy).
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

    /// Take all data (for sealing): returns ordered samples grouped by series; the table resets to empty.
    pub fn take(&mut self) -> BTreeMap<SeriesId, Vec<Sample>> {
        let mut out = BTreeMap::new();
        let index = std::mem::take(&mut self.index);
        for (series, entry) in index {
            if let Some(mut buf) = self.pool.get_mut(entry.slot).map(std::mem::take) {
                buf.sort_by_key(|s| s.ts);
                buf.dedup_by_key(|s| s.ts);
                out.insert(series, buf);
            }
            // the slot is empty now (the Vec was taken away); free it for reuse
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
        assert_eq!(mt.range(9, 0, 100).count(), 0); // unknown series
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
        // writable again after reset
        mt.insert(1, Sample::new(100, 1.0)).unwrap();
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn take_sorts_and_dedups() {
        let mut mt = MemTable::new(16, 2);
        mt.insert(1, Sample::new(30, 3.0)).unwrap();
        mt.insert(1, Sample::new(10, 1.0)).unwrap();
        mt.insert(1, Sample::new(20, 2.0)).unwrap();
        mt.insert(1, Sample::new(20, 2.5)).unwrap(); // duplicate ts
        let data = mt.take();
        let v = data.get(&1).unwrap();
        assert_eq!(v.iter().map(|s| s.ts).collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    /// v0.3: LRU evicts the least-recently-written series; other series' data is intact, counts are correct.
    #[test]
    fn insert_lru_evicts_least_recently_written_series() {
        let mut mt = MemTable::new(4, 4); // capacity 4 samples
        mt.insert_lru(1, Sample::new(1, 1.0)).unwrap(); // tick 1
        mt.insert_lru(2, Sample::new(1, 2.0)).unwrap(); // tick 2
        mt.insert_lru(1, Sample::new(2, 1.5)).unwrap(); // tick 3 -> series 1 becomes newer
        mt.insert_lru(3, Sample::new(1, 3.0)).unwrap(); // tick 4, table full
        assert!(mt.is_full());
        assert_eq!(mt.lru_evictions(), 0);

        // the 5th sample -> evicts series 2, which has the smallest last_used
        let evicted = mt.insert_lru(3, Sample::new(2, 3.5)).unwrap();
        assert!(evicted);
        assert_eq!(mt.lru_evictions(), 1);
        assert_eq!(mt.len(), 4, "evict 1 sample then insert 1; still a full table of 4");
        assert_eq!(mt.range(2, 0, 100).count(), 0, "series 2 has been evicted as a whole");
        assert_eq!(mt.range(1, 0, 100).count(), 2, "series 1 data is intact");
        assert_eq!(mt.range(3, 0, 100).count(), 2);
    }

    /// v0.3: continuous writes never hit SeriesFull; spare-pool reuse keeps capacity bounded.
    #[test]
    fn insert_lru_never_full_and_reuses_buffers() {
        let mut mt = MemTable::new(8, 2); // small table + 2 series slots
        let mut evictions = 0u64;
        for i in 0..1000i64 {
            if mt.insert_lru((i % 2) as u32, Sample::new(i, i as f64)).unwrap() {
                evictions += 1;
            }
        }
        assert!(evictions > 0, "a small table must see evictions");
        assert_eq!(mt.lru_evictions(), evictions);
        assert!(mt.len() <= 8);
        // the table is still usable after evict-reuse cycles
        let recent: Vec<Sample> = mt.range(1, 0, 1000).collect();
        assert!(!recent.is_empty());
        assert!(recent.iter().all(|s| s.ts % 2 == 1));
    }

    /// v0.3: after take resets the table, the LRU count is preserved (cumulative semantics); spares are not cleared.
    #[test]
    fn take_preserves_eviction_counter() {
        let mut mt = MemTable::new(2, 2);
        mt.insert_lru(1, Sample::new(1, 0.0)).unwrap();
        mt.insert_lru(1, Sample::new(2, 0.0)).unwrap(); // full (2 samples)
        mt.insert_lru(2, Sample::new(1, 0.0)).unwrap(); // evict series 1
        assert_eq!(mt.lru_evictions(), 1);
        let data = mt.take();
        assert_eq!(data.len(), 1);
        assert_eq!(mt.lru_evictions(), 1, "take does not clear the count");
        mt.insert_lru(9, Sample::new(1, 0.0)).unwrap(); // spare reuse path
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn series_slot_pool_exhaustion_is_full() {
        let mut mt = MemTable::new(100, 2); // pool has only 2 slots
        mt.insert(1, Sample::new(0, 0.0)).unwrap();
        mt.insert(2, Sample::new(0, 0.0)).unwrap();
        assert!(matches!(mt.insert(3, Sample::new(0, 0.0)), Err(Error::SeriesFull)));
    }
}
