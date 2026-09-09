//! v0.8 segment compaction (SPEC §1): deterministic policy + merge executor.
//!
//! Scope and rules:
//!
//! - Only **local, never-archived** segments participate: any segment whose name appears in
//!   `archive.catalog` (or whose catalog entry is `Archived`) is skipped, so a compaction delete
//!   can never resurrect a cold-tier copy via the append-only catalog.
//! - Inputs are processed in existing catalog (in-memory segment list) order. The merge
//!   **preserves every sample at a duplicate timestamp**, ordered by `(ts, input catalog
//!   index)` inside the output segment. Scan collects entries in catalog order, applies
//!   predicates at the decode layer, then stable-sorts by ts and dedups — so a plain scan
//!   still yields the first writer, a predicated scan still yields the first *matching*
//!   writer, and both are sample-identical before/after a merge under arbitrary predicates.
//! - The policy prefers time-adjacent, same-series segments: catalog order equals seal order,
//!   so a maximal run of eligible same-series entries is already time-adjacent. Runs are broken
//!   at any ineligible same-series entry (archived or catalog-listed), because merging across
//!   such an entry could reorder duplicate-timestamp winners relative to scan.
//! - Merge eligibility requires `min_segments`; `max_input_bytes` greedily splits long runs.
//!   Both are deterministic functions of the catalog snapshot plus on-disk file sizes.
//!
//! Crash/concurrency write order (SPEC §1):
//!
//! 1. the merged output is written to `*.seg.tmp` and renamed into place by
//!    [`SegmentWriter`], honoring the configured [`SyncPolicy`] fsync discipline;
//! 2. the in-memory `SegEntry` set is swapped in one short critical section, after
//!    re-validating that every selected input is still present, local, and unchanged
//!    (same zone map) — a concurrent seal/archive cannot be compacted away;
//! 3. superseded input files are deleted best-effort after the swap, **newest first**
//!    (reverse catalog order), with a directory fsync after each delete per [`SyncPolicy`]
//!    (`Group`/`Always` fsync; `None` skips it — the same durability tier as
//!    `SegmentWriter::write_unsynced`, where the page cache already survives a process crash).
//!
//! A crash before the rename leaves only old data (the `.seg.tmp` file is ignored at open);
//! a crash after the rename but before the deletes leaves old+new files side by side, which
//! reopen tolerates (name order keeps the older inputs first, and scan dedups by timestamp).
//! A crash mid-delete can only have durably removed a *suffix* of the newest inputs: the
//! remaining oldest inputs still precede the output segment in name order, and the output
//! preserves the inputs' internal (ts, catalog-index) order, so duplicate-timestamp winners
//! are unchanged. All file I/O happens outside the `state` lock.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use rti_core::{Error, Profile, Result, Sample, SeriesId, SyncPolicy, Timestamp};
use rti_store::{SegmentReader, SegmentWriter, ZoneMap};

use crate::{load_catalog, SegEntry, SegLoc, Shared};

/// Cumulative compaction counters for observability (SPEC §1).
///
/// `runs` counts executed merge runs (one per emitted output segment); a `compact()` call
/// that finds nothing eligible adds nothing. Byte counters use on-disk file sizes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStats {
    /// Merge runs executed so far.
    pub runs: u64,
    /// Input segments removed by compaction so far.
    pub input_segments: u64,
    /// Output segments written by compaction so far.
    pub output_segments: u64,
    /// Total on-disk bytes of removed input segments.
    pub input_bytes: u64,
    /// Total on-disk bytes of written output segments.
    pub output_bytes: u64,
}

/// Deterministic selection policy: a run needs at least `min_segments` inputs; when
/// `max_input_bytes` is `Some`, runs are greedily split so one merge's inputs stay under it
/// (deterministic: file sizes are read from a quiesced-name snapshot, inputs in catalog order).
pub(crate) struct CompactPolicy {
    pub min_segments: usize,
    pub max_input_bytes: Option<u64>,
}

impl Default for CompactPolicy {
    fn default() -> Self {
        Self {
            min_segments: 2,
            max_input_bytes: None,
        }
    }
}

/// One selected input segment (snapshot taken under the state lock, I/O done later).
struct Input {
    /// Segment file name (unique; doubles as the identity for swap revalidation).
    name: String,
    /// Zone map at snapshot time; re-checked before the swap ("unchanged" proof).
    zone: ZoneMap,
}

/// One merge plan: same-series inputs in catalog order, plus the swap insertion point.
struct Plan {
    series: SeriesId,
    inputs: Vec<Input>,
}

/// Entry point behind `Db::compact` / `Db::compact_series`.
///
/// Returns the number of input segments removed. The Deterministic profile returns `Ok(0)`
/// without touching the filesystem (SPEC §0.5 / §1).
pub(crate) fn compact(shared: &Shared, series_filter: Option<SeriesId>) -> Result<usize> {
    if shared.config.profile == Profile::Deterministic {
        return Ok(0);
    }
    let dir = shared
        .config
        .data_dir
        .as_ref()
        .ok_or_else(|| Error::Corrupt("compact requires data_dir = Some(..)".into()))?
        .clone();
    // Serialize concurrent compact*() calls; ingest and scans are unaffected.
    let _run = shared.compact_lock.lock().unwrap();

    // Names present in archive.catalog never participate (cold-tier resurrection guard).
    // This is file I/O, deliberately done before (and outside) the state lock.
    let cataloged: HashSet<String> = load_catalog(&dir)?.into_iter().map(|e| e.name).collect();

    let policy = CompactPolicy::default();
    // Single deterministic snapshot. Plans are disjoint runs of entries, so committing one
    // plan cannot invalidate another's inputs; each plan revalidates at swap time anyway.
    let plans = {
        let state = shared.state.lock().unwrap();
        select_plans(&policy, &state.segments, &cataloged, series_filter)
    };
    let mut removed = 0usize;
    for plan in plans {
        removed += run_plan(shared, &dir, &policy, plan)?;
    }
    Ok(removed)
}

/// Snapshot-only selection (no I/O): maximal runs of eligible same-series entries in catalog
/// order. An entry is eligible iff it is local and its name is absent from `cataloged`.
/// Runs break at ineligible same-series entries so the merge never reorders duplicate-ts
/// winners relative to a skipped segment (scan-order preservation).
fn select_plans(
    policy: &CompactPolicy,
    segments: &[SegEntry],
    cataloged: &HashSet<String>,
    series_filter: Option<SeriesId>,
) -> Vec<Plan> {
    // Group target-series entries by series, preserving catalog order (BTreeMap iteration is
    // series-id order; within a series, push order is catalog order — both deterministic).
    let mut by_series: BTreeMap<SeriesId, Vec<Option<Input>>> = BTreeMap::new();
    for e in segments {
        if let Some(s) = series_filter {
            if e.series != s {
                continue;
            }
        }
        let eligible = matches!(e.loc, SegLoc::Local(_)) && !cataloged.contains(&e.name);
        by_series
            .entry(e.series)
            .or_default()
            .push(eligible.then(|| Input {
                name: e.name.clone(),
                zone: e.zone,
            }));
    }
    let mut plans = Vec::new();
    for (series, entries) in by_series {
        let mut run: Vec<Input> = Vec::new();
        for entry in entries {
            match entry {
                Some(input) => run.push(input),
                None => {
                    if run.len() >= policy.min_segments {
                        plans.push(Plan {
                            series,
                            inputs: std::mem::take(&mut run),
                        });
                    } else {
                        run.clear();
                    }
                }
            }
        }
        if run.len() >= policy.min_segments {
            plans.push(Plan {
                series,
                inputs: run,
            });
        }
    }
    plans
}

/// Execute one merge plan: open inputs by name → k-way merge (duplicates preserved in
/// `(ts, catalog-index)` order) → write the
/// output segment (tmp + rename + policy fsync) → short critical-section swap → best-effort
/// delete of the inputs. No file I/O is performed while holding the `state` lock.
///
/// Returns the number of input segments removed (0 when the plan aborted because an input
/// raced away or changed between snapshot and swap — nothing was merged then).
fn run_plan(shared: &Shared, dir: &Path, policy: &CompactPolicy, plan: Plan) -> Result<usize> {
    // Open inputs by name (fresh reads; never the cached readers — the swap must be provable
    // against the on-disk bytes that a reopen would see).
    let mut readers = Vec::with_capacity(plan.inputs.len());
    let mut sizes = Vec::with_capacity(plan.inputs.len());
    for input in &plan.inputs {
        let path = dir.join(&input.name);
        let meta = match fs::metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(Error::Io(e)),
        };
        let reader = match SegmentReader::open(&path) {
            Ok(r) => r,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        readers.push(reader);
        sizes.push(meta.len());
    }

    // Apply max_input_bytes by greedy deterministic split (no-op under the default policy).
    let groups = split_by_bytes(&plan, &sizes, policy);
    let mut removed = 0usize;
    for (lo, hi) in groups {
        if hi - lo < policy.min_segments {
            continue;
        }
        // A group whose revalidation fails aborts just that group (its inputs raced away);
        // earlier committed groups still count.
        removed += merge_group(
            shared,
            dir,
            plan.series,
            &plan.inputs[lo..hi],
            &readers[lo..hi],
            &sizes[lo..hi],
        )?;
    }
    Ok(removed)
}

/// Half-open index ranges `[lo, hi)` into the plan inputs, greedily packed under
/// `max_input_bytes` in input order (deterministic). With no cap the whole plan is one group.
fn split_by_bytes(plan: &Plan, sizes: &[u64], policy: &CompactPolicy) -> Vec<(usize, usize)> {
    let n = plan.inputs.len();
    let Some(cap) = policy.max_input_bytes else {
        return vec![(0, n)];
    };
    let mut out = Vec::new();
    let mut lo = 0usize;
    let mut acc = 0u64;
    for (i, &sz) in sizes.iter().enumerate() {
        if i > lo && acc.saturating_add(sz) > cap {
            out.push((lo, i));
            lo = i;
            acc = 0;
        }
        acc = acc.saturating_add(sz);
    }
    if lo < n {
        out.push((lo, n));
    }
    out
}

/// Merge one input group and commit it. Returns the number of inputs removed (0 when the swap
/// revalidation failed — a concurrent archive/seal changed an input; the output is cleaned up).
fn merge_group(
    shared: &Shared,
    dir: &Path,
    series: SeriesId,
    inputs: &[Input],
    readers: &[SegmentReader],
    sizes: &[u64],
) -> Result<usize> {
    let merged = kway_merge(readers)?;
    if merged.is_empty() {
        // Defensive: non-empty segments always decode; never write an empty segment.
        return Ok(0);
    }

    // Monotonic, never-reused output name: seg_seq is shared with the seal path and was
    // pre-fixed at open to max(local + cataloged names) + 1 (SPEC §1 required pre-fix).
    let out_name = {
        let mut seq = shared.seg_seq.lock().unwrap();
        let name = format!("seg-{:06}-s{:06}.seg", *seq, series);
        *seq += 1;
        name
    };
    let out_path = dir.join(&out_name);
    // Write order step 1+2: *.seg.tmp → atomic rename → fsync per SyncPolicy (mirrors seal:
    // SyncPolicy::None requests no durability and skips the fsync entirely).
    if shared.config.wal_sync == SyncPolicy::None {
        SegmentWriter::write_unsynced(&out_path, series, &merged)?;
    } else {
        SegmentWriter::write(&out_path, series, &merged)?;
    }
    let out_bytes = fs::metadata(&out_path)?.len();
    // Pre-open the output reader outside the critical section (I/O stays out of the lock).
    let out_reader = match SegmentReader::open(&out_path) {
        Ok(r) => r,
        Err(e) => {
            let _ = fs::remove_file(&out_path);
            return Err(e);
        }
    };

    // Write order step 3: one short critical section. Revalidate that every selected input is
    // still present, local (not archived meanwhile), and unchanged (same zone map) — a
    // concurrent seal/archive must not be compacted away (SPEC §1).
    let input_names: HashSet<&str> = inputs.iter().map(|i| i.name.as_str()).collect();
    let mut state = shared.state.lock().unwrap();
    let still_valid = inputs.iter().all(|input| {
        state.segments.iter().any(|e| {
            e.name == input.name && matches!(e.loc, SegLoc::Local(_)) && e.zone == input.zone
        })
    });
    if !still_valid {
        drop(state);
        let _ = fs::remove_file(&out_path); // best-effort rollback of the uninstalled output
        return Ok(0);
    }
    // Install at the position of the first input: every same-series entry before the run stays
    // before the output and every one after stays after, so scan's first-writer-wins dedup is
    // preserved exactly for data the merge did not cover.
    let pos = state
        .segments
        .iter()
        .position(|e| e.name == inputs[0].name)
        .expect("first input revalidated above");
    state
        .segments
        .retain(|e| !input_names.contains(e.name.as_str()));
    let at = pos.min(state.segments.len());
    state
        .segments
        .insert(at, SegEntry::local(out_name, out_reader));
    drop(state);

    // Counters only count committed merges.
    {
        let mut st = shared.compact_stats.lock().unwrap();
        st.runs += 1;
        st.input_segments += inputs.len() as u64;
        st.output_segments += 1;
        st.input_bytes += sizes.iter().sum::<u64>();
        st.output_bytes += out_bytes;
    }

    // Write order step 4: best-effort delete of superseded inputs after the swap, **newest
    // first** (reverse catalog order), fsyncing the directory after each durable delete. A
    // crash mid-delete then leaves the oldest inputs plus the output: name order keeps the
    // oldest input ahead of the output, and the output preserves (ts, catalog-index) order,
    // so duplicate-timestamp winners are exactly the pre-compaction ones. Deleting oldest
    // first would be wrong: a crash could leave only a *newer* input ahead of the output,
    // promoting its duplicate-losing samples to winners on reopen.
    let sync_dir = shared.config.wal_sync != SyncPolicy::None;
    for input in inputs.iter().rev() {
        if fs::remove_file(dir.join(&input.name)).is_ok() && sync_dir {
            // SyncPolicy::None skips this on purpose: same durability tier as the unsynced
            // segment write above (process-crash safe via the page cache, no power-loss
            // guarantee was requested).
            let _ = fsync_dir(dir);
        }
    }
    Ok(inputs.len())
}

/// Directory fsync making a just-performed segment-file delete durable (SPEC §1 crash
/// semantics: deletion order newest→oldest must survive a crash).
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_data()
}

/// K-way merge over same-series segment iterators, ordered by `(ts, input catalog index)`
/// (SPEC §1): **all** samples are preserved, including duplicate timestamps — losers are not
/// dropped. Scan's decode-time predicate filtering plus stable-sort + `dedup_by_key` then
/// reproduce the pre-compaction result exactly: the first writer for a plain scan, the first
/// *matching* writer for a predicated scan.
fn kway_merge(readers: &[SegmentReader]) -> Result<Vec<Sample>> {
    let mut iters = Vec::with_capacity(readers.len());
    let mut total = 0usize;
    for r in readers {
        total += r.zone_map().count as usize;
        iters.push(r.iter()?.peekable());
    }
    let mut out = Vec::with_capacity(total);
    loop {
        // Lowest head timestamp; the lowest iterator index (== catalog order) wins ties, so
        // equal timestamps are emitted adjacent and in input order.
        let mut pick: Option<(Timestamp, usize)> = None;
        for (i, it) in iters.iter_mut().enumerate() {
            if let Some(s) = it.peek() {
                if pick.map(|(ts, _)| s.ts < ts).unwrap_or(true) {
                    pick = Some((s.ts, i));
                }
            }
        }
        let Some((_, i)) = pick else { break };
        out.push(iters[i].next().expect("peeked above"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rti_core::{Config, Profile, SyncPolicy};
    use rti_query::{Agg, Pred};

    use crate::Db;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-db-compact-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn config(dir: std::path::PathBuf) -> Config {
        Config {
            data_dir: Some(dir),
            memtable_max: 1 << 12, // large: seals in these tests are explicit
            wal_sync: SyncPolicy::Group { interval_us: 1_000 },
            ..Config::default()
        }
    }

    fn scan_all(db: &Db, series: SeriesId) -> Vec<Sample> {
        db.scan(series, i64::MIN, i64::MAX, None, None)
            .unwrap()
            .collect()
    }

    fn seg_files(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".seg"))
            .collect();
        v.sort();
        v
    }

    /// Write one batch and seal it into its own segment.
    fn seal_batch(db: &Db, series: SeriesId, samples: &[(i64, f64)]) {
        for &(ts, v) in samples {
            db.put(series, Sample::new(ts, v)).unwrap();
        }
        db.seal().unwrap();
    }

    /// SPEC §4 gate: merging N overlapping same-series segments leaves scan sample-identical,
    /// including a predicate and aggregations. Overlapping prefixes rewrite the same values;
    /// the discriminating-predicate case (loser matches, winner does not) is covered by
    /// `compact_discriminating_predicate_scan_identical`.
    #[test]
    fn compact_merge_scan_sample_identical() {
        let d = tmpdir("merge-identical");
        let db = Db::open(config(d.clone())).unwrap();
        // Three overlapping segments for series 1.
        seal_batch(&db, 1, &(0..100).map(|t| (t, t as f64)).collect::<Vec<_>>());
        seal_batch(
            &db,
            1,
            &(40..140).map(|t| (t, t as f64)).collect::<Vec<_>>(),
        ); // pure duplicates on 40..99
        seal_batch(
            &db,
            1,
            &(80..180).map(|t| (t, t as f64)).collect::<Vec<_>>(),
        ); // pure duplicates on 80..139
        assert_eq!(db.segment_count(), 3);

        let full_before = scan_all(&db, 1);
        assert_eq!(full_before.len(), 180);
        let pred = Some(Pred::Gt(37.0));
        let pred_before: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();
        assert!(!pred_before.is_empty() && pred_before.len() < full_before.len());
        let sum_before: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, None, Some(Agg::Sum))
            .unwrap()
            .collect();
        let max_before: Vec<Sample> = db
            .scan(1, 0, 179, Some(Pred::Lt(150.0)), Some(Agg::Max))
            .unwrap()
            .collect();
        let stats_before = db.compaction_stats();

        let removed = db.compact().unwrap();
        assert_eq!(removed, 3, "all three segments merge into one");
        assert_eq!(db.segment_count(), 1);
        assert_eq!(
            seg_files(&d),
            vec!["seg-000003-s000001.seg".to_string()],
            "output name is monotonic, inputs deleted"
        );

        assert_eq!(
            scan_all(&db, 1),
            full_before,
            "full scan must be sample-identical"
        );
        let pred_after: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();
        assert_eq!(
            pred_after, pred_before,
            "predicate scan must be sample-identical"
        );
        let sum_after: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, None, Some(Agg::Sum))
            .unwrap()
            .collect();
        assert_eq!(sum_after, sum_before, "Sum aggregation must be identical");
        let max_after: Vec<Sample> = db
            .scan(1, 0, 179, Some(Pred::Lt(150.0)), Some(Agg::Max))
            .unwrap()
            .collect();
        assert_eq!(
            max_after, max_before,
            "predicated Max aggregation must be identical"
        );

        let stats = db.compaction_stats();
        assert_eq!(stats.runs, stats_before.runs + 1);
        assert_eq!(stats.input_segments, stats_before.input_segments + 3);
        assert_eq!(stats.output_segments, stats_before.output_segments + 1);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §1: duplicate timestamps keep the first writer (catalog order), exactly the scan
    /// rule — before and after compaction. The `Lt(500.0)` predicate matches every winner and
    /// no loser, so predicate scans are pinned identical too.
    #[test]
    fn compact_duplicate_ts_first_writer_wins() {
        let d = tmpdir("first-wins");
        let db = Db::open(config(d.clone())).unwrap();
        seal_batch(&db, 1, &(0..100).map(|t| (t, t as f64)).collect::<Vec<_>>());
        seal_batch(
            &db,
            1,
            &(50..150)
                .map(|t| (t, 1000.0 + t as f64))
                .collect::<Vec<_>>(),
        );
        seal_batch(
            &db,
            1,
            &(120..180)
                .map(|t| (t, 2000.0 + t as f64))
                .collect::<Vec<_>>(),
        );

        let expected: Vec<Sample> = (0..180)
            .map(|t| {
                let v = if t < 100 {
                    t as f64 // seg-000000 wins
                } else if t < 150 {
                    1000.0 + t as f64 // seg-000001 wins over seg-000002
                } else {
                    2000.0 + t as f64
                };
                Sample::new(t, v)
            })
            .collect();
        assert_eq!(
            scan_all(&db, 1),
            expected,
            "baseline: catalog order decides duplicates"
        );
        let pred = Some(Pred::Lt(500.0));
        let pred_before: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();

        assert_eq!(db.compact().unwrap(), 3);
        assert_eq!(db.segment_count(), 1);
        let after = scan_all(&db, 1);
        assert_eq!(after, expected, "merged segment keeps first-writer values");
        assert_eq!(
            after[60].value, 60.0,
            "ts 60 must come from the first segment"
        );
        assert_eq!(
            after[130].value, 1130.0,
            "ts 130 must come from the second segment"
        );
        assert_eq!(after[160].value, 2160.0);
        let pred_after: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();
        assert_eq!(
            pred_after, pred_before,
            "predicate scan identical across compaction"
        );

        // Reopen: the merged segment decodes to the same logical series.
        drop(db);
        let db = Db::open(config(d.clone())).unwrap();
        assert_eq!(
            scan_all(&db, 1),
            expected,
            "reopen after compaction keeps first-writer values"
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §4: a **discriminating** predicate — one that matches the duplicate *loser* but
    /// not the winner — must be sample-identical across compaction. The output segment
    /// preserves duplicates in `(ts, catalog-index)` order, so decode-time predicate filtering
    /// plus the scan layer's stable-sort dedup see exactly the same candidates as before.
    #[test]
    fn compact_discriminating_predicate_scan_identical() {
        let d = tmpdir("pred-discriminating");
        let db = Db::open(config(d.clone())).unwrap();
        // seg-000000 wins duplicates at ts 50..99 with values 50..99 (never match Gt(500));
        // seg-000001 loses with values 1050..1099 (always match).
        seal_batch(&db, 1, &(0..100).map(|t| (t, t as f64)).collect::<Vec<_>>());
        seal_batch(
            &db,
            1,
            &(50..150)
                .map(|t| (t, 1000.0 + t as f64))
                .collect::<Vec<_>>(),
        );
        let full_before = scan_all(&db, 1);
        assert_eq!(full_before.len(), 150);
        let pred = Some(Pred::Gt(500.0));
        let pred_before: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();
        assert_eq!(
            pred_before,
            (50..150)
                .map(|t| Sample::new(t, 1000.0 + t as f64))
                .collect::<Vec<_>>(),
            "baseline: predicated scan sees the duplicate losers"
        );
        let sum_before: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, Some(Agg::Sum))
            .unwrap()
            .collect();

        assert_eq!(db.compact().unwrap(), 2);
        assert_eq!(db.segment_count(), 1);
        // The output segment physically preserves both duplicates at ts 50..99.
        let out = SegmentReader::open(d.join("seg-000002-s000001.seg")).unwrap();
        assert_eq!(
            out.iter().unwrap().count(),
            200,
            "duplicates must be preserved in the output"
        );

        assert_eq!(
            scan_all(&db, 1),
            full_before,
            "plain scan still first-writer"
        );
        let pred_after: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();
        assert_eq!(
            pred_after, pred_before,
            "discriminating predicate scan must be sample-identical"
        );
        let sum_after: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, Some(Agg::Sum))
            .unwrap()
            .collect();
        assert_eq!(
            sum_after, sum_before,
            "predicated aggregation must be identical"
        );

        drop(db);
        let db = Db::open(config(d.clone())).unwrap();
        assert_eq!(scan_all(&db, 1), full_before);
        let p: Vec<Sample> = db
            .scan(1, i64::MIN, i64::MAX, pred, None)
            .unwrap()
            .collect();
        assert_eq!(
            p, pred_before,
            "reopen keeps the predicated result identical"
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Swap revalidation: an input that changed (or was archived/removed) between the catalog
    /// snapshot and the swap aborts the merge — the uninstalled output is cleaned up, inputs
    /// stay intact, stats are untouched, and a fresh compact() still succeeds.
    #[test]
    fn compact_swap_revalidation_aborts_on_changed_input() {
        let d = tmpdir("revalidate-abort");
        let db = Db::open(config(d.clone())).unwrap();
        seal_batch(&db, 1, &(0..50).map(|t| (t, 1.0)).collect::<Vec<_>>());
        seal_batch(&db, 1, &(50..100).map(|t| (t, 2.0)).collect::<Vec<_>>());

        // Take the same snapshot the executor would...
        let policy = CompactPolicy::default();
        let plan = {
            let state = db.shared.state.lock().unwrap();
            select_plans(&policy, &state.segments, &HashSet::new(), None)
                .into_iter()
                .next()
                .expect("two eligible segments form a plan")
        };
        // ...then simulate a concurrent change to an input entry (a racing op replaced it):
        // the unchanged-proof (zone equality) must fail at swap time.
        {
            let mut state = db.shared.state.lock().unwrap();
            let e = state
                .segments
                .iter_mut()
                .find(|e| e.name == plan.inputs[0].name)
                .unwrap();
            e.zone.count += 1;
        }
        let removed = run_plan(&db.shared, &d, &policy, plan).unwrap();
        assert_eq!(
            removed, 0,
            "a changed input must abort the merge at swap time"
        );
        assert_eq!(
            seg_files(&d).len(),
            2,
            "the uninstalled output is rolled back, inputs intact"
        );
        assert_eq!(
            db.compaction_stats(),
            CompactionStats::default(),
            "aborts move no counters"
        );
        // A fresh snapshot revalidates against the current state and compacts normally.
        assert_eq!(db.compact().unwrap(), 2);
        assert_eq!(db.segment_count(), 1);
        assert_eq!(scan_all(&db, 1).len(), 100);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §0.5 / §4: Deterministic profile compaction is Ok(0) and touches no filesystem.
    #[test]
    fn compact_deterministic_noop() {
        let d = tmpdir("det-noop");
        let data = d.join("should-not-exist");
        let cfg = Config {
            data_dir: Some(data.clone()),
            memtable_max: 1 << 10,
            profile: Profile::Deterministic,
            wal_sync: SyncPolicy::Always, // force-overridden to None
            ..Config::default()
        };
        let db = Db::open(cfg).unwrap();
        for i in 0..500i64 {
            db.put(1, Sample::new(i, i as f64)).unwrap();
        }
        db.flush().unwrap();
        assert_eq!(db.compact().unwrap(), 0);
        assert_eq!(db.compact_series(1).unwrap(), 0);
        assert_eq!(db.compaction_stats(), CompactionStats::default());
        assert!(
            !data.exists(),
            "Deterministic compaction must not touch the filesystem"
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §1/§4: a segment whose name appears in `archive.catalog` is skipped (crash-leftover
    /// local copy of an archived segment), breaks the merge run, and survives compaction; the
    /// remaining eligible segments still merge, and scan stays sample-identical.
    #[test]
    fn compact_skips_cataloged_segments() {
        let d = tmpdir("cataloged-skip");
        let db = Db::open(config(d.clone())).unwrap();
        seal_batch(&db, 1, &(0..50).map(|t| (t, 1.0)).collect::<Vec<_>>());
        seal_batch(&db, 1, &(50..100).map(|t| (t, 2.0)).collect::<Vec<_>>()); // will be "cataloged"
        seal_batch(&db, 1, &(100..150).map(|t| (t, 3.0)).collect::<Vec<_>>());
        seal_batch(&db, 1, &(150..200).map(|t| (t, 4.0)).collect::<Vec<_>>());
        assert_eq!(seg_files(&d).len(), 4);

        // Simulate a crash between catalog append and local delete during archive: the catalog
        // names seg-000001 while the local file is still (again) present.
        let cataloged = "seg-000001-s000001.seg";
        let line = format!(
            "A {cataloged} 1 50 99 {} {} 50\n",
            2.0f64.to_bits(),
            2.0f64.to_bits()
        );
        fs::write(d.join("archive.catalog"), line).unwrap();

        let before = scan_all(&db, 1);
        let removed = db.compact().unwrap();
        assert_eq!(
            removed, 2,
            "only the eligible run [seg-000002, seg-000003] merges"
        );
        let files = seg_files(&d);
        assert!(
            files.contains(&cataloged.to_string()),
            "cataloged segment must survive compaction"
        );
        assert!(
            files.contains(&"seg-000000-s000001.seg".to_string()),
            "run-broken singleton stays"
        );
        assert!(
            files.contains(&"seg-000004-s000001.seg".to_string()),
            "merged output uses the next free seq"
        );
        assert_eq!(files.len(), 3);
        assert_eq!(
            scan_all(&db, 1),
            before,
            "scan identical with a cataloged segment in the middle"
        );

        // Reopen: sequence allocation must clear both local and cataloged names (pre-fix §1).
        drop(db);
        let db = Db::open(config(d.clone())).unwrap();
        assert_eq!(scan_all(&db, 1), before, "reopen keeps the logical series");
        seal_batch(&db, 1, &[(200, 5.0)]);
        assert!(
            d.join("seg-000005-s000001.seg").exists(),
            "reopen must allocate max(local + cataloged) + 1, never reusing a seq"
        );
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Hand-build a compaction output exactly as the executor does (k-way over the named
    /// inputs in catalog order, **all** samples preserved, ordered by `(ts, input index)`).
    fn hand_merged(dir: &Path, names: &[&str]) -> Vec<Sample> {
        let mut merged: Vec<Sample> = Vec::new();
        for name in names {
            let r = SegmentReader::open(dir.join(name)).unwrap();
            merged.extend(r.iter().unwrap());
        }
        merged.sort_by_key(|s| s.ts); // stable: equal ts stay in input (catalog) order
        merged
    }

    /// SPEC §1 crash windows: simulate a crash after the output rename but before the swap's
    /// deletes (old inputs and new output coexist) plus a crash before a rename (stale
    /// `*.seg.tmp`). Reopen must lose nothing and keep the logical scan correct, and a later
    /// real compaction still works with fresh sequence numbers.
    #[test]
    fn compact_crash_old_and_new_coexist_reopen_correct() {
        let d = tmpdir("crash-coexist");
        let expected: Vec<Sample> = (0..150)
            .map(|t| Sample::new(t, if t < 100 { t as f64 } else { 1000.0 + t as f64 }))
            .collect();
        // Discriminating predicate: the duplicate winner (v = ts) never matches, the loser
        // (v = 1000+ts) always does — pre-compaction predicated scans see the *losers*.
        let pred = Some(Pred::Gt(500.0));
        let pred_expected: Vec<Sample> = (50..150)
            .map(|t| Sample::new(t, 1000.0 + t as f64))
            .collect();
        {
            let db = Db::open(config(d.clone())).unwrap();
            seal_batch(&db, 1, &(0..100).map(|t| (t, t as f64)).collect::<Vec<_>>());
            seal_batch(
                &db,
                1,
                &(50..150)
                    .map(|t| (t, 1000.0 + t as f64))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(scan_all(&db, 1), expected);
            let p: Vec<Sample> = db
                .scan(1, i64::MIN, i64::MAX, pred, None)
                .unwrap()
                .collect();
            assert_eq!(p, pred_expected);
        }

        // Hand-build the compaction output, name it with the next sequence number, and leave
        // the inputs in place (crash after rename, before swap/deletes).
        let merged = hand_merged(&d, &["seg-000000-s000001.seg", "seg-000001-s000001.seg"]);
        assert_eq!(merged.len(), 200, "duplicates are preserved, not deduped");
        SegmentWriter::write(d.join("seg-000002-s000001.seg"), 1, &merged).unwrap();
        // Stale temp file from a crash before an even later rename: must be ignored by open.
        fs::write(
            d.join("seg-000004-s000001.seg.tmp"),
            b"RTISEG01-partial-garbage",
        )
        .unwrap();

        // Reopen with old + new side by side: no loss, no duplication, dedup keeps it logical.
        {
            let db = Db::open(config(d.clone())).unwrap();
            assert_eq!(
                db.segment_count(),
                3,
                "all three .seg files load; the .tmp is ignored"
            );
            assert_eq!(
                scan_all(&db, 1),
                expected,
                "duplicate old/new segments reopen correctly"
            );
            let p: Vec<Sample> = db
                .scan(1, i64::MIN, i64::MAX, pred, None)
                .unwrap()
                .collect();
            assert_eq!(
                p, pred_expected,
                "predicated scan is unchanged with old+new coexisting"
            );
            // A real compaction now merges all three; the output takes seq 3 (the stale .tmp
            // never reserved it) and all superseded files are deleted.
            assert_eq!(db.compact().unwrap(), 3);
            assert_eq!(seg_files(&d), vec!["seg-000003-s000001.seg".to_string()]);
            assert_eq!(scan_all(&db, 1), expected);
            let p: Vec<Sample> = db
                .scan(1, i64::MIN, i64::MAX, pred, None)
                .unwrap()
                .collect();
            assert_eq!(p, pred_expected);
        }
        // Final reopen over the compacted directory.
        {
            let db = Db::open(config(d.clone())).unwrap();
            assert_eq!(scan_all(&db, 1), expected);
            let p: Vec<Sample> = db
                .scan(1, i64::MIN, i64::MAX, pred, None)
                .unwrap()
                .collect();
            assert_eq!(p, pred_expected);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// SPEC §1 crash window: crash **mid-delete**. Superseded inputs are deleted newest →
    /// oldest with a directory fsync per delete, so the only durable mid-delete state is
    /// "oldest inputs + output". Simulate exactly that: the output is present and only the
    /// NEWER input was durably deleted. Reopen must keep duplicate-timestamp winners (plain
    /// and predicated) identical to the pre-compaction scan.
    #[test]
    fn compact_crash_mid_delete_keeps_duplicate_winners() {
        let d = tmpdir("crash-mid-delete");
        let expected: Vec<Sample> = (0..150)
            .map(|t| Sample::new(t, if t < 100 { t as f64 } else { 1000.0 + t as f64 }))
            .collect();
        let pred = Some(Pred::Gt(500.0));
        let pred_expected: Vec<Sample> = (50..150)
            .map(|t| Sample::new(t, 1000.0 + t as f64))
            .collect();
        {
            let db = Db::open(config(d.clone())).unwrap();
            seal_batch(&db, 1, &(0..100).map(|t| (t, t as f64)).collect::<Vec<_>>());
            seal_batch(
                &db,
                1,
                &(50..150)
                    .map(|t| (t, 1000.0 + t as f64))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(scan_all(&db, 1), expected);
        }

        // Output written and renamed; newest-first deletion completed exactly one step
        // (seg-000001 gone, directory fsynced) before the crash.
        let merged = hand_merged(&d, &["seg-000000-s000001.seg", "seg-000001-s000001.seg"]);
        SegmentWriter::write(d.join("seg-000002-s000001.seg"), 1, &merged).unwrap();
        fs::remove_file(d.join("seg-000001-s000001.seg")).unwrap();
        fsync_dir(&d).unwrap();

        {
            let db = Db::open(config(d.clone())).unwrap();
            assert_eq!(
                seg_files(&d),
                vec![
                    "seg-000000-s000001.seg".to_string(),
                    "seg-000002-s000001.seg".to_string(),
                ]
            );
            // Winner at the duplicated ts 50..99 is still seg-000000's sample: name order puts
            // it ahead of the output, and the output's internal (ts, catalog-index) order keeps
            // the older input's sample first as well.
            assert_eq!(
                scan_all(&db, 1),
                expected,
                "mid-delete crash must not change winners"
            );
            assert_eq!(scan_all(&db, 1)[60].value, 60.0);
            let p: Vec<Sample> = db
                .scan(1, i64::MIN, i64::MAX, pred, None)
                .unwrap()
                .collect();
            assert_eq!(
                p, pred_expected,
                "mid-delete crash must not change predicated results"
            );
            // Recovery compaction finishes the job.
            assert_eq!(db.compact().unwrap(), 2);
            assert_eq!(seg_files(&d), vec!["seg-000003-s000001.seg".to_string()]);
            assert_eq!(scan_all(&db, 1), expected);
            let p: Vec<Sample> = db
                .scan(1, i64::MIN, i64::MAX, pred, None)
                .unwrap()
                .collect();
            assert_eq!(p, pred_expected);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// `compact_series` touches only the target series: other series' segments, names, and
    /// scan results are untouched.
    #[test]
    fn compact_series_only_affects_target() {
        let d = tmpdir("series-only");
        let db = Db::open(config(d.clone())).unwrap();
        for b in 0..3i64 {
            for i in 0..50i64 {
                let ts = b * 100 + i;
                db.put(1, Sample::new(ts, ts as f64)).unwrap();
                db.put(2, Sample::new(ts, 1000.0 + ts as f64)).unwrap();
            }
            db.seal().unwrap();
        }
        assert_eq!(db.segment_count(), 6);
        let s1_before = scan_all(&db, 1);
        let s2_before = scan_all(&db, 2);
        let s2_files_before = seg_files(&d)
            .into_iter()
            .filter(|n| n.contains("-s000002."))
            .collect::<Vec<_>>();

        let removed = db.compact_series(1).unwrap();
        assert_eq!(removed, 3, "all three series-1 segments merge");
        assert_eq!(db.segment_count(), 4, "series 2 keeps its three segments");
        let s2_files_after = seg_files(&d)
            .into_iter()
            .filter(|n| n.contains("-s000002."))
            .collect::<Vec<_>>();
        assert_eq!(s2_files_after, s2_files_before, "series-2 files untouched");
        assert_eq!(scan_all(&db, 1), s1_before, "series-1 scan identical");
        assert_eq!(scan_all(&db, 2), s2_before, "series-2 scan identical");

        // Unknown/empty series is a no-op; the other series compacts independently afterwards.
        assert_eq!(db.compact_series(99).unwrap(), 0);
        assert_eq!(db.compact_series(2).unwrap(), 3);
        assert_eq!(db.segment_count(), 2);
        assert_eq!(scan_all(&db, 2), s2_before);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Stats are cumulative, byte-accurate against on-disk sizes, and unchanged by no-op runs.
    #[test]
    fn compaction_stats_are_correct() {
        let d = tmpdir("stats");
        let db = Db::open(config(d.clone())).unwrap();
        assert_eq!(db.compaction_stats(), CompactionStats::default());
        seal_batch(&db, 1, &(0..50).map(|t| (t, 1.0)).collect::<Vec<_>>());
        seal_batch(&db, 1, &(50..100).map(|t| (t, 2.0)).collect::<Vec<_>>());
        seal_batch(&db, 1, &(100..150).map(|t| (t, 3.0)).collect::<Vec<_>>());
        let input_bytes: u64 = seg_files(&d)
            .iter()
            .map(|n| fs::metadata(d.join(n)).unwrap().len())
            .sum();

        assert_eq!(db.compact().unwrap(), 3);
        let out_bytes = fs::metadata(d.join("seg-000003-s000001.seg"))
            .unwrap()
            .len();
        let stats = db.compaction_stats();
        assert_eq!(
            stats,
            CompactionStats {
                runs: 1,
                input_segments: 3,
                output_segments: 1,
                input_bytes,
                output_bytes: out_bytes,
            }
        );
        assert!(
            stats.output_bytes < stats.input_bytes,
            "merge must not grow the footprint"
        );

        // Nothing left to merge: a no-op run returns 0 and does not move any counter.
        assert_eq!(db.compact().unwrap(), 0);
        assert_eq!(db.compaction_stats(), stats);
        drop(db);
        std::fs::remove_dir_all(&d).ok();
    }
}
