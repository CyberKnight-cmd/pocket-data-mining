use std::collections::HashMap;
use std::sync::Arc;
use crate::types::{ItemId, Utility, RawTransaction};
use crate::mining::core::memory_guard::{MemoryGuard, Reservation, map_bytes, vec_bytes};

/// Estimated Utility Co-occurrence Structure.
/// Maps (item_i, item_j) where i < j to the sum of transaction utilities
/// of transactions containing both items.
///
/// Used to prune join candidates: if eucs[(x,y)] < min_utility, skip the join.
///
/// EUCS is only an optimisation, and pruning with an incomplete EUCS is WRONG (a pair
/// that was never recorded would be pruned as "never co-occurred"). So if it cannot be
/// built within its memory allowance it is discarded entirely and prunes nothing.
pub struct Eucs {
    inner: HashMap<(ItemId, ItemId), Utility>,
    /// Triangular matrix form (FHM's original layout), used instead of `inner` when it fits:
    /// no hashing, so building it is a fraction of the cost on dense or long-transaction data.
    dense: Option<Dense>,
    /// False once the structure was abandoned for lack of memory.
    enabled: bool,
    /// Max bytes this structure may hold (a share of the budget).
    cap_bytes: usize,
    reservation: Option<Reservation>,
}

impl Eucs {
    /// Unbounded EUCS (no budget accounting).
    pub fn new() -> Self {
        Self { inner: HashMap::new(), dense: None, enabled: true, cap_bytes: usize::MAX, reservation: None }
    }

    /// EUCS that charges `guard` and gives up once it would exceed `cap_bytes`.
    pub fn with_budget(guard: &Arc<MemoryGuard>, cap_bytes: usize) -> Self {
        Self { inner: HashMap::new(), dense: None, enabled: true, cap_bytes, reservation: Some(guard.reserve_force(0)) }
    }

    fn disable(&mut self) {
        self.enabled = false;
        self.inner = HashMap::new();
        self.dense = None;
        if let Some(r) = self.reservation.as_mut() {
            let b = r.bytes();
            r.shrink(b);
        }
    }

    /// Make sure one more distinct key fits; accounts table growth before it happens.
    fn ensure_room_for_one(&mut self) -> bool {
        if self.inner.len() < self.inner.capacity() { return true; }
        let new_cap = (self.inner.capacity() * 2).max(64);
        let need = map_bytes::<(ItemId, ItemId), Utility>(new_cap);
        // While rehashing, old and new tables coexist.
        let peak = need + map_bytes::<(ItemId, ItemId), Utility>(self.inner.capacity());
        if peak > self.cap_bytes { return false; }
        match self.reservation.as_mut() {
            Some(r) => {
                let have = r.bytes();
                if peak > have && !r.try_grow(peak - have) { return false; }
                self.inner.reserve(new_cap - self.inner.len());
                let actual = map_bytes::<(ItemId, ItemId), Utility>(self.inner.capacity());
                r.resize_force(actual);
                true
            }
            None => true, // unbounded mode
        }
    }

    /// Add one transaction. Returns false if EUCS has been (or just got) disabled.
    pub fn add_transaction(&mut self, items: &[ItemId], tu: Utility, _guard: &MemoryGuard) -> bool {
        if !self.enabled { return false; }
        for i in 0..items.len() {
            for j in (i+1)..items.len() {
                let (a, b) = if items[i] < items[j] { (items[i], items[j]) } else { (items[j], items[i]) };
                if let Some(v) = self.inner.get_mut(&(a, b)) {
                    *v += tu;
                    continue;
                }
                if !self.ensure_room_for_one() {
                    self.disable();
                    return false;
                }
                self.inner.insert((a, b), tu);
            }
        }
        true
    }

    pub fn build<'a, I>(transactions: I, guard: &MemoryGuard) -> Self
    where
        I: Iterator<Item = &'a RawTransaction>,
    {
        let mut eucs = Self::new();
        for tx in transactions {
            let items: Vec<ItemId> = tx.items.iter().map(|e| e.item).collect();
            if !eucs.add_transaction(&items, tx.transaction_utility, guard) { break; }
        }
        eucs
    }

    /// Returns true if the join of any prefix ending in x with y can be pruned.
    pub fn can_prune(&self, x: ItemId, y: ItemId, min_utility: Utility) -> bool {
        if !self.enabled { return false; }
        if let Some(d) = &self.dense { return d.can_prune(x, y, min_utility); }
        let key = if x < y { (x, y) } else { (y, x) };
        if let Some(&tu) = self.inner.get(&key) {
            tu < min_utility
        } else {
            true // Never co-occurred, prune immediately
        }
    }

    /// Build a complete, already-compacted EUCS within `cap_bytes` by partitioning pairs on
    /// their smaller item: pass `p` of `P` counts only pairs with `a % P == p`, drops pairs
    /// below `min_utility`, and merges the survivors. Peak memory is one partition's raw
    /// table plus the compacted result, instead of every distinct pair at once.
    ///
    /// `scan` must re-read the (filtered) database, calling `f(items, tu)` per transaction
    /// and stopping early if `f` returns false. `est_pairs` is an upper bound on distinct
    /// pairs (e.g. total pair occurrences), used to choose the starting partition count.
    /// Returns a disabled EUCS if even the compacted result cannot fit.
    pub fn build_partitioned<S>(
        mut scan: S,
        guard: &Arc<MemoryGuard>,
        cap_bytes: usize,
        min_utility: Utility,
        est_pairs: usize,
        mut on_pass: impl FnMut(usize, usize),
    ) -> std::io::Result<Self>
    where
        S: FnMut(&mut dyn FnMut(&[ItemId], Utility) -> bool) -> std::io::Result<()>,
    {
        const MAX_PARTITIONS: usize = 256;
        let pair_bytes = |n: usize| map_bytes::<(ItemId, ItemId), Utility>(n);
        // Growth doubles the table and briefly holds old + new: budget 1.5x the final size.
        let est = pair_bytes(est_pairs) * 3 / 2;
        let mut parts = est.div_ceil(cap_bytes.max(1)).clamp(1, MAX_PARTITIONS).next_power_of_two();

        'restart: loop {
            let mut result = Self::with_budget(guard, cap_bytes);
            for p in 0..parts {
                on_pass(p, parts);
                // This partition may use whatever the compacted result does not.
                let mut part = Self::with_budget(guard, cap_bytes.saturating_sub(
                    result.reservation.as_ref().map_or(0, |r| r.bytes())));
                let mut overflow = false;
                scan(&mut |items, tu| {
                    for i in 0..items.len() {
                        for j in (i + 1)..items.len() {
                            let (a, b) = if items[i] < items[j] { (items[i], items[j]) } else { (items[j], items[i]) };
                            if (a as usize) % parts != p { continue; }
                            if let Some(v) = part.inner.get_mut(&(a, b)) { *v += tu; continue; }
                            if !part.ensure_room_for_one() { overflow = true; return false; }
                            part.inner.insert((a, b), tu);
                        }
                    }
                    true
                })?;
                if overflow {
                    drop(part);
                    drop(result);
                    if parts >= MAX_PARTITIONS { break 'restart; }
                    parts *= 2;
                    continue 'restart;
                }
                part.compact(min_utility);
                // Merge survivors into the result.
                for (k, v) in part.inner.drain() {
                    if !result.ensure_room_for_one() { break 'restart; }
                    result.inner.insert(k, v);
                }
                drop(part);
            }
            result.inner.shrink_to_fit();
            if let Some(r) = result.reservation.as_mut() {
                r.resize_force(pair_bytes(result.inner.capacity()));
            }
            return Ok(result);
        }
        // Could not fit even the compacted structure: run without EUCS (still exact).
        let mut disabled = Self::with_budget(guard, 0);
        disabled.disable();
        Ok(disabled)
    }

    /// Build the triangular-matrix form over `items` (every item that may appear in a kept
    /// pair; `scan` may pass others, which are ignored). Returns None, having reserved
    /// nothing, when the matrix does not fit `cap_bytes` or the budget.
    pub fn build_dense<S>(mut scan: S, items: &[ItemId], guard: &Arc<MemoryGuard>, cap_bytes: usize)
        -> std::io::Result<Option<Self>>
    where
        S: FnMut(&mut dyn FnMut(&[ItemId], Utility) -> bool) -> std::io::Result<()>,
    {
        let n = items.len();
        let max_id = items.iter().copied().max().unwrap_or(0) as usize;
        let cells = n * n.saturating_sub(1) / 2;
        let bytes = vec_bytes::<u32>(cells) + vec_bytes::<u32>(max_id + 1);
        if n < 2 || bytes > cap_bytes { return Ok(None); }
        let Some(res) = guard.reserve(bytes) else { return Ok(None) };
        let mut rank = vec![u32::MAX; max_id + 1];
        for (r, &i) in items.iter().enumerate() { rank[i as usize] = r as u32; }
        let mut d = Dense { rank, cells: vec![0u32; cells] };
        let mut ranks: Vec<u32> = Vec::new();
        let mut negative = false;
        scan(&mut |tx, tu| {
            // Clamping a negative utility would undercount a pair and prune it wrongly.
            if tu < 0 { negative = true; return false; }
            ranks.clear();
            ranks.extend(tx.iter().filter_map(|&i| d.rank.get(i as usize).copied().filter(|&r| r != u32::MAX)));
            let add = tu.min(u32::MAX as Utility) as u32;
            for (k, &a) in ranks.iter().enumerate() {
                for &b in &ranks[k + 1..] {
                    let c = &mut d.cells[Dense::index(a, b)];
                    *c = c.saturating_add(add);
                }
            }
            true
        })?;
        if negative { return Ok(None); }
        Ok(Some(Self { inner: HashMap::new(), dense: Some(d), enabled: true, cap_bytes, reservation: Some(res) }))
    }

    /// Drop pairs whose TWU is below `min_utility`. `can_prune` treats a missing pair
    /// exactly like a below-threshold one, so this frees memory without changing results
    /// (valid as long as the threshold never decreases, which holds for min-util and Top-K).
    pub fn compact(&mut self, min_utility: Utility) {
        if !self.enabled || self.dense.is_some() { return; }
        self.inner.retain(|_, tu| *tu >= min_utility);
        self.inner.shrink_to_fit();
        if let Some(r) = self.reservation.as_mut() {
            r.resize_force(map_bytes::<(ItemId, ItemId), Utility>(self.inner.capacity()));
        }
    }

    pub fn is_enabled(&self) -> bool { self.enabled }

    pub fn pair_count(&self) -> usize {
        match &self.dense { Some(d) => d.cells.iter().filter(|&&c| c > 0).count(), None => self.inner.len() }
    }
}

/// Pair TWUs in a triangular matrix indexed by item rank. Cells saturate at `u32::MAX`, and
/// a saturated cell never prunes (its true value is unknown but at least that large).
struct Dense {
    /// Item id -> rank, `u32::MAX` for items outside the matrix (no kept pair contains them).
    rank: Vec<u32>,
    cells: Vec<u32>,
}

impl Dense {
    #[inline]
    fn index(a: u32, b: u32) -> usize {
        let (lo, hi) = if a < b { (a as usize, b as usize) } else { (b as usize, a as usize) };
        hi * (hi - 1) / 2 + lo
    }

    fn can_prune(&self, x: ItemId, y: ItemId, min_utility: Utility) -> bool {
        let r = |i: ItemId| self.rank.get(i as usize).copied().filter(|&r| r != u32::MAX);
        let (Some(a), Some(b)) = (r(x), r(y)) else { return true };
        if a == b { return false; }
        let c = self.cells[Self::index(a, b)];
        c != u32::MAX && (c as Utility) < min_utility
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ItemEntry;

    fn tx(tu: i64, items: &[(u32, i64)]) -> RawTransaction {
        RawTransaction {
            tid: 0,
            transaction_utility: tu,
            items: items.iter().map(|(i, u)| ItemEntry { item: *i, utility: *u }).collect(),
        }
    }

    #[test]
    fn eucs_basic() {
        let txs = vec![
            tx(100, &[(1, 50), (2, 50)]),
            tx(200, &[(1, 100), (3, 100)]),
        ];
        
        let store = std::sync::Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().path(), false
        ).unwrap());
        let guard = crate::mining::core::MemoryGuard::new(1024 * 1024, store);
        let eucs = Eucs::build(txs.iter(), &guard);
        // (1,2): tx0 = 100
        // (1,3): tx1 = 200
        assert!(!eucs.can_prune(1, 2, 100));
        assert!(eucs.can_prune(1, 2, 101));
        assert!(!eucs.can_prune(1, 3, 200));
        assert!(eucs.can_prune(2, 3, 1)); // never co-occurred
    }

    #[test]
    fn eucs_symmetric_key() {
        let txs = vec![tx(100, &[(5, 50), (3, 50)])];
        let store = std::sync::Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().path(), false
        ).unwrap());
        let guard = crate::mining::core::MemoryGuard::new(1024 * 1024, store);
        let eucs = Eucs::build(txs.iter(), &guard);
        // (3,5) and (5,3) should be the same
        assert!(!eucs.can_prune(3, 5, 100));
        assert!(!eucs.can_prune(5, 3, 100));
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn eucs_over_budget_disables_instead_of_pruning_wrongly() {
        let store = std::sync::Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().path(), false
        ).unwrap());
        let guard = Arc::new(MemoryGuard::new(1 << 30, store));
        let mut eucs = Eucs::with_budget(&guard, 4096);
        let mut ok = true;
        for t in 0..200u32 {
            ok &= eucs.add_transaction(&[t, t + 1000, t + 2000], 10, &guard);
        }
        assert!(!ok);
        assert!(!eucs.is_enabled());
        // A disabled EUCS must never prune.
        assert!(!eucs.can_prune(1, 2, 1_000_000));
        drop(eucs);
        assert_eq!(guard.used(), 0);
    }

    #[test]
    fn dense_matches_hashed_and_saturates_safely() {
        let store = std::sync::Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().path(), false
        ).unwrap());
        let guard = Arc::new(MemoryGuard::new(1 << 30, store));
        let txs: Vec<(Vec<ItemId>, Utility)> = vec![
            (vec![1, 2, 7], 100), (vec![2, 7], 50), (vec![1, 9], 30), (vec![7, 9], 5_000_000_000),
        ];
        let scan = |f: &mut dyn FnMut(&[ItemId], Utility) -> bool| {
            for (items, tu) in &txs { if !f(items, *tu) { break; } }
            Ok(())
        };
        let dense = Eucs::build_dense(scan, &[1, 2, 7, 9], &guard, 1 << 20).unwrap().unwrap();
        let hashed = Eucs::build_partitioned(scan, &guard, 1 << 20, 0, 16, |_, _| {}).unwrap();
        for x in [1, 2, 7, 9, 4] {
            for y in [1, 2, 7, 9, 4] {
                if x == y { continue; }
                for t in [1, 30, 31, 100, 150, 151, 5_000_000_000] {
                    // A saturated cell (7,9 > u32::MAX) never prunes; elsewhere both agree.
                    let want = if (x.min(y), x.max(y)) == (7, 9) { false } else { hashed.can_prune(x, y, t) };
                    assert_eq!(dense.can_prune(x, y, t), want, "pair ({x},{y}) at {t}");
                }
            }
        }
        // Too large for the cap: nothing built, nothing reserved.
        drop((dense, hashed));
        assert!(Eucs::build_dense(scan, &[1, 2, 7, 9], &guard, 8).unwrap().is_none());
        assert_eq!(guard.used(), 0);
    }
}
