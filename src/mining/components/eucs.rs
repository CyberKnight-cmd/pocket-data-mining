use std::collections::HashMap;
use std::sync::Arc;
use crate::types::{ItemId, Utility, RawTransaction};
use crate::mining::core::memory_guard::{MemoryGuard, Reservation, map_bytes};

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
    /// False once the structure was abandoned for lack of memory.
    enabled: bool,
    /// Max bytes this structure may hold (a share of the budget).
    cap_bytes: usize,
    reservation: Option<Reservation>,
}

impl Eucs {
    /// Unbounded EUCS (no budget accounting).
    pub fn new() -> Self {
        Self { inner: HashMap::new(), enabled: true, cap_bytes: usize::MAX, reservation: None }
    }

    /// EUCS that charges `guard` and gives up once it would exceed `cap_bytes`.
    pub fn with_budget(guard: &Arc<MemoryGuard>, cap_bytes: usize) -> Self {
        Self { inner: HashMap::new(), enabled: true, cap_bytes, reservation: Some(guard.reserve_force(0)) }
    }

    fn disable(&mut self) {
        self.enabled = false;
        self.inner = HashMap::new();
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

    /// Drop pairs whose TWU is below `min_utility`. `can_prune` treats a missing pair
    /// exactly like a below-threshold one, so this frees memory without changing results
    /// (valid as long as the threshold never decreases, which holds for min-util and Top-K).
    pub fn compact(&mut self, min_utility: Utility) {
        if !self.enabled { return; }
        self.inner.retain(|_, tu| *tu >= min_utility);
        self.inner.shrink_to_fit();
        if let Some(r) = self.reservation.as_mut() {
            r.resize_force(map_bytes::<(ItemId, ItemId), Utility>(self.inner.capacity()));
        }
    }

    pub fn is_enabled(&self) -> bool { self.enabled }

    pub fn pair_count(&self) -> usize { self.inner.len() }
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
}
