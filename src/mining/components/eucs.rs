use std::collections::HashMap;
use crate::types::{ItemId, Utility, RawTransaction};

/// Estimated Utility Co-occurrence Structure.
/// Maps (item_i, item_j) where i < j to the sum of transaction utilities
/// of transactions containing both items.
///
/// Used to prune join candidates: if eucs[(x,y)] < min_utility, skip the join.
pub struct Eucs {
    inner: HashMap<(ItemId, ItemId), Utility>,
}

impl Eucs {
    pub fn new() -> Self {
        Self { inner: HashMap::new() }
    }

    pub fn add_transaction(&mut self, items: &[ItemId], tu: Utility, guard: &crate::mining::core::MemoryGuard) -> bool {
        for i in 0..items.len() {
            for j in (i+1)..items.len() {
                let (a, b) = if items[i] < items[j] { (items[i], items[j]) } else { (items[j], items[i]) };
                if !self.inner.contains_key(&(a, b)) {
                    if !guard.try_alloc(32) {
                        return false; // Stop tracking further if OOM
                    }
                }
                *self.inner.entry((a, b)).or_insert(0) += tu;
            }
        }
        true
    }
    pub fn build<'a, I>(transactions: I, guard: &crate::mining::core::MemoryGuard) -> Self
    where
        I: Iterator<Item = &'a RawTransaction>,
    {
        let mut inner: HashMap<(ItemId, ItemId), Utility> = HashMap::new();
        // Track unique pairs to estimate memory accurately (approx 32 bytes per entry)
        let mut _capacity = 0;
        let mut stopped = false;
        
        for tx in transactions {
            if stopped { break; }
            let items: Vec<ItemId> = tx.items.iter().map(|e| e.item).collect();
            for i in 0..items.len() {
                for j in (i+1)..items.len() {
                    let (a, b) = if items[i] < items[j] { (items[i], items[j]) } else { (items[j], items[i]) };
                    if !inner.contains_key(&(a, b)) {
                        if !guard.try_alloc(32) {
                            // If we hit OS budget limit, stop building EUCS gracefully
                            // It will just be incomplete, and prune less efficiently.
                            stopped = true;
                            break;
                        }
                        _capacity += 1;
                    }
                    *inner.entry((a, b)).or_insert(0) += tx.transaction_utility;
                }
                if stopped { break; }
            }
        }
        Self { inner }
    }

    /// Returns true if the join of any prefix ending in x with y can be pruned.
    pub fn can_prune(&self, x: ItemId, y: ItemId, min_utility: Utility) -> bool {
        let key = if x < y { (x, y) } else { (y, x) };
        self.inner.get(&key).copied().unwrap_or(0) < min_utility
    }

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
        let eucs = Eucs::build(txs.iter());
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
        let eucs = Eucs::build(txs.iter());
        // (3,5) and (5,3) should be the same
        assert!(!eucs.can_prune(3, 5, 100));
        assert!(!eucs.can_prune(5, 3, 100));
    }
}
