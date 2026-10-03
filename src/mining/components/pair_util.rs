//! Exact utilities of 1- and 2-itemsets, reduced to the K-th largest value.
//!
//! Top-K miners use this as a *pre-evaluation* border (REPT's and TKU's PE-style strategies):
//! any K distinct itemsets with known exact utilities give a valid lower bound on the K-th best
//! utility, so mining can start with that threshold instead of 0.
//!
//! Pair utilities are accumulated in partitions (pairs whose smaller item falls in partition p),
//! each within a memory cap; only a K-sized heap of values survives between partitions.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io;
use std::sync::Arc;
use crate::mining::core::memory_guard::{MemoryGuard, map_bytes};
use crate::types::{ItemId, Utility};
use super::tx_spool::TxSpool;

/// K-th largest exact utility among all single items (`item_utils`) and all co-occurring pairs
/// in `db`, or 0 when fewer than K itemsets exist.
pub fn kth_best_small_itemset_utility(
    db: &TxSpool,
    item_utils: impl IntoIterator<Item = Utility>,
    k: usize,
    guard: &Arc<MemoryGuard>,
    cap_bytes: usize,
    est_pairs: usize,
) -> io::Result<Utility> {
    if k == 0 { return Ok(0); }
    let mut heap: BinaryHeap<Reverse<Utility>> = BinaryHeap::with_capacity(k + 1);
    let _heap_res = guard.reserve_force((k + 1) * 8);
    let offer = |heap: &mut BinaryHeap<Reverse<Utility>>, u: Utility| {
        if heap.len() < k { heap.push(Reverse(u)); } else if u > heap.peek().unwrap().0 { heap.pop(); heap.push(Reverse(u)); }
    };
    for u in item_utils { offer(&mut heap, u); }

    let entry = map_bytes::<(ItemId, ItemId), Utility>(1).max(24);
    let per_part_pairs = (cap_bytes / (2 * entry)).max(1024);
    let mut parts = est_pairs.div_ceil(per_part_pairs).clamp(1, 1024).next_power_of_two();
    'restart: loop {
        let mut local = heap.clone();
        for p in 0..parts {
            let mut map: HashMap<(ItemId, ItemId), Utility> = HashMap::new();
            let mut res = guard.reserve_force(0);
            let mut overflow = false;
            db.scan(|items, utils, _| {
                for i in 0..items.len() {
                    for j in (i + 1)..items.len() {
                        let (a, b) = if items[i] < items[j] { (items[i], items[j]) } else { (items[j], items[i]) };
                        if (a as usize) % parts != p { continue; }
                        let before = map.capacity();
                        *map.entry((a, b)).or_insert(0) += utils[i] + utils[j];
                        if map.capacity() != before {
                            let need = map_bytes::<(ItemId, ItemId), Utility>(map.capacity());
                            if need > cap_bytes { overflow = true; return false; }
                            res.resize_force(need);
                        }
                    }
                }
                true
            })?;
            if overflow {
                if parts >= 1024 { break 'restart; }
                parts *= 2;
                continue 'restart;
            }
            for (_, u) in map { offer(&mut local, u); }
        }
        heap = local;
        break;
    }
    Ok(if heap.len() == k { heap.peek().unwrap().0 } else { 0 })
}
