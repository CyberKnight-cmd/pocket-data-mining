//! Partition projection for the tree miners (Han, Pei & Yin, SIGMOD 2000).
//!
//! When a global prefix tree does not fit the budget, mining it from spilled node pages
//! thrashes: every parent or node-link step can be a page load (UP-Growth+ on retail at
//! 32 MB read 207 GB of pages for a 26 MB tree). Instead the database is split by item.
//! Each transaction (items in tree order, root side first) goes to the partition of its last
//! item. Items are processed from the bottom of the tree order up: partition `i` then holds
//! exactly the transactions containing `i`, cut after `i`. A small tree of those rows is built
//! in RAM and `i` is mined on it, which sees the same paths, node utilities and counts as `i`
//! in the global tree. Then `i` is dropped from each row, which moves to the partition of its
//! new last item. All reads and writes are sequential.
//!
//! To bound the open write buffers, items are grouped into contiguous rank ranges of about
//! sqrt(n) items: one spool per group, split into per-item spools when the group's turn comes.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use crate::mining::core::context::MiningContext;
use crate::types::{ItemId, Utility};
use super::tx_spool::{spool_segment_bytes, TxSpool};

/// Whether tree miners use partition projection: when the global tree would not fit (auto),
/// always, or never. Default from `AIR_HUIM_TREE_PARTITION` (auto|always|off).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PartitionMode { Auto, Always, Off }

impl PartitionMode {
    pub fn from_env() -> Self {
        match std::env::var("AIR_HUIM_TREE_PARTITION").ok().as_deref() {
            Some("always") => Self::Always,
            Some("off") => Self::Off,
            _ => Self::Auto,
        }
    }

    /// Partition a tree estimated at `tree_bytes`? Auto: when the estimate exceeds what is
    /// free. The estimate (one node per item occurrence) is an upper bound, since transactions
    /// share prefixes; on retail it is ~1.5x the real tree.
    pub fn use_partitions(self, tree_bytes: usize, ctx: &MiningContext) -> bool {
        match self {
            Self::Always => true,
            Self::Off => false,
            Self::Auto => tree_bytes > ctx.guard.native_remaining(),
        }
    }
}

/// Upper bounds for a prefix tree of `db` over the ranked items: nodes (one per item
/// occurrence) and the sum of node depths (for per-node utility vectors).
pub fn tree_size(db: &TxSpool, rank: &HashMap<ItemId, u32>) -> io::Result<(u64, u64)> {
    let (mut nodes, mut depths) = (0u64, 0u64);
    db.scan(|its, _, _| {
        let l = its.iter().filter(|i| rank.contains_key(i)).count() as u64;
        nodes += l;
        depths += l * (l + 1) / 2;
        true
    })?;
    Ok((nodes, depths))
}

/// Run `on_item(item, partition, present)` for every ranked item, from the highest rank (the
/// bottom of the tree) to rank 0. `partition` holds the transactions containing `item`,
/// restricted to ranked items, sorted by rank and cut after `item` (so `item` is last);
/// `present` lists the items occurring in it, sorted by rank.
pub fn mine_partitioned(
    db: &TxSpool,
    rank: &HashMap<ItemId, u32>,
    ctx: &MiningContext,
    mut on_item: impl FnMut(ItemId, &TxSpool, &[ItemId]) -> io::Result<()>,
) -> io::Result<()> {
    let n = rank.len();
    if n == 0 { return Ok(()); }
    let mut by_rank: Vec<ItemId> = vec![0; n];
    for (&i, &r) in rank { by_rank[r as usize] = i; }
    let k = ((n as f64).sqrt().ceil() as usize).max(1);
    let m = n.div_ceil(k);
    // At most m group spools and k item spools are open; their buffers share a quarter of
    // what is free.
    let seg = (ctx.guard.native_remaining() / 4 / (m + k)).clamp(1024, spool_segment_bytes(&ctx.guard));
    let new_spool = || TxSpool::with_segment(Arc::clone(&ctx.store), &ctx.guard, seg);

    let mut groups: Vec<Option<TxSpool>> = (0..m).map(|_| None).collect();
    let mut row: Vec<(u32, ItemId, Utility)> = Vec::new();
    let mut buf: Vec<(ItemId, Utility)> = Vec::new();
    let mut err: Option<io::Error> = None;
    db.scan(|its, us, tu| {
        row.clear();
        row.extend(its.iter().zip(us).filter_map(|(i, &u)| rank.get(i).map(|&r| (r, *i, u))));
        if row.is_empty() { return true; }
        row.sort_unstable_by_key(|e| e.0);
        let last = row.last().unwrap().0 as usize;
        buf.clear();
        buf.extend(row.iter().map(|e| (e.1, e.2)));
        if let Err(e) = groups[last / k].get_or_insert_with(new_spool).push(&buf, tu) {
            err = Some(e);
            return false;
        }
        true
    })?;
    if let Some(e) = err { return Err(e); }

    for g in (0..m).rev() {
        let Some(mut gs) = groups[g].take() else { continue };
        gs.seal()?;
        let (lo, hi) = (g * k, ((g + 1) * k).min(n));
        let mut parts: Vec<Option<TxSpool>> = (lo..hi).map(|_| None).collect();
        gs.scan(|its, us, tu| {
            let last = rank[its.last().unwrap()] as usize;
            buf.clear();
            buf.extend(its.iter().copied().zip(us.iter().copied()));
            if let Err(e) = parts[last - lo].get_or_insert_with(new_spool).push(&buf, tu) {
                err = Some(e);
                return false;
            }
            true
        })?;
        if let Some(e) = err.take() { return Err(e); }
        drop(gs);

        for r in (lo..hi).rev() {
            let Some(mut ps) = parts[r - lo].take() else { continue };
            ps.seal()?;
            let present: Vec<ItemId> = {
                let mut set: HashSet<ItemId> = HashSet::new();
                ps.scan(|its, _, _| { set.extend(its.iter().copied()); true })?;
                let mut v: Vec<ItemId> = set.into_iter().collect();
                v.sort_unstable_by_key(|i| rank[i]);
                v
            };
            ctx.progress.set_stage(&format!("Tree mining by partitions: item {} of {}", n - r, n));
            on_item(by_rank[r], &ps, &present)?;
            // Forward each row without its last item to the partition of its new last item.
            ps.scan(|its, us, tu| {
                let l = its.len();
                if l <= 1 { return true; }
                let nl = rank[&its[l - 2]] as usize;
                buf.clear();
                buf.extend(its[..l - 1].iter().copied().zip(us[..l - 1].iter().copied()));
                let target = if nl >= lo { parts[nl - lo].get_or_insert_with(new_spool) }
                             else { groups[nl / k].get_or_insert_with(new_spool) };
                if let Err(e) = target.push(&buf, tu) {
                    err = Some(e);
                    return false;
                }
                true
            })?;
            if let Some(e) = err.take() { return Err(e); }
        }
    }
    Ok(())
}
