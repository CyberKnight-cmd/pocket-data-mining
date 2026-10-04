//! Shared, budget-aware projection-based miner (EFIM, EFIM-Closed, HAUI-Miner).
//!
//! Memory model:
//! * the filtered database is a `PagedDb` (RAM segments while the budget allows, pool
//!   pages otherwise),
//! * top-level items are processed in batches; one database scan builds the initial
//!   projections of a whole batch, and batch size is derived from the budget,
//! * every projection holds a `Reservation`, or is spilled to a pool page when the budget
//!   has no room — so the DFS stack can never outgrow the budget,
//! * per-node utility maps are accounted while they live.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::{Arc, atomic::Ordering};
use crate::buffer_pool::pool::OwnedPage;
use crate::mining::core::{
    context::{MiningContext, WriterProxy},
    data_source::DataSource,
    memory_guard::{Reservation, vec_bytes, map_bytes},
};
use crate::preprocessing::{db_reader::DbReader, twu_filter::TwuFilter};
use crate::types::{ItemId, Utility};
use super::paged_db::{PagedDb, PagedDbBuilder};

#[derive(Clone, Copy)]
pub struct ProjMinerConfig {
    pub name: &'static str,
    /// Only output closed itemsets (EFIM-Closed).
    pub closed: bool,
}

/// Compact projection entry: which transaction, where the suffix starts, and utilities.
#[derive(Clone, Copy, Debug)]
struct ProjTx {
    tx_idx: u32,
    offset: u16,
    prefix_utility: Utility,
}

const PROJ_BYTES: usize = 4 + 2 + 8;

enum Proj {
    Mem(Vec<ProjTx>, Option<Reservation>),
    Disk(OwnedPage, usize),
}

impl Proj {
    fn new(mut v: Vec<ProjTx>, ctx: &MiningContext) -> io::Result<Proj> {
        v.shrink_to_fit();
        match ctx.guard.reserve(vec_bytes::<ProjTx>(v.capacity())) {
            Some(r) => Ok(Proj::Mem(v, Some(r))),
            None => {
                let mut bytes = Vec::with_capacity(v.len() * PROJ_BYTES);
                for p in &v {
                    bytes.extend_from_slice(&p.tx_idx.to_le_bytes());
                    bytes.extend_from_slice(&p.offset.to_le_bytes());
                    bytes.extend_from_slice(&p.prefix_utility.to_le_bytes());
                }
                let n = v.len();
                drop(v);
                Ok(Proj::Disk(OwnedPage::create(&ctx.pool, bytes)?, n))
            }
        }
    }

    /// Borrow (in RAM) or load (accounted temporary) the entries.
    fn with<R>(&self, ctx: &MiningContext, f: impl FnOnce(&[ProjTx]) -> R) -> io::Result<R> {
        match self {
            Proj::Mem(v, _) => Ok(f(v)),
            Proj::Disk(page, n) => {
                let _r = ctx.guard.reserve_force(vec_bytes::<ProjTx>(*n));
                let pin = page.pin()?;
                let v: Vec<ProjTx> = pin.chunks_exact(PROJ_BYTES).map(|c| ProjTx {
                    tx_idx: u32::from_le_bytes(c[0..4].try_into().unwrap()),
                    offset: u16::from_le_bytes(c[4..6].try_into().unwrap()),
                    prefix_utility: i64::from_le_bytes(c[6..14].try_into().unwrap()),
                }).collect();
                drop(pin);
                Ok(f(&v))
            }
        }
    }
}

struct Miner<'a> {
    cfg: ProjMinerConfig,
    ctx: &'a MiningContext,
    db: &'a PagedDb,
}

impl Miner<'_> {
    /// Closed check: is there an item outside `prefix` present in every projected tx?
    fn is_closed(&self, prefix: &[ItemId], proj: &[ProjTx]) -> io::Result<bool> {
        let Some(first) = proj.first() else { return Ok(true) };
        let mut cur = self.db.cursor();
        let candidates: Vec<ItemId> = {
            let t = cur.tx(first.tx_idx)?;
            (0..t.len).map(|k| t.item(k)).filter(|i| !prefix.contains(i)).collect()
        };
        'cand: for c in candidates {
            for p in &proj[1..] {
                if !cur.tx(p.tx_idx)?.contains(c) { continue 'cand; }
            }
            return Ok(false);
        }
        Ok(true)
    }

    fn emit(&self, prefix: &[ItemId], utility: Utility, proj: &[ProjTx], w: &mut WriterProxy) -> io::Result<()> {
        if self.cfg.closed && !self.is_closed(prefix, proj)? {
            return Ok(());
        }
        w.write_hui(prefix, utility)?;
        self.ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn mine(&self, prefix: &mut Vec<ItemId>, proj: &Proj, w: &mut WriterProxy) -> io::Result<()> {
        let ctx = self.ctx;
        let min = ctx.min_utility;
        ctx.progress.current_depth.store(prefix.len(), Ordering::Relaxed);

        // Local utility (lu) and subtree utility (su) of every item in the projection.
        let (exts, _maps_res) = proj.with(ctx, |p| -> io::Result<(Vec<ItemId>, Reservation)> {
            let mut lu: HashMap<ItemId, Utility> = HashMap::new();
            let mut su: HashMap<ItemId, Utility> = HashMap::new();
            let mut res = ctx.guard.reserve_force(0);
            let mut cur = self.db.cursor();
            for t in p {
                let tx = cur.tx(t.tx_idx)?;
                let start = t.offset as usize;
                if start >= tx.len { continue; }
                let path = t.prefix_utility + tx.util(start) + tx.rem(start);
                for k in start..tx.len {
                    let item = tx.item(k);
                    *lu.entry(item).or_insert(0) += path;
                    *su.entry(item).or_insert(0) += t.prefix_utility + tx.util(k) + tx.rem(k);
                }
                res.resize_force(2 * map_bytes::<ItemId, Utility>(lu.capacity()));
            }
            ctx.progress.fast_path_reads.fetch_add(p.len() as u64, Ordering::Relaxed);
            let mut exts: Vec<ItemId> = su.iter()
                .filter(|&(i, &s)| s >= min && lu.get(i).copied().unwrap_or(0) >= min)
                .map(|(&i, _)| i)
                .collect();
            exts.sort_unstable();
            Ok((exts, res))
        })??;
        drop(_maps_res);

        for item in exts {
            // Project on `item`.
            let (new_proj, util) = proj.with(ctx, |p| -> io::Result<(Vec<ProjTx>, Utility)> {
                let _r = ctx.guard.reserve_force(vec_bytes::<ProjTx>(p.len()));
                let mut out: Vec<ProjTx> = Vec::with_capacity(p.len());
                let mut util = 0;
                let mut cur = self.db.cursor();
                for t in p {
                    let tx = cur.tx(t.tx_idx)?;
                    if let Some(pos) = tx.position_from(t.offset as usize, item) {
                        let pu = t.prefix_utility + tx.util(pos);
                        util += pu;
                        out.push(ProjTx { tx_idx: t.tx_idx, offset: (pos + 1) as u16, prefix_utility: pu });
                    }
                }
                Ok((out, util))
            })??;
            if new_proj.is_empty() { continue; }
            ctx.progress.fast_path_writes.fetch_add(new_proj.len() as u64, Ordering::Relaxed);

            prefix.push(item);
            ctx.progress.set_active_prefix(prefix);
            if util >= min {
                self.emit(prefix, util, &new_proj, w)?;
            }
            let child = Proj::new(new_proj, ctx)?;
            self.mine(prefix, &child, w)?;
            prefix.pop();
        }
        Ok(())
    }
}

pub fn run_proj_miner(cfg: ProjMinerConfig, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
    let path = source.expect_file(cfg.name).to_path_buf();
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;

    ctx.progress.set_stage(&format!("{}: Pass 1 (TWU)", cfg.name));
    let reader = DbReader::new(BufReader::new(File::open(&path)?));
    // Support of every item (= its first-level projection size), counted in the same pass.
    let mut count: HashMap<ItemId, u64> = HashMap::new();
    let twu = TwuFilter::new(min).compute(reader.filter_map(Result::ok).inspect(|tx| {
        for e in &tx.items { *count.entry(e.item).or_insert(0) += 1; }
    }));
    count.retain(|i, _| twu.passes(*i));

    // Admission control, before pass 2 fills free memory with data that could spill. Fixed:
    // per-item maps (counts, TWU, su, primary list), the DB builder's segment buffers and the
    // output queue. Per worker: a spilled projection is loaded whole while its child is built,
    // so up to two projections of the most frequent item; the per-level lu/su maps; one pinned
    // DB segment; the smallest first-level batch (one segment's worth). Everything else
    // (projections, DB pages) spills.
    {
        let n = count.len().max(1);
        let max_proj = vec_bytes::<ProjTx>(count.values().copied().max().unwrap_or(0) as usize);
        let multi = ctx.threads > 1;
        let estimate = |b: usize| -> (usize, usize) {
            let queue = if multi { (b / 64).clamp(64 * 96, 100_000 * 96) } else { 0 };
            (3 * map_bytes::<ItemId, Utility>(n) + n * 8 + 2 * super::paged_db::segment_bytes_for(b) + queue,
             2 * max_proj + 2 * map_bytes::<ItemId, Utility>(n) + 2 * super::paged_db::segment_bytes_for(b))
        };
        ctx.admit(cfg.name, &estimate)?;
    }

    // Pass 2: build the paged DB; root subtree utility per item.
    ctx.progress.set_stage(&format!("{}: Pass 2 (Load DB)", cfg.name));
    let mut builder = PagedDbBuilder::new(&ctx.pool, &ctx.guard);
    let mut su_root: HashMap<ItemId, Utility> = HashMap::new();
    let mut items: Vec<ItemId> = Vec::new();
    let mut utils: Vec<Utility> = Vec::new();
    let reader = DbReader::new(BufReader::new(File::open(&path)?));
    for tx in reader.filter_map(Result::ok) {
        let Some(ftx) = twu.apply(&tx) else { continue };
        items.clear();
        utils.clear();
        for e in &ftx.items { items.push(e.item); utils.push(e.utility); }
        let mut rem: Utility = utils.iter().sum();
        for (k, &it) in items.iter().enumerate() {
            rem -= utils[k];
            *su_root.entry(it).or_insert(0) += utils[k] + rem;
        }
        builder.push(&items, &utils)?;
    }
    let db = builder.finish()?;
    crate::mining::core::memory_guard::release_free_memory();

    // Primary items (EFIM subtree-utility pruning at the root): an itemset whose first
    // item is `i` has utility <= su(i), so items with su(i) < min_util start no HUI.
    let mut primary: Vec<ItemId> = su_root.iter().filter(|&(_, &s)| s >= min).map(|(&i, _)| i).collect();
    primary.sort_by_key(|&i| (twu.twu.get(&i).copied().unwrap_or(0), i));
    drop(su_root);


    // Batch top-level items so that one DB scan builds a whole batch of projections.
    // Each concurrently running batch may use a slice of the free native budget.
    let threads = ctx.threads.max(1);
    let per_batch = (ctx.guard.native_remaining() / (2 * threads)).max(super::paged_db::segment_bytes_for(ctx.guard.budget()));
    let min_batches = threads * 4;
    let target_items = primary.len().div_ceil(min_batches).max(1);
    let mut batches: Vec<Vec<ItemId>> = Vec::new();
    let mut cur: Vec<ItemId> = Vec::new();
    let mut cur_bytes = 0usize;
    for &i in &primary {
        let b = vec_bytes::<ProjTx>(count[&i] as usize);
        if !cur.is_empty() && (cur_bytes + b > per_batch || cur.len() >= target_items) {
            batches.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
        cur.push(i);
        cur_bytes += b;
    }
    if !cur.is_empty() { batches.push(cur); }
    drop(count);

    ctx.progress.set_stage(&format!(
        "{}: Mining ({} batches, DB {:.0}MB in RAM / {:.0}MB paged)",
        cfg.name, batches.len(), db.resident_bytes as f64 / 1048576.0, db.paged_bytes as f64 / 1048576.0
    ));

    let miner = Miner { cfg, ctx, db: &db };
    ctx.execute_tasks(batches, |batch, w| {
        let mut run = || -> io::Result<()> {
            // One scan of the DB builds the initial projections for the whole batch.
            let slot: HashMap<ItemId, usize> = batch.iter().enumerate().map(|(k, &i)| (i, k)).collect();
            let mut projs: Vec<Vec<ProjTx>> = vec![Vec::new(); batch.len()];
            let mut utils_1: Vec<Utility> = vec![0; batch.len()];
            let mut res = ctx.guard.reserve_force(0);
            let mut c = db.cursor();
            for idx in 0..db.len() {
                let tx = c.tx(idx)?;
                for k in 0..tx.len {
                    if let Some(&s) = slot.get(&tx.item(k)) {
                        let u = tx.util(k);
                        utils_1[s] += u;
                        let v = &mut projs[s];
                        let before = v.capacity();
                        v.push(ProjTx { tx_idx: idx, offset: (k + 1) as u16, prefix_utility: u });
                        if v.capacity() != before {
                            res.grow_force(vec_bytes::<ProjTx>(v.capacity()) - vec_bytes::<ProjTx>(before));
                        }
                    }
                }
            }
            drop(c);
            for (k, item) in batch.iter().enumerate() {
                let p = std::mem::take(&mut projs[k]);
                res.shrink(vec_bytes::<ProjTx>(p.capacity()));
                let mut prefix = vec![*item];
                ctx.progress.set_active_prefix(&prefix);
                if utils_1[k] >= min {
                    miner.emit(&prefix, utils_1[k], &p, w)?;
                }
                let proj = Proj::new(p, ctx)?;
                miner.mine(&mut prefix, &proj, w)?;
            }
            Ok(())
        };
        if let Err(e) = run() {
            eprintln!("{}: batch failed: {}", cfg.name, e);
        }
    });

    ctx.progress.set_active_prefix(&[]);
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}
