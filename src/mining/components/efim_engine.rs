//! EFIM (Zida, Fournier-Viger, Lin, Wu & Tseng, 2015/2017) with its full set of techniques,
//! inside the memory budget:
//!
//! * items with TWU >= min_utility are renamed 1..m in ascending TWU order and every
//!   transaction is sorted by the new names,
//! * high-utility database projection: the projected database of β = α ∪ {z} holds, for each
//!   transaction containing z, the items after z that are still in Secondary(α), plus the
//!   utility of β in that transaction,
//! * high-utility transaction merging: identical consecutive projected transactions are merged
//!   (utilities summed), which is what makes EFIM fast on dense data,
//! * subtree utility (su) and local utility (lu) bounds computed with utility-bin arrays;
//!   Primary(β) = {z ≻ i : su(β, z) >= min}, Secondary(β) = {z ≻ i : lu(β, z) >= min}.
//!
//! Memory: every (projected) database is a list of ~1 MB segments, each kept in RAM while the
//! budget allows and stored as a buffer-pool page otherwise; segments are read in place.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::{Arc, atomic::Ordering};
use crate::buffer_pool::pool::{BufferPool, OwnedPage};
use crate::mining::core::{
    context::{MiningContext, WriterProxy},
    data_source::DataSource,
    memory_guard::{MemoryGuard, Reservation},
};
use crate::preprocessing::{db_reader::DbReader, twu_filter::TwuFilter};
use crate::types::{ItemId, Utility};

const SEGMENT: usize = 1 << 20;

/// Segment size: 1 MB, smaller under tiny budgets (a scan pins one segment at a time).
fn segment_bytes(guard: &MemoryGuard) -> usize {
    (guard.budget() / 64).clamp(4 * 1024, SEGMENT)
}

enum Seg {
    Mem(Vec<u8>, Reservation),
    Disk(OwnedPage),
}

/// A (projected) transaction database. Record layout:
/// `len: u32 | prefix_utility: i64 | items: u32 x len | utils: i64 x len`.
pub struct TxDb {
    segs: Vec<Seg>,
    pub ntx: usize,
    pub bytes: usize,
}

struct TxDbWriter<'a> {
    pool: &'a Arc<BufferPool>,
    guard: &'a Arc<MemoryGuard>,
    db: TxDb,
    cur: Vec<u8>,
    cur_res: Reservation,
    seg: usize,
    /// Offset of the last record in `cur` (for merging identical consecutive transactions).
    last: Option<usize>,
    pub merged: u64,
}

impl<'a> TxDbWriter<'a> {
    fn new(pool: &'a Arc<BufferPool>, guard: &'a Arc<MemoryGuard>) -> Self {
        Self { pool, guard, db: TxDb { segs: Vec::new(), ntx: 0, bytes: 0 }, cur: Vec::new(),
               cur_res: guard.reserve_force(0), last: None, merged: 0, seg: segment_bytes(guard) }
    }

    fn push(&mut self, items: &[u32], utils: &[Utility], pu: Utility) -> io::Result<()> {
        if items.is_empty() { return Ok(()); }
        // Transaction merging: same item sequence as the previous record -> add utilities.
        if let Some(off) = self.last {
            let len = u32::from_le_bytes(self.cur[off..off + 4].try_into().unwrap()) as usize;
            if len == items.len() {
                let it = off + 12;
                let same = (0..len).all(|k| u32::from_le_bytes(self.cur[it + 4 * k..it + 4 * k + 4].try_into().unwrap()) == items[k]);
                if same {
                    let p = i64::from_le_bytes(self.cur[off + 4..off + 12].try_into().unwrap()) + pu;
                    self.cur[off + 4..off + 12].copy_from_slice(&p.to_le_bytes());
                    let us = it + 4 * len;
                    for k in 0..len {
                        let o = us + 8 * k;
                        let v = i64::from_le_bytes(self.cur[o..o + 8].try_into().unwrap()) + utils[k];
                        self.cur[o..o + 8].copy_from_slice(&v.to_le_bytes());
                    }
                    self.merged += 1;
                    return Ok(());
                }
            }
        }
        if !self.cur.is_empty() && self.cur.len() + 12 + 12 * items.len() > self.seg {
            self.seal()?;
        }
        self.last = Some(self.cur.len());
        let need = self.cur.len() + 12 + 12 * items.len();
        if need > self.cur.capacity() {
            self.cur.reserve(need - self.cur.len());
            self.cur_res.resize_force(self.cur.capacity());
        }
        self.cur.extend_from_slice(&(items.len() as u32).to_le_bytes());
        self.cur.extend_from_slice(&pu.to_le_bytes());
        for &i in items { self.cur.extend_from_slice(&i.to_le_bytes()); }
        for &u in utils { self.cur.extend_from_slice(&u.to_le_bytes()); }
        self.db.ntx += 1;
        Ok(())
    }

    fn seal(&mut self) -> io::Result<()> {
        if self.cur.is_empty() { return Ok(()); }
        let mut buf = std::mem::take(&mut self.cur);
        buf.shrink_to_fit();
        self.cur_res.resize_force(0);
        self.db.bytes += buf.len();
        let seg = match self.guard.reserve(buf.len() + 16) {
            Some(r) => Seg::Mem(buf, r),
            None => Seg::Disk(OwnedPage::create(self.pool, buf)?),
        };
        self.db.segs.push(seg);
        self.last = None;
        Ok(())
    }

    fn finish(mut self) -> io::Result<TxDb> {
        self.seal()?;
        Ok(std::mem::replace(&mut self.db, TxDb { segs: Vec::new(), ntx: 0, bytes: 0 }))
    }
}

impl TxDb {
    /// Visit every transaction: f(items, utils, prefix_utility).
    fn scan(&self, mut f: impl FnMut(&[u32], &[Utility], Utility) -> io::Result<()>) -> io::Result<()> {
        let mut items: Vec<u32> = Vec::new();
        let mut utils: Vec<Utility> = Vec::new();
        for seg in &self.segs {
            let pin;
            let bytes: &[u8] = match seg {
                Seg::Mem(v, _) => v,
                Seg::Disk(p) => { pin = p.pin()?; &pin[..] }
            };
            let mut pos = 0;
            while pos < bytes.len() {
                let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                let pu = i64::from_le_bytes(bytes[pos + 4..pos + 12].try_into().unwrap());
                let it = pos + 12;
                let us = it + 4 * len;
                items.clear();
                utils.clear();
                for k in 0..len {
                    items.push(u32::from_le_bytes(bytes[it + 4 * k..it + 4 * k + 4].try_into().unwrap()));
                    utils.push(i64::from_le_bytes(bytes[us + 8 * k..us + 8 * k + 8].try_into().unwrap()));
                }
                f(&items, &utils, pu)?;
                pos = us + 8 * len;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct EfimConfig {
    pub name: &'static str,
}

struct Ctx<'a> {
    ctx: &'a MiningContext,
    min: Utility,
    /// new name -> original item id
    orig: &'a [ItemId],
}

/// Per-task scratch: utility bins indexed by item name, and the list of touched names.
struct Bins {
    su: Vec<Utility>,
    lu: Vec<Utility>,
    touched: Vec<u32>,
    _res: Reservation,
}

impl Ctx<'_> {
    /// Project `db` on item `z` keeping only items flagged in `keep`; returns (β-DB, u(β)).
    fn project(&self, db: &TxDb, z: u32, keep: &[bool]) -> io::Result<(TxDb, Utility)> {
        let mut w = TxDbWriter::new(&self.ctx.pool, &self.ctx.guard);
        let mut u_beta = 0;
        let mut si: Vec<u32> = Vec::new();
        let mut su: Vec<Utility> = Vec::new();
        db.scan(|items, utils, pu| {
            let Ok(p) = items.binary_search(&z) else { return Ok(()) };
            let pz = pu + utils[p];
            u_beta += pz;
            si.clear();
            su.clear();
            for k in p + 1..items.len() {
                if keep[items[k] as usize] { si.push(items[k]); su.push(utils[k]); }
            }
            w.push(&si, &su, pz)
        })?;
        self.ctx.progress.fast_path_writes.fetch_add(w.merged, Ordering::Relaxed);
        Ok((w.finish()?, u_beta))
    }

    /// su(β, i) and lu(β, i) for every item i in β-DB, accumulated into the bins.
    fn bounds(&self, db: &TxDb, bins: &mut Bins) -> io::Result<()> {
        db.scan(|items, utils, pu| {
            let total: Utility = utils.iter().sum();
            let mut rem = total;
            for k in 0..items.len() {
                let i = items[k] as usize;
                if bins.su[i] == 0 && bins.lu[i] == 0 { bins.touched.push(items[k]); }
                bins.lu[i] += pu + total;
                bins.su[i] += pu + rem; // u(i) + utilities of the items after i
                rem -= utils[k];
            }
            Ok(())
        })
    }

    fn search(&self, prefix: &mut Vec<u32>, db: &TxDb, primary: &[u32], secondary: &[bool],
              bins: &mut Bins, w: &mut WriterProxy) -> io::Result<()> {
        self.ctx.progress.current_depth.store(prefix.len(), Ordering::Relaxed);
        for &z in primary {
            let (beta_db, u_beta) = self.project(db, z, secondary)?;
            self.expand(prefix, z, &beta_db, u_beta, secondary, bins, w)?;
        }
        Ok(())
    }

    /// Output β = prefix ∪ {z} if it is a HUI, compute its Primary/Secondary sets from β-DB
    /// and recurse.
    #[allow(clippy::too_many_arguments)]
    fn expand(&self, prefix: &mut Vec<u32>, z: u32, beta_db: &TxDb, u_beta: Utility, secondary: &[bool],
              bins: &mut Bins, w: &mut WriterProxy) -> io::Result<()> {
        {
            prefix.push(z);
            if u_beta >= self.min {
                let items: Vec<ItemId> = prefix.iter().map(|&n| self.orig[n as usize]).collect();
                w.write_hui(&items, u_beta)?;
                self.ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
            }
            if beta_db.ntx > 0 {
                self.bounds(beta_db, bins)?;
                let mut next_primary: Vec<u32> = Vec::new();
                let mut next_secondary = vec![false; secondary.len()];
                for &i in &bins.touched {
                    let iu = i as usize;
                    if i > z && secondary[iu] {
                        if bins.su[iu] >= self.min { next_primary.push(i); }
                        if bins.lu[iu] >= self.min { next_secondary[iu] = true; }
                    }
                    bins.su[iu] = 0;
                    bins.lu[iu] = 0;
                }
                bins.touched.clear();
                next_primary.sort_unstable();
                if !next_primary.is_empty() {
                    let _r = self.ctx.guard.reserve_force(next_secondary.len() + next_primary.len() * 4);
                    self.search(prefix, beta_db, &next_primary, &next_secondary, bins, w)?;
                }
            }
            prefix.pop();
        }
        Ok(())
    }
}

pub fn run_efim(cfg: EfimConfig, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
    let path = source.expect_file(cfg.name).to_path_buf();
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;

    ctx.progress.set_stage(&format!("{}: Pass 1 (TWU)", cfg.name));
    let twu = TwuFilter::new(min).compute(DbReader::new(BufReader::new(File::open(&path)?)).filter_map(Result::ok));
    // Rename promising items 1..m by ascending TWU.
    let mut order: Vec<(Utility, ItemId)> = twu.twu.iter().map(|(&i, &t)| (t, i)).collect();
    order.sort_unstable();
    let mut orig: Vec<ItemId> = vec![0];
    let mut name: HashMap<ItemId, u32> = HashMap::with_capacity(order.len());
    for (k, &(_, i)) in order.iter().enumerate() {
        name.insert(i, (k + 1) as u32);
        orig.push(i);
    }
    let m = orig.len();
    let _maps = ctx.guard.reserve_force(m * (4 + 24) + m * 4);

    // Admission control. Fixed: renaming maps, su/proj-size arrays. Per worker: utility bins
    // (2 x m), a pinned input segment and an output segment per projection level in progress,
    // and the smallest first-level batch of projections.
    {
        let estimate = |b: usize| -> (usize, usize) {
            let seg = (b / 64).clamp(4 * 1024, SEGMENT); // same rule as segment_bytes
            (m * (4 + 24 + 4 + 16), m * 16 + 4 * seg + 4 * seg)
        };
        ctx.admit(cfg.name, &estimate)?;
    }

    ctx.progress.set_stage(&format!("{}: Pass 2 (renamed database, su of items)", cfg.name));
    let mut su0 = vec![0 as Utility; m];
    // Bytes of each item's first-level projection (to size batches of projections per scan).
    let mut proj_bytes = vec![0usize; m];
    let mut w = TxDbWriter::new(&ctx.pool, &ctx.guard);
    let mut row: Vec<(u32, Utility)> = Vec::new();
    let mut items: Vec<u32> = Vec::new();
    let mut utils: Vec<Utility> = Vec::new();
    for tx in DbReader::new(BufReader::new(File::open(&path)?)).filter_map(Result::ok) {
        row.clear();
        row.extend(tx.items.iter().filter_map(|e| name.get(&e.item).map(|&n| (n, e.utility))));
        if row.is_empty() { continue; }
        row.sort_unstable_by_key(|e| e.0);
        items.clear();
        utils.clear();
        let mut rem: Utility = row.iter().map(|e| e.1).sum();
        let len = row.len();
        for (k, &(n, u)) in row.iter().enumerate() {
            proj_bytes[n as usize] += 12 + 12 * (len - k - 1);
            su0[n as usize] += rem;
            rem -= u;
            items.push(n);
            utils.push(u);
        }
        w.push(&items, &utils, 0)?;
    }
    let root = w.finish()?;
    drop(name);
    crate::mining::core::memory_guard::release_free_memory();

    let primary: Vec<u32> = (1..m as u32).filter(|&n| su0[n as usize] >= min).collect();
    drop(su0);
    let secondary = vec![true; m];
    ctx.progress.set_stage(&format!("{}: Mining ({} primary items, root DB {:.1} MB in {} tx)",
        cfg.name, primary.len(), root.bytes as f64 / 1048576.0, root.ntx));

    // Batches of top-level items: one scan of the root database builds all first-level
    // projections of a batch. Each concurrently running batch may use a slice of the budget.
    let threads = ctx.threads.max(1);
    // Floor: four segments (a batch's projections, including their open segments, are bounded
    // by the sum of their sizes; admission counts this floor per worker).
    let per_batch = (ctx.guard.native_remaining() / (2 * threads)).max(4 * segment_bytes(&ctx.guard));
    let target = primary.len().div_ceil(threads * 4).max(1);
    let mut batches: Vec<Vec<u32>> = Vec::new();
    let mut cur: Vec<u32> = Vec::new();
    let mut cur_b = 0usize;
    for &z in &primary {
        let b = proj_bytes[z as usize];
        if !cur.is_empty() && (cur_b + b > per_batch || cur.len() >= target) {
            batches.push(std::mem::take(&mut cur));
            cur_b = 0;
        }
        cur.push(z);
        cur_b += b;
    }
    if !cur.is_empty() { batches.push(cur); }
    drop(proj_bytes);

    let c = Ctx { ctx, min, orig: &orig };
    let root_ref = &root;
    let secondary_ref = &secondary;
    ctx.execute_tasks(batches, |batch, w| {
        let mut run = || -> io::Result<()> {
            let mut bins = Bins { su: vec![0; m], lu: vec![0; m], touched: Vec::new(), _res: ctx.guard.reserve_force(m * 16) };
            // One root scan -> projections for the whole batch.
            let slot: HashMap<u32, usize> = batch.iter().enumerate().map(|(k, &z)| (z, k)).collect();
            let mut writers: Vec<TxDbWriter> = batch.iter().map(|_| TxDbWriter::new(&ctx.pool, &ctx.guard)).collect();
            let mut u: Vec<Utility> = vec![0; batch.len()];
            root_ref.scan(|items, utils, _| {
                for p in 0..items.len() {
                    if let Some(&s) = slot.get(&items[p]) {
                        u[s] += utils[p];
                        writers[s].push(&items[p + 1..], &utils[p + 1..], utils[p])?;
                    }
                }
                Ok(())
            })?;
            let dbs: Vec<TxDb> = writers.into_iter().map(|w| w.finish()).collect::<io::Result<_>>()?;
            for (k, (&z, db)) in batch.iter().zip(dbs.into_iter()).enumerate() {
                ctx.progress.set_active_prefix(&[orig[z as usize]]);
                c.expand(&mut Vec::new(), z, &db, u[k], secondary_ref, &mut bins, w)?;
            }
            Ok(())
        };
        if let Err(e) = run() { eprintln!("{}: batch failed: {}", cfg.name, e); }
    });
    ctx.progress.set_active_prefix(&[]);
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}
