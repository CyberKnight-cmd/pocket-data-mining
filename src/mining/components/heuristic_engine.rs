//! Shared engine for the bio-inspired (approximate) HUIM algorithms:
//! HUIM-GA (genetic algorithm), HUIM-BPSO (binary particle swarm) and MHUI-ACO (ant colony).
//!
//! These search the itemset space stochastically instead of exhaustively. Every itemset
//! they report is a true HUI with its exact utility (utilities are computed from TID lists,
//! never estimated); what they trade away is recall — some HUIs may be missed.
//!
//! **Transaction-guided search.** On sparse data almost every random combination of items
//! occurs in no transaction (utility 0), so unguided operators waste nearly every evaluation.
//! All three algorithms therefore build itemsets from transactions:
//! * a new itemset is a random subset of a random transaction,
//! * an itemset is extended with an item taken from a random transaction that contains it,
//! * a GA/BPSO candidate that occurs nowhere is repaired by dropping random items.
//! Every evaluated itemset therefore occurs in the database. GA children and BPSO particles that
//! land on an already-evaluated itemset are mutated (diversity maintenance), so populations do
//! not collapse onto a few itemsets.
//!
//! * Search space: items whose TWU >= min_utility ("promising" items).
//! * Data: per-item TID lists (budget-aware `ItemListBuilder`) for exact utilities, and a
//!   budget-aware horizontal copy of the filtered database (`PagedDb`) for sampling.
//! * Found HUIs are written immediately; only a 64-bit hash per HUI is kept for de-duplication.
//! * Reproducible: AIR_HUIM_SEED (default 42), AIR_HUIM_ITERS (2000), AIR_HUIM_POP (30),
//!   AIR_HUIM_MAXLEN (8).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufReader};
use std::sync::atomic::Ordering;
use rand::{rngs::StdRng, Rng, SeedableRng};
use crate::mining::core::{context::MiningContext, data_source::DataSource, memory_guard::{Reservation, map_bytes}};
use crate::preprocessing::{db_reader::DbReader, twu_filter::TwuFilter};
use crate::types::{ItemId, ULEntry, Utility};
use super::item_lists::ItemListBuilder;
use super::paged_db::{PagedDb, PagedDbBuilder};
use super::ul_join::{BodyAlloc, BodyCursor, UlBody};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Heuristic { Ga, Bpso, Aco }

fn env_or<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

struct Space<'a> {
    items: Vec<ItemId>,       // promising items, index = position
    index: HashMap<ItemId, usize>,
    bodies: Vec<UlBody>,
    db: PagedDb,
    alloc: BodyAlloc<'a>,
    ctx: &'a MiningContext,
    found: FoundSet,
    cache: HashMap<u64, Utility>,
    cache_res: Reservation,
    cache_cap: usize,
    writer: crate::mining::core::result_writer::ResultWriter,
    evaluations: u64,
    _index_res: Reservation,
}

/// HUIs already written (by itemset hash), so each is output once. Exact while the budget
/// lets it grow; then it becomes a Bloom filter of the same size. A Bloom false positive only
/// skips writing a genuinely new HUI (lower recall); it never adds a wrong or duplicate one.
struct FoundSet {
    exact: Option<HashSet<u64>>,
    bloom: Vec<u64>,
    res: Reservation,
}

impl FoundSet {
    /// Record `k`; true if it was not recorded before.
    fn insert(&mut self, k: u64) -> bool {
        if let Some(set) = &mut self.exact {
            if set.contains(&k) { return false; }
            if set.len() < set.capacity() {
                set.insert(k);
                return true;
            }
            let need = map_bytes::<u64, ()>((set.capacity() * 2).max(16));
            if self.res.try_grow(need.saturating_sub(self.res.bytes())) {
                set.insert(k);
                return true;
            }
            let words = (self.res.bytes().max(4 * 1024) / 8).next_power_of_two();
            self.bloom = vec![0u64; words];
            let old = self.exact.take().unwrap();
            for &x in &old { self.bloom_insert(x); }
            drop(old);
            self.res.resize_force(words * 8);
        }
        self.bloom_insert(k)
    }

    /// Set the 4 bits of `k`; true if at least one was clear (definitely new).
    fn bloom_insert(&mut self, k: u64) -> bool {
        let bits = (self.bloom.len() * 64) as u64;
        let (h1, h2) = (k, k.rotate_left(32) | 1);
        let mut new = false;
        for j in 0..4u64 {
            let b = h1.wrapping_add(j.wrapping_mul(h2)) & (bits - 1);
            let (w, m) = ((b / 64) as usize, 1u64 << (b % 64));
            if self.bloom[w] & m == 0 { new = true; self.bloom[w] |= m; }
        }
        new
    }
}

/// Result of evaluating an itemset.
#[derive(Clone, Copy)]
struct Eval { utility: Utility, new_hui: bool }

impl Space<'_> {
    fn key(sel: &[usize]) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sel.hash(&mut h);
        h.finish()
    }

    /// Visit the transactions containing all selected items, with the itemset's utility in
    /// each, in TID order; `f` returns false to stop. A streaming k-way intersection: one
    /// cursor (one chunk in memory) per selected item, whatever the list lengths.
    fn scan_occurrences(&self, sel: &[usize], mut f: impl FnMut(u32, Utility) -> bool) -> io::Result<()> {
        if sel.is_empty() { return Ok(()); }
        let mut cs: Vec<BodyCursor> = sel.iter().map(|&i| BodyCursor::new(self.alloc, &self.bodies[i])).collect();
        loop {
            let mut target = 0u32;
            for c in cs.iter_mut() {
                match c.head()? { Some(e) => target = target.max(e.tid), None => return Ok(()) }
            }
            let (mut all, mut u) = (true, 0);
            for c in cs.iter_mut() {
                loop {
                    match c.head()? {
                        None => return Ok(()),
                        Some(e) if e.tid < target => c.advance(),
                        Some(e) => {
                            if e.tid == target { u += e.iutils } else { all = false }
                            break;
                        }
                    }
                }
            }
            if all {
                if !f(target, u) { return Ok(()); }
                for c in cs.iter_mut() { c.advance(); }
            }
        }
    }

    fn utility(&mut self, sel: &[usize]) -> io::Result<Utility> {
        if sel.is_empty() { return Ok(0); }
        let k = Self::key(sel);
        if let Some(&u) = self.cache.get(&k) { return Ok(u); }
        self.evaluations += 1;
        let mut u: Utility = 0;
        self.scan_occurrences(sel, |_, x| { u += x; true })?;
        if self.cache.len() < self.cache_cap {
            let before = self.cache.capacity();
            self.cache.insert(k, u);
            if self.cache.capacity() != before {
                self.cache_res.resize_force(map_bytes::<u64, Utility>(self.cache.capacity()));
            }
        }
        Ok(u)
    }

    /// Evaluate; if it is a new HUI, write it.
    fn evaluate(&mut self, sel: &[usize]) -> io::Result<Eval> {
        let u = self.utility(sel)?;
        let mut new_hui = false;
        if u >= self.ctx.min_utility && !sel.is_empty() {
            let k = Self::key(sel);
            if self.found.insert(k) {
                let itemset: Vec<ItemId> = sel.iter().map(|&i| self.items[i]).collect();
                self.writer.write_hui(&itemset, u)?;
                self.ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
                new_hui = true;
            }
        }
        Ok(Eval { utility: u, new_hui })
    }

    /// Promising-item indices of transaction `tid`.
    fn tx_items(&self, tid: u32, out: &mut Vec<usize>) -> io::Result<()> {
        out.clear();
        let mut c = self.db.cursor();
        let t = c.tx(tid)?;
        for k in 0..t.len {
            if let Some(&i) = self.index.get(&t.item(k)) { out.push(i); }
        }
        Ok(())
    }

    /// A random subset (1..=maxlen items) of a random transaction: an itemset that occurs.
    fn random_from_tx(&self, rng: &mut StdRng, maxlen: usize) -> io::Result<Vec<usize>> {
        let mut items = Vec::new();
        for _ in 0..8 {
            let tid = rng.gen_range(0..self.db.len());
            self.tx_items(tid, &mut items)?;
            if !items.is_empty() { break; }
        }
        if items.is_empty() { return Ok(Vec::new()); }
        let len = rng.gen_range(1..=items.len().min(maxlen));
        for k in 0..len {
            let j = rng.gen_range(k..items.len());
            items.swap(k, j);
        }
        items.truncate(len);
        items.sort_unstable();
        Ok(items)
    }

    /// Items that can extend `sel` while keeping it present: the other items of a random
    /// transaction containing `sel`.
    fn extension_candidates(&self, rng: &mut StdRng, sel: &[usize], out: &mut Vec<usize>) -> io::Result<()> {
        out.clear();
        // One uniformly random occurrence (reservoir sampling over the stream).
        let (mut tid, mut seen) = (None, 0u64);
        self.scan_occurrences(sel, |t, _| {
            seen += 1;
            if rng.gen_range(0..seen) == 0 { tid = Some(t); }
            true
        })?;
        let Some(tid) = tid else { return Ok(()) };
        let mut items = Vec::new();
        self.tx_items(tid, &mut items)?;
        out.extend(items.into_iter().filter(|i| sel.binary_search(i).is_err()));
        Ok(())
    }

    /// Does the itemset occur in at least one transaction?
    fn occurs(&self, sel: &[usize]) -> io::Result<bool> {
        let mut any = false;
        self.scan_occurrences(sel, |_, _| { any = true; false })?;
        Ok(any)
    }

    /// Already evaluated (cache hit)?
    fn seen(&self, sel: &[usize]) -> bool {
        self.cache.contains_key(&Self::key(sel))
    }

    /// Diversity maintenance: if `sel` was already evaluated, apply up to three guided
    /// mutations, then fall back to a fresh random subset of a random transaction.
    fn diversify(&self, rng: &mut StdRng, sel: &mut Vec<usize>, maxlen: usize) -> io::Result<()> {
        for _ in 0..3 {
            if sel.is_empty() || !self.seen(sel) { return Ok(()); }
            guided_mutation(self, rng, sel, maxlen)?;
        }
        if sel.is_empty() || self.seen(sel) {
            *sel = self.random_from_tx(rng, maxlen)?;
        }
        Ok(())
    }

    /// Drop random items until the itemset occurs somewhere (empty if nothing remains).
    fn repair(&self, rng: &mut StdRng, sel: &mut Vec<usize>) -> io::Result<()> {
        while !sel.is_empty() && !self.occurs(sel)? {
            let k = rng.gen_range(0..sel.len());
            sel.remove(k);
        }
        Ok(())
    }
}

/// Pick an index with probability proportional to `w` (all weights >= 0).
fn roulette(rng: &mut StdRng, w: &[f64]) -> usize {
    let total: f64 = w.iter().sum();
    if total <= 0.0 { return rng.gen_range(0..w.len()); }
    let mut r = rng.gen_range(0.0..total);
    for (i, &x) in w.iter().enumerate() {
        if r < x { return i; }
        r -= x;
    }
    w.len() - 1
}

/// Add or remove one item, keeping the itemset present in the database.
fn guided_mutation(sp: &Space, rng: &mut StdRng, sel: &mut Vec<usize>, maxlen: usize) -> io::Result<()> {
    if !sel.is_empty() && (sel.len() >= maxlen || rng.gen_bool(0.5)) {
        let k = rng.gen_range(0..sel.len());
        sel.remove(k);
        return Ok(());
    }
    let mut cand = Vec::new();
    sp.extension_candidates(rng, sel, &mut cand)?;
    if !cand.is_empty() {
        let i = cand[rng.gen_range(0..cand.len())];
        if let Err(p) = sel.binary_search(&i) { sel.insert(p, i); }
    }
    Ok(())
}

pub fn run_heuristic(kind: Heuristic, name: &str, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
    let path = source.expect_file(name).to_path_buf();
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;
    let iters: usize = env_or("AIR_HUIM_ITERS", 2000);
    let pop: usize = env_or("AIR_HUIM_POP", 30);
    let maxlen: usize = env_or("AIR_HUIM_MAXLEN", 8).max(1);
    let mut rng = StdRng::seed_from_u64(env_or("AIR_HUIM_SEED", 42u64));

    ctx.progress.set_stage(&format!("{}: Pass 1 (TWU)", name));
    let twu = TwuFilter::new(min).compute(DbReader::new(BufReader::new(File::open(&path)?)).filter_map(Result::ok));

    // Admission control, before pass 2 fills free memory with data that could spill. Fixed:
    // per-item index, TWU / pheromone / velocity-sized arrays, the smallest evaluation cache
    // and found-set, pass-2 buffers. Per worker (one): an evaluation streams one chunk per
    // selected item, plus one pinned DB segment.
    {
        let n = twu.twu.len();
        let estimate = |b: usize| -> (usize, usize) {
            let chunk_b = (b / 256 / 20).clamp(64, 4096) * 20;
            (map_bytes::<ItemId, usize>(n) + n * (4 * 8 + std::mem::size_of::<UlBody>()) + 64 * 48 + 4 * 1024
                 + 2 * (b / 32).clamp(4 * 1024, 1 << 20) + 2 * super::paged_db::segment_bytes_for(b),
             maxlen * chunk_b + super::paged_db::segment_bytes_for(b))
        };
        ctx.admit(name, &estimate)?;
    }

    // Pass 2: TID lists (exact utilities) and a horizontal copy (sampling). The TID of a
    // transaction is its position in the horizontal copy.
    ctx.progress.set_stage(&format!("{}: Pass 2 (TID lists + transactions)", name));
    let alloc = BodyAlloc::new(&ctx.pool, &ctx.guard);
    let mut builder = ItemListBuilder::new(alloc, 0.4);
    let mut dbb = PagedDbBuilder::new(&ctx.pool, &ctx.guard);
    let mut its: Vec<ItemId> = Vec::new();
    let mut us: Vec<Utility> = Vec::new();
    for tx in DbReader::new(BufReader::new(File::open(&path)?)).filter_map(Result::ok) {
        let Some(ftx) = twu.apply(&tx) else { continue };
        its.clear();
        us.clear();
        for e in &ftx.items { its.push(e.item); us.push(e.utility); }
        let tid = dbb.push(&its, &us)?;
        for e in &ftx.items {
            builder.push(e.item, ULEntry { tid, iutils: e.utility, rutils: 0 })?;
        }
    }
    let lists = builder.finish()?;
    let db = dbb.finish()?;
    crate::mining::core::memory_guard::release_free_memory();

    let mut items = Vec::with_capacity(lists.len());
    let mut twus = Vec::with_capacity(lists.len());
    let mut bodies = Vec::with_capacity(lists.len());
    for (item, _, body) in lists {
        items.push(item);
        twus.push(twu.twu.get(&item).copied().unwrap_or(0));
        bodies.push(body);
    }
    let n = items.len();
    let index: HashMap<ItemId, usize> = items.iter().enumerate().map(|(k, &i)| (i, k)).collect();
    let writer = ctx.open_writer()?;
    let mut sp = Space {
        _index_res: ctx.guard.reserve_force(map_bytes::<ItemId, usize>(index.capacity())),
        items, index, bodies, db, alloc, ctx,
        found: FoundSet { exact: Some(HashSet::new()), bloom: Vec::new(), res: ctx.guard.reserve_force(0) },
        cache: HashMap::new(), cache_res: ctx.guard.reserve_force(0),
        cache_cap: (ctx.guard.native_remaining() / 4 / 48).clamp(64, 1 << 22),
        writer, evaluations: 0,
    };
    if n == 0 || sp.db.len() == 0 {
        sp.writer.finalize()?;
        return Ok(0);
    }
    let max_twu = *twus.iter().max().unwrap() as f64;
    let eta: Vec<f64> = twus.iter().map(|&t| (t as f64 / max_twu).max(1e-9)).collect();

    ctx.progress.set_stage(&format!("{}: searching ({} promising items, {} iterations)", name, n, iters));
    match kind {
        Heuristic::Ga => {
            let mut popn: Vec<Vec<usize>> = (0..pop).map(|_| sp.random_from_tx(&mut rng, maxlen)).collect::<io::Result<_>>()?;
            for _ in 0..iters {
                let fit: Vec<f64> = popn.iter().map(|s| sp.evaluate(s).map(|e| e.utility as f64 + 1.0)).collect::<io::Result<_>>()?;
                let mut next = Vec::with_capacity(pop);
                // Elitism: keep the best individual.
                let best = (0..pop).max_by(|&a, &b| fit[a].partial_cmp(&fit[b]).unwrap()).unwrap();
                next.push(popn[best].clone());
                while next.len() < pop {
                    let (a, b) = (roulette(&mut rng, &fit), roulette(&mut rng, &fit));
                    // Uniform crossover over the union of the parents' items, then repair.
                    let mut child: Vec<usize> = popn[a].iter().chain(popn[b].iter())
                        .filter(|_| rng.gen_bool(0.5)).copied().collect();
                    child.sort_unstable();
                    child.dedup();
                    child.truncate(maxlen);
                    sp.repair(&mut rng, &mut child)?;
                    if rng.gen_bool(0.3) || child.is_empty() {
                        guided_mutation(&sp, &mut rng, &mut child, maxlen)?;
                    }
                    if child.is_empty() { child = sp.random_from_tx(&mut rng, maxlen)?; }
                    sp.diversify(&mut rng, &mut child, maxlen)?;
                    next.push(child);
                }
                popn = next;
            }
        }
        Heuristic::Bpso => {
            // Velocities are kept only for items a particle, its personal best or the global
            // best ever touched; every other item has velocity -VMAX (selection probability ~0).
            const VMAX: f64 = 4.0;
            let (w, c1, c2) = (0.7, 1.5, 1.5);
            let sig = |v: f64| 1.0 / (1.0 + (-v).exp());
            let mut pos: Vec<Vec<usize>> = (0..pop).map(|_| sp.random_from_tx(&mut rng, maxlen)).collect::<io::Result<_>>()?;
            let mut vel: Vec<HashMap<usize, f64>> = vec![HashMap::new(); pop];
            let mut pbest = pos.clone();
            let mut pfit: Vec<Utility> = pos.iter().map(|s| sp.evaluate(s).map(|e| e.utility)).collect::<io::Result<_>>()?;
            let g0 = (0..pop).max_by_key(|&i| pfit[i]).unwrap();
            let mut gbest = pbest[g0].clone();
            let mut gfit = pfit[g0];
            for _ in 0..iters {
                for p in 0..pop {
                    let mut cand: Vec<usize> = pos[p].iter().chain(pbest[p].iter()).chain(gbest.iter()).copied().collect();
                    cand.sort_unstable();
                    cand.dedup();
                    let mut next: Vec<usize> = Vec::new();
                    for &i in &cand {
                        let x = pos[p].binary_search(&i).is_ok() as i32 as f64;
                        let pb = pbest[p].binary_search(&i).is_ok() as i32 as f64;
                        let gb = gbest.binary_search(&i).is_ok() as i32 as f64;
                        let v = vel[p].entry(i).or_insert(-VMAX / 2.0);
                        *v = (w * *v + c1 * rng.gen_range(0.0f64..1.0) * (pb - x)
                              + c2 * rng.gen_range(0.0f64..1.0) * (gb - x)).clamp(-VMAX, VMAX);
                        if rng.gen_range(0.0f64..1.0) < sig(*v) { next.push(i); }
                    }
                    next.truncate(maxlen);
                    sp.repair(&mut rng, &mut next)?;
                    // Exploration: extend with an item from a transaction containing the particle.
                    if rng.gen_bool(0.3) || next.is_empty() {
                        guided_mutation(&sp, &mut rng, &mut next, maxlen)?;
                    }
                    if next.is_empty() { next = sp.random_from_tx(&mut rng, maxlen)?; }
                    sp.diversify(&mut rng, &mut next, maxlen)?;
                    let f = sp.evaluate(&next)?.utility;
                    if f > pfit[p] { pfit[p] = f; pbest[p] = next.clone(); }
                    if f > gfit { gfit = f; gbest = next.clone(); }
                    pos[p] = next;
                    if vel[p].len() > 4 * maxlen * 8 { vel[p].retain(|_, v| *v > -VMAX + 0.5); }
                }
            }
        }
        Heuristic::Aco => {
            // Ants start at an item chosen by tau^a * eta^b, then repeatedly add an item taken from
            // a random transaction that contains the current itemset (same weighting), stopping
            // at a random length. Pheromone evaporates (rho), has a floor (TAU_MIN) so the colony
            // keeps exploring, and is deposited on items of high-utility and newly found HUIs.
            let (alpha, beta, rho) = (1.0f64, 1.0f64, 0.1f64);
            const TAU_MIN: f64 = 0.2;
            let mut tau = vec![1.0f64; n];
            let mut cand: Vec<usize> = Vec::new();
            for _ in 0..iters {
                let weight: Vec<f64> = (0..n).map(|i| tau[i].powf(alpha) * eta[i].powf(beta)).collect();
                let mut sols: Vec<(Vec<usize>, Eval)> = Vec::with_capacity(pop);
                for _ in 0..pop {
                    let len = rng.gen_range(1..=maxlen);
                    let mut sel = vec![roulette(&mut rng, &weight)];
                    while sel.len() < len {
                        sp.extension_candidates(&mut rng, &sel, &mut cand)?;
                        if cand.is_empty() { break; }
                        let w: Vec<f64> = cand.iter().map(|&i| weight[i]).collect();
                        let i = cand[roulette(&mut rng, &w)];
                        if let Err(p) = sel.binary_search(&i) { sel.insert(p, i); }
                    }
                    let e = sp.evaluate(&sel)?;
                    sols.push((sel, e));
                }
                for t in tau.iter_mut() { *t = (*t * (1.0 - rho)).max(TAU_MIN); }
                for (sel, e) in &sols {
                    let mut d = (e.utility as f64 / min.max(1) as f64).min(2.0);
                    if e.new_hui { d += 1.0; }
                    for &i in sel { tau[i] += d / sel.len() as f64; }
                }
            }
        }
    }
    ctx.progress.set_stage(&format!("{}: done ({} evaluations)", name, sp.evaluations));
    sp.writer.finalize()?;
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}
