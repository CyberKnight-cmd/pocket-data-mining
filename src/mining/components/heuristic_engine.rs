//! Shared engine for the bio-inspired (approximate) HUIM algorithms:
//! HUIM-GA (genetic algorithm), HUIM-BPSO (binary particle swarm) and MHUI-ACO (ant colony).
//!
//! These search the itemset space stochastically instead of exhaustively. Every itemset
//! they report is a true HUI with its exact utility (utilities are computed from TID lists,
//! never estimated); what they trade away is recall — some HUIs may be missed.
//!
//! * Search space: items whose TWU >= min_utility ("promising" items).
//! * Fitness: exact utility, by intersecting the items' TID lists (built by the
//!   budget-aware `ItemListBuilder`, so lists live in RAM or are paged as the budget allows).
//! * Found HUIs are written immediately; only a 64-bit hash per HUI is kept for de-duplication.
//! * Runs are reproducible: seeded RNG (AIR_HUIM_SEED, default 42), iterations
//!   (AIR_HUIM_ITERS, default 2000), population (AIR_HUIM_POP, default 30),
//!   maximum itemset length explored (AIR_HUIM_MAXLEN, default 8).

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
use super::ul_join::{BodyAlloc, UlBody};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Heuristic { Ga, Bpso, Aco }

fn env_or<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

struct Space<'a> {
    items: Vec<ItemId>,       // promising items, index = position
    twu: Vec<Utility>,
    bodies: Vec<UlBody>,
    alloc: BodyAlloc<'a>,
    ctx: &'a MiningContext,
    found: HashSet<u64>,
    found_res: Reservation,
    cache: HashMap<u64, Utility>,
    cache_res: Reservation,
    cache_cap: usize,
    writer: crate::mining::core::result_writer::ResultWriter,
    evaluations: u64,
}

impl Space<'_> {
    fn key(sel: &[usize]) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sel.hash(&mut h);
        h.finish()
    }

    /// Exact utility of the itemset made of the selected item indices (sorted, distinct).
    fn utility(&mut self, sel: &[usize]) -> io::Result<Utility> {
        if sel.is_empty() { return Ok(0); }
        let k = Self::key(sel);
        if let Some(&u) = self.cache.get(&k) { return Ok(u); }
        self.evaluations += 1;
        // Intersect starting from the shortest list.
        let mut order: Vec<usize> = sel.to_vec();
        let lens: Vec<usize> = order.iter().map(|&i| self.alloc.view(&self.bodies[i]).map(|v| v.len())).collect::<io::Result<_>>()?;
        let mut idx: Vec<usize> = (0..order.len()).collect();
        idx.sort_by_key(|&j| lens[j]);
        order = idx.iter().map(|&j| order[j]).collect();
        let first = self.alloc.view(&self.bodies[order[0]])?;
        let _r = self.ctx.guard.reserve_force(first.len() * 12);
        let mut acc: Vec<(u32, Utility)> = first.iter().map(|e| (e.tid, e.iutils)).collect();
        drop(first);
        for &i in &order[1..] {
            if acc.is_empty() { break; }
            let other = self.alloc.view(&self.bodies[i])?;
            let (mut a, mut b, mut w) = (0, 0, 0);
            while a < acc.len() && b < other.len() {
                let (ta, tb) = (acc[a].0, other[b].tid);
                if ta == tb {
                    acc[w] = (ta, acc[a].1 + other[b].iutils);
                    w += 1; a += 1; b += 1;
                } else if ta < tb { a += 1 } else { b += 1 }
            }
            acc.truncate(w);
        }
        let u: Utility = acc.iter().map(|x| x.1).sum();
        if self.cache.len() < self.cache_cap {
            let before = self.cache.capacity();
            self.cache.insert(k, u);
            if self.cache.capacity() != before {
                self.cache_res.resize_force(map_bytes::<u64, Utility>(self.cache.capacity()));
            }
        }
        Ok(u)
    }

    /// Evaluate; if it is a new HUI, write it. Returns the utility (the fitness).
    fn evaluate(&mut self, sel: &[usize]) -> io::Result<Utility> {
        let u = self.utility(sel)?;
        if u >= self.ctx.min_utility && !sel.is_empty() {
            let k = Self::key(sel);
            let before = self.found.capacity();
            if self.found.insert(k) {
                if self.found.capacity() != before {
                    self.found_res.resize_force(map_bytes::<u64, ()>(self.found.capacity()));
                }
                let itemset: Vec<ItemId> = sel.iter().map(|&i| self.items[i]).collect();
                self.writer.write_hui(&itemset, u)?;
                self.ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(u)
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

/// A random itemset of 1..=maxlen items, chosen with probability proportional to TWU.
fn random_selection(rng: &mut StdRng, twu_w: &[f64], maxlen: usize) -> Vec<usize> {
    let len = rng.gen_range(1..=maxlen.min(twu_w.len()).max(1));
    let mut sel: Vec<usize> = Vec::with_capacity(len);
    for _ in 0..len * 3 {
        if sel.len() >= len { break; }
        let i = roulette(rng, twu_w);
        if !sel.contains(&i) { sel.push(i); }
    }
    sel.sort_unstable();
    sel
}

pub fn run_heuristic(kind: Heuristic, name: &str, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
    let path = source.expect_file(name).to_path_buf();
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;
    let iters: usize = env_or("AIR_HUIM_ITERS", 2000);
    let pop: usize = env_or("AIR_HUIM_POP", 30);
    let maxlen: usize = env_or("AIR_HUIM_MAXLEN", 8);
    let mut rng = StdRng::seed_from_u64(env_or("AIR_HUIM_SEED", 42u64));

    ctx.progress.set_stage(&format!("{}: Pass 1 (TWU)", name));
    let twu = TwuFilter::new(min).compute(DbReader::new(BufReader::new(File::open(&path)?)).filter_map(Result::ok));

    ctx.progress.set_stage(&format!("{}: Pass 2 (TID lists)", name));
    let alloc = BodyAlloc::new(&ctx.pool, &ctx.guard);
    let mut builder = ItemListBuilder::new(alloc, 0.5);
    for tx in DbReader::new(BufReader::new(File::open(&path)?)).filter_map(Result::ok) {
        let Some(ftx) = twu.apply(&tx) else { continue };
        for e in &ftx.items {
            builder.push(e.item, ULEntry { tid: ftx.tid, iutils: e.utility, rutils: 0 })?;
        }
    }
    let lists = builder.finish()?;
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
    let writer = ctx.open_writer()?;
    let mut sp = Space {
        items, twu: twus, bodies, alloc, ctx,
        found: HashSet::new(), found_res: ctx.guard.reserve_force(0),
        cache: HashMap::new(), cache_res: ctx.guard.reserve_force(0),
        cache_cap: (ctx.guard.native_remaining() / 4 / 48).clamp(1024, 1 << 22),
        writer, evaluations: 0,
    };
    if n == 0 {
        sp.writer.finalize()?;
        return Ok(0);
    }
    let max_twu = *sp.twu.iter().max().unwrap() as f64;
    let twu_w: Vec<f64> = sp.twu.iter().map(|&t| t as f64 / max_twu).collect();
    let maxlen = maxlen.min(n).max(1);

    ctx.progress.set_stage(&format!("{}: searching ({} promising items, {} iterations)", name, n, iters));
    match kind {
        Heuristic::Ga => {
            let mut popn: Vec<Vec<usize>> = (0..pop).map(|_| random_selection(&mut rng, &twu_w, maxlen)).collect();
            for _ in 0..iters {
                let fit: Vec<f64> = popn.iter().map(|s| sp.evaluate(s).map(|u| u as f64 + 1.0)).collect::<io::Result<_>>()?;
                let mut next = Vec::with_capacity(pop);
                // Elitism: keep the best individual.
                let best = (0..pop).max_by(|&a, &b| fit[a].partial_cmp(&fit[b]).unwrap()).unwrap();
                next.push(popn[best].clone());
                while next.len() < pop {
                    let (a, b) = (roulette(&mut rng, &fit), roulette(&mut rng, &fit));
                    // Uniform crossover over the union of the parents' items.
                    let mut child: Vec<usize> = popn[a].iter().chain(popn[b].iter())
                        .filter(|_| rng.gen_bool(0.5)).copied().collect();
                    child.sort_unstable();
                    child.dedup();
                    // Mutation: add or remove one item.
                    if rng.gen_bool(0.3) || child.is_empty() {
                        if !child.is_empty() && (child.len() >= maxlen || rng.gen_bool(0.5)) {
                            let k = rng.gen_range(0..child.len());
                            child.remove(k);
                        } else {
                            let i = roulette(&mut rng, &twu_w);
                            if let Err(p) = child.binary_search(&i) { child.insert(p, i); }
                        }
                    }
                    child.truncate(maxlen);
                    if child.is_empty() { child = random_selection(&mut rng, &twu_w, maxlen); }
                    next.push(child);
                }
                popn = next;
            }
        }
        Heuristic::Bpso => {
            // Velocities are kept only for items a particle, its personal best or the global
            // best ever touched; every other item has velocity -VMAX (selection probability
            // ~0), which keeps BPSO practical with tens of thousands of items.
            const VMAX: f64 = 4.0;
            let (w, c1, c2) = (0.7, 1.5, 1.5);
            let sig = |v: f64| 1.0 / (1.0 + (-v).exp());
            let mut pos: Vec<Vec<usize>> = (0..pop).map(|_| random_selection(&mut rng, &twu_w, maxlen)).collect();
            let mut vel: Vec<HashMap<usize, f64>> = vec![HashMap::new(); pop];
            let mut pbest = pos.clone();
            let mut pfit: Vec<Utility> = pos.iter().map(|s| sp.evaluate(s)).collect::<io::Result<_>>()?;
            let mut g = (0..pop).max_by_key(|&i| pfit[i]).unwrap();
            let mut gbest = pbest[g].clone();
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
                        *v = (w * *v + c1 * rng.gen_range(0.0f64..1.0) * (pb - x) + c2 * rng.gen_range(0.0f64..1.0) * (gb - x)).clamp(-VMAX, VMAX);
                        if rng.gen_range(0.0f64..1.0) < sig(*v) { next.push(i); }
                    }
                    // Occasional exploration of a new item.
                    if rng.gen_bool(0.1) {
                        let i = roulette(&mut rng, &twu_w);
                        if let Err(k) = next.binary_search(&i) { next.insert(k, i); }
                    }
                    next.truncate(maxlen);
                    if next.is_empty() { next = random_selection(&mut rng, &twu_w, maxlen); }
                    let f = sp.evaluate(&next)?;
                    if f > pfit[p] { pfit[p] = f; pbest[p] = next.clone(); }
                    if f > pfit[g] { g = p; gbest = next.clone(); }
                    pos[p] = next;
                    if vel[p].len() > 4 * maxlen * 8 { vel[p].retain(|_, v| *v > -VMAX + 0.5); }
                }
            }
        }
        Heuristic::Aco => {
            // Ants build itemsets item by item, choosing with probability ~ tau^a * eta^b
            // (eta = normalised TWU); pheromone evaporates and is deposited on the items
            // of high-utility solutions.
            let (alpha, beta, rho) = (1.0f64, 2.0f64, 0.1f64);
            let mut tau = vec![1.0f64; n];
            for _ in 0..iters {
                let mut sols: Vec<(Vec<usize>, Utility)> = Vec::with_capacity(pop);
                let weight: Vec<f64> = (0..n).map(|i| tau[i].powf(alpha) * twu_w[i].powf(beta)).collect();
                for _ in 0..pop {
                    let len = rng.gen_range(1..=maxlen);
                    let mut sel: Vec<usize> = Vec::with_capacity(len);
                    for _ in 0..len * 3 {
                        if sel.len() >= len { break; }
                        let i = roulette(&mut rng, &weight);
                        if !sel.contains(&i) { sel.push(i); }
                    }
                    sel.sort_unstable();
                    let u = sp.evaluate(&sel)?;
                    sols.push((sel, u));
                }
                for t in tau.iter_mut() { *t = (*t * (1.0 - rho)).max(1e-3); }
                for (sel, u) in &sols {
                    let d = (*u as f64 / min.max(1) as f64).min(5.0);
                    for &i in sel { tau[i] += d; }
                }
            }
        }
    }
    ctx.progress.set_stage(&format!("{}: done ({} evaluations)", name, sp.evaluations));
    sp.writer.finalize()?;
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}
