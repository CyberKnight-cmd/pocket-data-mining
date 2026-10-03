//! Shared, budget-aware depth-first utility-list miner.
//!
//! FHM, FHM+, HUI-Miner, HUP-Miner, mHUIMiner, TKO, TKU and REPT (and the Family 6/7
//! wrappers over FHM) were eight near-identical copies of this search. They differ only
//! in whether EUCS pruning is used, whether length constraints apply, and whether the
//! threshold is raised by a Top-K heap — so they share this engine and every one of
//! them gets the same memory accounting:
//!
//! * 1-itemset lists are built by `ItemListBuilder` (spills to disk when over budget),
//! * EUCS gets a capped share of the budget and is dropped (not truncated) if it overflows,
//! * every derived list is kept in RAM only while the budget allows, else paged,
//! * finished sibling lists are released as soon as the DFS moves past them.

use std::{cmp::Reverse, collections::{BinaryHeap, HashMap}, io, sync::{Arc, Mutex, atomic::{AtomicI64, Ordering}}};
use std::fs::File;
use std::io::BufReader;
use smallvec::SmallVec;
use crate::{
    mining::core::{context::{MiningContext, WriterProxy}, data_source::DataSource, result_writer::ResultWriter,
                   memory_guard::{Reservation, vec_bytes}},
    preprocessing::{db_reader::DbReader, twu_filter::TwuFilter},
    prefetch::{prefetch_queue::PrefetchQueue, predictor::AccessPredictor, utility_predictor::UtilityPredictor},
    types::{ItemId, Utility, ULEntry, UtilityList},
};
use super::{
    eucs::Eucs,
    item_lists::ItemListBuilder,
    traversal::{TraversalContext, CandidateExtension},
    ul_join::{join_srcs, materialize, BodyAlloc, BodyCursor, CostStats, DropPolicy, LaPrune, ListSrc,
              RematMode, SpillArena, UlBody},
};

/// Which variant of the utility-list search to run.
#[derive(Clone, Copy, Default)]
pub struct UlMinerConfig {
    pub name: &'static str,
    /// Use EUCS co-occurrence pruning (FHM family).
    pub use_eucs: bool,
    /// Honour ctx.min_length / ctx.max_length (FHM+).
    pub length_constraints: bool,
    pub enable_prefetch: bool,
    /// High *average*-utility mining (HAUI-Miner): an itemset X qualifies when
    /// u(X) / |X| >= min_utility. List entries carry the transaction's maximum item
    /// utility in `rutils`, so a list's `sum_rutils` is the anti-monotone auub bound.
    pub average: bool,
    /// PU-prune with this many tid partitions (HUP-Miner); 0 = off.
    pub pu_partitions: usize,
    /// LA-prune: abandon a join once its upper bound falls below the threshold (HUP-Miner).
    pub la_prune: bool,
    /// Skip joins of item pairs that never occur together (mHUIMiner's role for the
    /// IHUP-tree: no utility list is built for an itemset absent from the database).
    pub cooccur_filter: bool,
    /// Multiple minimum utilities (HUIM-MMU): item i has threshold
    /// mu(i) = max(min_utility, beta * u(i)); X qualifies when u(X) >= min over X of mu.
    pub mmu_beta: Option<f64>,
    /// Top-K: before mining, raise the threshold to the K-th best exact utility among all
    /// 1- and 2-itemsets (REPT's pre-evaluation).
    pub topk_seed: bool,
    /// Incremental mining (IncFHM): use only transactions with tid < `max_tid` (0 = all) and
    /// explore only itemsets that occur in a transaction with tid >= `min_tid`.
    pub min_tid: u32,
    pub max_tid: u32,
    /// Fixed processing order by item id (lists stay valid as TWU changes between batches).
    pub order_by_item: bool,
}

/// One candidate extension: its utility list, body, and (PU-prune) per-partition bound sums.
pub struct Ext {
    pub ul: UtilityList,
    pub body: UlBody,
    pub pu: Option<Box<[Utility]>>,
    /// Rematerialisation recipe: this list = join(parent prefix, parent P·x, parent sibling
    /// `recipe`). Needed when `body` is `Dropped`.
    pub recipe: Option<u32>,
}

/// The parent search level, i.e. what this level's lists were joined from. A dropped list at
/// this level is recomputed as join(prefix, px, sibs[recipe]), recursively if that sibling was
/// dropped too.
struct Parent<'p> {
    prefix: Option<&'p UlBody>,
    px: &'p UlBody,
    sibs: Sibs<'p>,
    up: Option<&'p Parent<'p>>,
}

#[derive(Clone, Copy)]
enum Sibs<'p> {
    /// Extensions of the parent level.
    Exts(&'p [Ext]),
    /// The 1-itemset lists (always stored).
    Lists(&'p [(ItemId, UtilityList, UlBody)]),
}

impl<'p> Sibs<'p> {
    fn body(self, j: u32) -> &'p UlBody {
        match self { Sibs::Exts(e) => &e[j as usize].body, Sibs::Lists(l) => &l[j as usize].2 }
    }
    fn recipe(self, j: u32) -> Option<u32> {
        match self { Sibs::Exts(e) => e[j as usize].recipe, Sibs::Lists(_) => None }
    }
}

/// Readable source of a list: the stored body, or (if dropped) a lazy join of its parents.
fn src_for<'p>(parent: Option<&'p Parent<'p>>, body: &'p UlBody, recipe: Option<u32>) -> ListSrc<'p> {
    if !body.is_dropped() {
        return ListSrc::Body(body);
    }
    let p = parent.expect("a dropped list always has a parent level");
    let j = recipe.expect("a dropped list always has a recipe");
    ListSrc::Join(Box::new(super::ul_join::JoinSrc {
        prefix: p.prefix.map(ListSrc::Body),
        px: ListSrc::Body(p.px),
        py: src_for(p.up, p.sibs.body(j), p.sibs.recipe(j)),
    }))
}

/// Shared Top-K state: a min-heap of the best K itemsets and the raised threshold.
pub struct TopKState {
    heap: Mutex<BinaryHeap<Reverse<(Utility, Vec<ItemId>)>>>,
    threshold: AtomicI64,
    k: usize,
    _res: Reservation,
}

impl TopKState {
    /// The heap must hold K itemsets at once; refuse a K the budget cannot hold
    /// instead of silently exceeding it.
    fn new(k: usize, ctx: &MiningContext) -> io::Result<Self> {
        // K entries of (utility, small itemset vec): ~64 bytes each.
        let need = k.saturating_add(1).saturating_mul(64);
        let res = ctx.guard.reserve(need).ok_or_else(|| io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!(
                "Top-K with K={} needs ~{:.1} MB for the result heap, but only {:.1} MB of the budget is free; \
                 lower K or raise the budget",
                k, need as f64 / 1048576.0, ctx.guard.native_remaining() as f64 / 1048576.0
            ),
        ))?;
        Ok(Self {
            heap: Mutex::new(BinaryHeap::new()),
            threshold: AtomicI64::new(0),
            k,
            _res: res,
        })
    }

    fn update(&self, utility: Utility, itemset: Vec<ItemId>) -> Utility {
        let mut heap = self.heap.lock().unwrap();
        heap.push(Reverse((utility, itemset)));
        if heap.len() > self.k {
            heap.pop();
        }
        if heap.len() == self.k {
            if let Some(&Reverse((min_u, _))) = heap.peek() {
                self.threshold.fetch_max(min_u, Ordering::Relaxed);
            }
        }
        self.threshold.load(Ordering::Relaxed)
    }

    fn get(&self) -> Utility {
        self.threshold.load(Ordering::Relaxed)
    }
}

struct Search<'a> {
    cfg: UlMinerConfig,
    ctx: &'a MiningContext,
    eucs: Option<&'a Eucs>,
    cooccur: Option<&'a Eucs>,
    top_k: Option<&'a TopKState>,
    n_tx: u64,
    /// HUIM-MMU per-item thresholds.
    mmu: Option<&'a HashMap<ItemId, Utility>>,
    min_tid: u32,
    /// Recompute-vs-spill: mode, measured costs, flash-write weight.
    remat: RematMode,
    stats: CostStats,
    write_weight: f64,
}

impl Search<'_> {
    /// Threshold for itemsets whose first (lowest-ranked) item is `first`.
    /// With multiple minimum utilities, items are ordered by ascending mu, so the first item
    /// carries the itemset's least minimum utility (sorted downward closure).
    fn threshold(&self, first: ItemId) -> Utility {
        if let Some(s) = self.top_k { return s.get(); }
        if let Some(m) = self.mmu { return m.get(&first).copied().unwrap_or(self.ctx.min_utility); }
        self.ctx.min_utility
    }

    fn length_ok(&self, len: usize) -> bool {
        !self.cfg.length_constraints || (len >= self.ctx.min_length && len <= self.ctx.max_length)
    }

    fn may_extend(&self, len: usize) -> bool {
        !self.cfg.length_constraints || len < self.ctx.max_length
    }

    /// Upper bound of a list's extensions: TWU-style (iutil + rutil), or auub.
    fn bound(&self, ul: &UtilityList) -> Utility {
        if self.cfg.average { ul.sum_rutils } else { ul.sum_iutils + ul.sum_rutils }
    }

    fn prune(&self, ul: &UtilityList, thresh: Utility) -> bool {
        ul.len == 0 || self.bound(ul) < thresh
    }

    /// Incremental mode: an itemset that does not occur in the new batch keeps its utility,
    /// and neither do its supersets — skip the whole subtree.
    fn untouched(&self, alloc: BodyAlloc, body: &UlBody) -> io::Result<bool> {
        if self.min_tid == 0 { return Ok(false); }
        Ok(body.last_tid(alloc)?.is_none_or(|t| t < self.min_tid))
    }

    fn is_hui(&self, ul: &UtilityList, thresh: Utility) -> bool {
        if ul.len == 0 { return false; }
        if self.cfg.average {
            ul.sum_iutils >= thresh.saturating_mul(ul.itemset.len() as Utility)
        } else {
            ul.sum_iutils >= thresh
        }
    }

    /// Pruning that can rule out joining x with y before any work is done.
    fn skip_pair(&self, x: ItemId, y: ItemId, px: &Option<Box<[Utility]>>, py: &Option<Box<[Utility]>>, thresh: Utility) -> bool {
        if self.eucs.is_some_and(|e| e.can_prune(x, y, thresh)) { return true; }
        // Existence filter: a pair that never co-occurs (threshold 1 = "seen at all").
        if self.cooccur.is_some_and(|e| e.can_prune(x, y, 1)) { return true; }
        // PU-prune: P·x·y only occurs in partitions where P·y occurs.
        if let (Some(a), Some(b)) = (px, py) {
            let ub: Utility = a.iter().zip(b.iter()).filter(|p| *p.1 > 0).map(|p| *p.0).sum();
            if ub < thresh { return true; }
        }
        false
    }

    /// Per-partition bound sums of a list (PU-prune), or None when disabled.
    fn partition_sums(&self, alloc: BodyAlloc, body: &UlBody) -> io::Result<Option<Box<[Utility]>>> {
        let p = self.cfg.pu_partitions;
        if p == 0 { return Ok(None); }
        let mut sums = vec![0 as Utility; p];
        let n = self.n_tx.max(1);
        let mut c = BodyCursor::new(alloc, body);
        while let Some(e) = c.head()? {
            let k = ((e.tid as u64 * p as u64) / n).min(p as u64 - 1) as usize;
            sums[k] += if self.cfg.average { e.rutils } else { e.iutils + e.rutils };
            c.advance();
        }
        Ok(Some(sums.into_boxed_slice()))
    }

    /// Join two sources into a new extension. `uses` = how many later joins will read the new
    /// list as P·y (drives the recompute-vs-spill decision); `recipe` = sibling index of `py`.
    #[allow(clippy::too_many_arguments)]
    fn join<'p>(&'p self, alloc: BodyAlloc<'p>, itemset: SmallVec<[ItemId; 8]>, prefix: Option<&ListSrc<'p>>,
                px: &ListSrc<'p>, py: &ListSrc<'p>, px_ul: &UtilityList, py_ul: &UtilityList, thresh: Utility,
                uses: u32, recipe: u32) -> io::Result<Option<Ext>> {
        let la = self.cfg.la_prune.then(|| LaPrune { start: self.bound(px_ul), threshold: thresh, average: self.cfg.average });
        let policy = DropPolicy {
            mode: self.remat,
            uses,
            recompute_entries: px_ul.len as u64 + py_ul.len as u64,
            max_len: px_ul.len.min(py_ul.len) as u64,
            write_weight: self.write_weight,
            stats: &self.stats,
        };
        // Partition sums (PU-prune) are taken from the join's output stream, so they are
        // available even when the list itself is dropped.
        let p = self.cfg.pu_partitions;
        let mut sums = vec![0 as Utility; p];
        let n = self.n_tx.max(1);
        let average = self.cfg.average;
        let mut acc = |e: &ULEntry| {
            let k = ((e.tid as u64 * p as u64) / n).min(p as u64 - 1) as usize;
            sums[k] += if average { e.rutils } else { e.iutils + e.rutils };
        };
        let on_entry: Option<&mut dyn FnMut(&ULEntry)> = if p > 0 { Some(&mut acc) } else { None };
        // Streaming join: constant working set (a few chunks), whatever the list lengths.
        let Some((ul, body)) = join_srcs(itemset, prefix, px, py, alloc, la, Some(policy), on_entry)? else { return Ok(None) };
        if ul.len == 0 { return Ok(None); }
        let pu = (p > 0).then(|| sums.into_boxed_slice());
        if let UlBody::InMemory(..) = &body {
            self.ctx.progress.fast_path_writes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(Ext { ul, body, pu, recipe: Some(recipe) }))
    }

    /// Record a HUI and return the (possibly raised) threshold.
    fn emit(&self, itemset: &[ItemId], utility: Utility, thresh: Utility, writer: &mut WriterProxy) -> io::Result<Utility> {
        if let Some(state) = self.top_k {
            return Ok(state.update(utility, itemset.to_vec()));
        }
        writer.write_hui(itemset, utility)?;
        self.ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
        Ok(thresh)
    }

    fn search<'p>(
        &'p self,
        alloc: BodyAlloc<'p>,
        prefix_body: Option<&'p UlBody>,
        parent: Option<&'p Parent<'p>>,
        mut extensions: Vec<Ext>,
        writer: &mut WriterProxy,
    ) -> io::Result<()> {
        let ctx = self.ctx;
        // Headers (itemset + sums + partition sums) are small but scale with the search width.
        let _hdr = ctx.guard.reserve_force(vec_bytes::<Ext>(extensions.capacity())
            + extensions.len() * self.cfg.pu_partitions * 8);

        for i in 0..extensions.len() {
            let mut thresh = self.threshold(extensions[i].ul.itemset[0]);
            if self.prune(&extensions[i].ul, thresh) {
                extensions[i].body = UlBody::InMemory(Vec::new(), None);
                continue;
            }
            // A dropped list is about to be used many times (as P·x and as the prefix of its
            // children): rematerialise it once now.
            if extensions[i].body.is_dropped() {
                let body = {
                    let src = src_for(parent, &extensions[i].body, extensions[i].recipe);
                    materialize(&src, alloc)?
                };
                extensions[i].body = body;
            }
            {
                let exts: &[Ext] = &extensions;
                let px_ext = &exts[i];
                let depth = px_ext.ul.itemset.len();
                ctx.progress.current_depth.store(depth, Ordering::Relaxed);
                ctx.progress.fast_path_reads.fetch_add(1, Ordering::Relaxed);
                if self.untouched(alloc, &px_ext.body)? {
                    continue;
                }

                let itemset_px: SmallVec<[ItemId; 8]> = px_ext.ul.itemset.clone();
                ctx.progress.set_active_prefix(&itemset_px);
                if self.is_hui(&px_ext.ul, thresh) && self.length_ok(depth) {
                    thresh = self.emit(&itemset_px, px_ext.ul.sum_iutils, thresh, writer)?;
                }

                if self.may_extend(depth) {
                    let item_x = *itemset_px.last().unwrap();
                    let prefix_src = prefix_body.map(ListSrc::Body);
                    let px_src = ListSrc::Body(&px_ext.body);
                    let mut next: Vec<Ext> = Vec::new();
                    for j in (i + 1)..exts.len() {
                        let py_ext = &exts[j];
                        let item_y = *py_ext.ul.itemset.last().unwrap();
                        if self.skip_pair(item_x, item_y, &px_ext.pu, &py_ext.pu, thresh) {
                            continue;
                        }
                        let mut new_itemset = itemset_px.clone();
                        new_itemset.push(item_y);
                        // A dropped sibling is read through a lazy join of its parents.
                        let py_src = src_for(parent, &py_ext.body, py_ext.recipe);
                        let uses = next.len() as u32;
                        if let Some(e) = self.join(alloc, new_itemset, prefix_src.as_ref(), &px_src, &py_src,
                                                   &px_ext.ul, &py_ext.ul, thresh, uses, j as u32)? {
                            next.push(e);
                        }
                    }
                    if !next.is_empty() {
                        let child = Parent { prefix: prefix_body, px: &px_ext.body, sibs: Sibs::Exts(exts), up: parent };
                        self.search(alloc, Some(&px_ext.body), Some(&child), next, writer)?;
                    }
                }
            }
            // No later sibling joins with this one as `px` or `py` any more: release it now.
            extensions[i].body = UlBody::InMemory(Vec::new(), None);
            extensions[i].pu = None;
        }
        Ok(())
    }
}

/// Run a utility-list miner variant over a file dataset.
pub fn run_ul_miner(cfg: UlMinerConfig, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
    let dataset_path = source.expect_file(cfg.name).to_path_buf();

    let prefetch_queue = if cfg.enable_prefetch {
        Some(PrefetchQueue::new(Arc::clone(&ctx.store)))
    } else {
        None
    };
    let predictor: Box<dyn AccessPredictor> = Box::new(UtilityPredictor);

    let top_k = ctx.k.map(|k| TopKState::new(k as usize, ctx)).transpose()?;
    let initial_threshold = if top_k.is_some() { 0 } else { ctx.min_utility };

    // The OS safety net may lower the budget; apply it before sizing anything from it.
    ctx.apply_os_safety_net();

    // Pass 1: TWU per item.
    ctx.progress.set_stage("Pass 1: TWU filtering");
    let max_tid = if cfg.max_tid == 0 { u32::MAX } else { cfg.max_tid };
    let db_reader = DbReader::new(BufReader::new(File::open(&dataset_path)?));
    let twu = TwuFilter::new(initial_threshold).compute(db_reader.filter_map(Result::ok).take_while(|t| t.tid < max_tid));

    // Admission control: what cannot be spilled. Fixed: per-item headers and maps, the output
    // queue, the list builder's segment buffers. Per worker: a streaming join (3 pinned input
    // chunks + 1 output chunk), its spill-arena page, and extension headers along the DFS path.
    {
        let n = twu.twu.len().max(1);
        let ext_b = std::mem::size_of::<Ext>() + cfg.pu_partitions * 8;
        let multi = ctx.threads > 1;
        // Same size rules as ul_join::chunk_entries, arena pages, builder segments, output queue.
        let estimate = |b: usize| -> (usize, usize) {
            let chunk_b = (b / 256 / 20).clamp(64, 4096) * 20;
            let queue = if multi { (b / 64).clamp(64 * 96, 100_000 * 96) } else { 0 };
            let fixed = n * (std::mem::size_of::<(ItemId, UtilityList, UlBody)>() + 96)
                + 2 * (b / 32).clamp(4 * 1024, 1 << 20) + queue;
            let per_thread = 4 * chunk_b + (b / 64).clamp(4 * 1024, 256 * 1024) + 2 * n.min(4096) * ext_b;
            (fixed, per_thread)
        };
        ctx.admit(cfg.name, &estimate)?;
    }

    // HUIM-MMU: per-item minimum utilities mu(i) = max(min_utility, beta * u(i)). They fix the
    // processing order (ascending mu), which remaining utilities must follow, so they are
    // computed before the lists are built.
    let mmu: Option<HashMap<ItemId, Utility>> = match cfg.mmu_beta {
        None => None,
        Some(beta) => {
            let mut item_u: HashMap<ItemId, Utility> = HashMap::new();
            for tx in DbReader::new(BufReader::new(File::open(&dataset_path)?)).filter_map(Result::ok) {
                for e in &tx.items {
                    if twu.passes(e.item) { *item_u.entry(e.item).or_insert(0) += e.utility; }
                }
            }
            Some(item_u.into_iter().map(|(i, u)| (i, ctx.min_utility.max((beta * u as f64) as Utility))).collect())
        }
    };
    let order_key = |i: ItemId| -> (Utility, Utility, ItemId) {
        if cfg.order_by_item { return (0, 0, i); }
        let t = twu.twu.get(&i).copied().unwrap_or(0);
        match &mmu { Some(m) => (m.get(&i).copied().unwrap_or(0), t, i), None => (0, t, i) }
    };

    // Pass 2: 1-itemset utility lists (+ EUCS), both within budget.
    ctx.progress.set_stage("Pass 2: EUCS & 1-Itemsets");
    let alloc = BodyAlloc::new(&ctx.pool, &ctx.guard);
    let mut builder = ItemListBuilder::new(alloc, 0.5);
    let mut pair_occurrences: usize = 0;
    let mut n_tx: u64 = 0;
    // Binary copy of the filtered DB for the EUCS partition scans (disk, not RAM).
    let seed = cfg.topk_seed && top_k.is_some();
    let pairs_needed = cfg.use_eucs || cfg.cooccur_filter || seed;
    let mut spool = pairs_needed.then(|| super::tx_spool::TxSpool::new(Arc::clone(&ctx.store)));
    let mut spool_row: Vec<(ItemId, Utility)> = Vec::new();

    let db_reader2 = DbReader::new(BufReader::new(File::open(&dataset_path)?));
    for tx in db_reader2.filter_map(Result::ok).take_while(|t| t.tid < max_tid) {
        let Some(mut ftx) = twu.apply(&tx) else { continue };
        if mmu.is_some() || cfg.order_by_item {
            ftx.items.sort_by_key(|e| order_key(e.item));
        }
        let n = ftx.items.len();
        pair_occurrences += n * n.saturating_sub(1) / 2;
        if let Some(sp) = spool.as_mut() {
            spool_row.clear();
            spool_row.extend(ftx.items.iter().map(|e| (e.item, e.utility)));
            sp.push(&spool_row, ftx.transaction_utility)?;
        }
        n_tx = n_tx.max(ftx.tid as u64 + 1);
        if cfg.average {
            let mu: Utility = ftx.items.iter().map(|e| e.utility).max().unwrap_or(0);
            for entry in &ftx.items {
                builder.push(entry.item, ULEntry { tid: ftx.tid, iutils: entry.utility, rutils: mu })?;
            }
        } else {
            let mut remaining: Utility = ftx.items.iter().map(|e| e.utility).sum();
            for entry in &ftx.items {
                remaining -= entry.utility;
                builder.push(entry.item, ULEntry { tid: ftx.tid, iutils: entry.utility, rutils: remaining })?;
            }
        }
    }

    if let Some(sp) = spool.as_mut() { sp.seal()?; }
    drop(spool_row);

    ctx.progress.set_stage("Pass 2: Finalizing 1-itemset lists");
    let mut lists = builder.finish()?;
    crate::mining::core::memory_guard::release_free_memory();

    // EUCS: built after the lists so it can use the memory the builder released, and in
    // as many partitions (extra DB scans) as needed to stay within budget. Pairs below
    // the threshold are dropped per partition, so the final structure is compact.
    // REPT pre-evaluation: start Top-K at the K-th best exact 1-/2-itemset utility.
    if seed {
        let state = top_k.as_ref().unwrap();
        ctx.progress.set_stage("Pre-evaluation: exact utilities of 1- and 2-itemsets");
        let border = super::pair_util::kth_best_small_itemset_utility(
            spool.as_ref().unwrap(), lists.iter().map(|(_, ul, _)| ul.sum_iutils), state.k,
            &ctx.guard, ctx.guard.native_remaining() / 2, pair_occurrences)?;
        state.threshold.fetch_max(border, Ordering::Relaxed);
    }
    let eucs = if cfg.use_eucs || cfg.cooccur_filter {
        let cap = ctx.guard.native_remaining() * 6 / 10;
        // The co-occurrence filter keeps every pair that occurs at all (threshold 1).
        let keep = if cfg.use_eucs { initial_threshold } else { 1 };
        let e = Eucs::build_partitioned(
            |f| spool.as_ref().unwrap().scan(|items, _, tu| f(items, tu)),
            &ctx.guard, cap, keep, pair_occurrences,
            |p, n| ctx.progress.set_stage(&format!("Pass 3: pair structure partition {}/{}", p + 1, n)),
        )?;
        if !e.is_enabled() {
            ctx.progress.set_stage("EUCS does not fit the budget (disabled, mining stays exact)");
        }
        drop(spool.take());
        crate::mining::core::memory_guard::release_free_memory();
        Some(e)
    } else {
        None
    };
    // NB: items with iutil + rutil < threshold must stay in `lists`: that bound only
    // limits itemsets that *start* with the item, and it can still extend earlier items.
    // The per-task `can_prune` check below applies the bound where it is valid.
    // Processing order: ascending TWU (FHM ordering), or ascending mu for HUIM-MMU (sorted
    // downward closure); ties by TWU then item. Must match the order used for rutils above.
    lists.sort_by_key(|(item, _, _)| order_key(*item));
    crate::mining::core::memory_guard::release_free_memory();
    let _lists_hdr = ctx.guard.reserve_force(vec_bytes::<(ItemId, UtilityList, UlBody)>(lists.capacity()));

    ctx.progress.set_stage(&format!(
        "DFS (Budget: {:.0}MB)", ctx.guard.budget() as f64 / 1024.0 / 1024.0
    ));

    let enabled = eucs.as_ref().filter(|e| e.is_enabled());
    let search = Search {
        cfg,
        ctx,
        eucs: if cfg.use_eucs { enabled } else { None },
        cooccur: if cfg.cooccur_filter && !cfg.use_eucs { enabled } else { None },
        top_k: top_k.as_ref(),
        n_tx,
        mmu: mmu.as_ref(),
        min_tid: cfg.min_tid,
        remat: ctx.remat,
        stats: CostStats::default(),
        write_weight: std::env::var("AIR_HUIM_WRITE_WEIGHT").ok().and_then(|v| v.parse().ok()).unwrap_or(1.0),
    };
    // PU-prune partition sums of the shared 1-itemset lists.
    let lists_pu: Vec<Option<Box<[Utility]>>> = if cfg.pu_partitions > 0 {
        let a = BodyAlloc::new(&ctx.pool, &ctx.guard);
        lists.iter().map(|(_, _, b)| search.partition_sums(a, b)).collect::<io::Result<_>>()?
    } else {
        Vec::new()
    };
    let _pu_res = ctx.guard.reserve_force(lists_pu.len() * cfg.pu_partitions * 8);
    let pu_of = |k: usize| -> Option<Box<[Utility]>> { lists_pu.get(k).cloned().flatten() };
    let lists_ref = &lists;
    let task_indices: Vec<usize> = (0..lists.len()).collect();

    ctx.execute_tasks(task_indices, |i, writer| {
        let (item_x, ul_x, body_x) = &lists_ref[i];
        let item_x = *item_x;
        ctx.progress.set_active_prefix(&[item_x]);

        let mut thresh = search.threshold(item_x);
        if search.prune(ul_x, thresh) {
            return;
        }
        if search.is_hui(ul_x, thresh) && search.length_ok(1) {
            thresh = search.emit(&[item_x], ul_x.sum_iutils, thresh, writer).unwrap();
        }
        if !search.may_extend(1) {
            return;
        }

        // Per-task arena: small spilled lists from this subtree share pages.
        let arena = SpillArena::new(&ctx.pool, &ctx.guard);
        let alloc = BodyAlloc::new(&ctx.pool, &ctx.guard).with_arena(&arena).with_stats(&search.stats);
        if search.untouched(alloc, body_x).unwrap() {
            return;
        }
        let pu_x = pu_of(i);
        let px_src = ListSrc::Body(body_x);
        let mut extensions: Vec<Ext> = Vec::new();
        for (j, (item_y, ul_y, body_y)) in lists_ref.iter().enumerate().skip(i + 1) {
            if search.skip_pair(item_x, *item_y, &pu_x, &pu_of(j), thresh) {
                continue;
            }
            let itemset: SmallVec<[ItemId; 8]> = [item_x, *item_y].iter().copied().collect();
            let uses = extensions.len() as u32;
            if let Some(e) = search.join(alloc, itemset, None, &px_src, &ListSrc::Body(body_y), ul_x, ul_y,
                                         thresh, uses, j as u32).unwrap() {
                extensions.push(e);
            }
        }

        if let Some(q) = &prefetch_queue {
            let traversal = TraversalContext {
                prefix: SmallVec::from_slice(&[item_x]),
                depth: 1,
                candidates: extensions.iter().map(|e| CandidateExtension {
                    item: *e.ul.itemset.last().unwrap(),
                    ul_page_id: e.ul.page_id,
                    twu: e.ul.twu(),
                    sum_iutils: e.ul.sum_iutils,
                    load_cost_ns: 0,
                }).collect(),
                min_utility: thresh,
            };
            q.submit_predictions(predictor.predict(&traversal.to_prefetch_state()));
        }

        if !extensions.is_empty() {
            let top = Parent { prefix: None, px: body_x, sibs: Sibs::Lists(lists_ref), up: None };
            search.search(alloc, Some(body_x), Some(&top), extensions, writer).unwrap();
        }
    });

    ctx.progress.set_active_prefix(&[]);

    // Recompute-vs-spill report.
    {
        use std::sync::atomic::Ordering::Relaxed;
        let st = &search.stats;
        let (dropped, saved, recomputed) = (st.dropped.load(Relaxed), st.dropped_bytes.load(Relaxed), st.recomputed_entries.load(Relaxed));
        ctx.progress.remat_dropped.fetch_add(dropped, Relaxed);
        ctx.progress.remat_bytes_saved.fetch_add(saved, Relaxed);
        ctx.progress.remat_recomputed.fetch_add(recomputed, Relaxed);
        if dropped > 0 || st.write_bytes.load(Relaxed) > 0 {
            eprintln!("[remat] mode={:?} dropped {} lists ({:.1} MB not written), recomputed {} entries; \
                       spilled {:.1} MB; measured: join {:.1} ns/entry, write {:.2} ns/B, read {:.2} ns/B",
                      search.remat, dropped, saved as f64 / 1048576.0, recomputed,
                      st.write_bytes.load(Relaxed) as f64 / 1048576.0,
                      st.cpu_ns_per_entry(), st.write_ns_per_byte(), st.read_ns_per_byte());
        }
    }

    // Top-K mode: the heap holds the answer; write it out (highest utility first).
    if let Some(state) = &top_k {
        let mut heap = state.heap.lock().unwrap();
        let mut results = Vec::with_capacity(heap.len());
        while let Some(Reverse((util, itemset))) = heap.pop() {
            results.push((itemset, util));
        }
        results.reverse();
        let mut w = ResultWriter::new(&ctx.output_path)?;
        for (itemset, util) in &results {
            w.write_hui(itemset, *util)?;
        }
        w.finalize()?;
        ctx.progress.huis_found.store(results.len() as u64, Ordering::Relaxed);
    }

    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}
