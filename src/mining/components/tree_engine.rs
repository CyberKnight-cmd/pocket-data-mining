//! Shared, budget-aware machinery for the tree-based two-phase miners
//! (IHUP, HUP-Tree, HUI-Trie, UP-Growth, UP-Growth+).
//!
//! * `NodeArena<T>`: fixed-size node pages, kept in RAM while the budget allows and
//!   stored as buffer-pool pages otherwise (previously *every* node access went through
//!   a pool pin + global mutex, so tree construction crawled and never used the budget).
//! * Conditional pattern bases are never materialised: node-links are walked twice
//!   (once for local TWU, once to build the conditional tree).
//! * Candidates (phase 1 output) are appended to an on-disk `ItemsetSpool` instead of an
//!   unbounded Vec, and verified (phase 2) in budget-sized parallel batches against a
//!   binary copy of the filtered database, using a rarest-item index.

use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, atomic::Ordering};
use crate::buffer_pool::pool::{BufferPool, OwnedPage};
use crate::mining::core::{context::MiningContext, memory_guard::{MemoryGuard, Reservation, map_bytes}};
use crate::storage::{chunk_store::ChunkStore, page_layout::PageFlags};
use crate::types::{ItemId, PageId, Utility};
use super::tx_spool::TxSpool;

pub const NULL: u32 = u32::MAX;
/// Node page size: 1/64 of the budget, 4 KB to 64 KB (small budgets get small pages, so a
/// pinned page is a small share of the budget).
fn arena_page_bytes(guard: &MemoryGuard) -> usize {
    (guard.budget() / 64).clamp(4 * 1024, 64 * 1024)
}

enum ArenaPage {
    Mem(Box<[u8]>, Reservation),
    Pool(OwnedPage),
}

/// Paged node storage addressed by u32 node pointers.
pub struct NodeArena<T: Copy> {
    pool: Arc<BufferPool>,
    guard: Arc<MemoryGuard>,
    pages: Vec<ArenaPage>,
    per_page: usize,
    page_bytes: usize,
    pub next: u32,
    _t: PhantomData<T>,
}

impl<T: Copy> NodeArena<T> {
    pub fn new(pool: &Arc<BufferPool>, guard: &Arc<MemoryGuard>) -> Self {
        Self::with_min_slots(pool, guard, 1)
    }

    /// Pages hold at least `min_slots` nodes (for `alloc_run` of up to that many).
    pub fn with_min_slots(pool: &Arc<BufferPool>, guard: &Arc<MemoryGuard>, min_slots: usize) -> Self {
        let sz = std::mem::size_of::<T>();
        let per_page = (arena_page_bytes(guard) / sz).max(min_slots);
        Self {
            pool: Arc::clone(pool),
            guard: Arc::clone(guard),
            pages: Vec::new(),
            per_page,
            page_bytes: per_page * sz,
            next: 0,
            _t: PhantomData,
        }
    }

    pub fn alloc(&mut self, node: T) -> io::Result<u32> {
        let ptr = self.next;
        while (ptr as usize) / self.per_page >= self.pages.len() {
            let page = match self.guard.reserve(self.page_bytes + 16) {
                Some(r) => ArenaPage::Mem(vec![0u8; self.page_bytes].into_boxed_slice(), r),
                None => ArenaPage::Pool(OwnedPage::create(&self.pool, vec![0u8; self.page_bytes])?),
            };
            self.pages.push(page);
        }
        self.next += 1;
        self.set(ptr, node)?;
        Ok(ptr)
    }

    /// Allocate `len` consecutive slots on one page (used for per-node utility vectors).
    /// Returns the first slot. `len` must not exceed one page.
    pub fn alloc_run(&mut self, len: usize, fill: impl Fn(usize) -> T) -> io::Result<u32> {
        if len > self.per_page {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("run of {} slots exceeds an arena page ({})", len, self.per_page)));
        }
        let off = self.next as usize % self.per_page;
        if off + len > self.per_page {
            self.next += (self.per_page - off) as u32; // skip to the next page
        }
        let start = self.next;
        for k in 0..len {
            self.alloc(fill(k))?;
        }
        Ok(start)
    }

    #[inline]
    fn loc(&self, ptr: u32) -> (usize, usize) {
        let p = ptr as usize;
        (p / self.per_page, (p % self.per_page) * std::mem::size_of::<T>())
    }

    #[inline]
    pub fn get(&self, ptr: u32) -> io::Result<T> {
        let (pg, off) = self.loc(ptr);
        match &self.pages[pg] {
            // SAFETY: off + size_of::<T>() <= page_bytes; read_unaligned handles packing.
            ArenaPage::Mem(b, _) => Ok(unsafe { std::ptr::read_unaligned(b.as_ptr().add(off) as *const T) }),
            ArenaPage::Pool(p) => {
                let pin = p.pin()?;
                Ok(unsafe { std::ptr::read_unaligned(pin.as_ptr().add(off) as *const T) })
            }
        }
    }

    #[inline]
    pub fn set(&mut self, ptr: u32, node: T) -> io::Result<()> {
        let (pg, off) = self.loc(ptr);
        match &mut self.pages[pg] {
            ArenaPage::Mem(b, _) => unsafe { std::ptr::write_unaligned(b.as_mut_ptr().add(off) as *mut T, node) },
            ArenaPage::Pool(p) => {
                let mut pin = self.pool.pin_mut(p.id)?;
                unsafe { std::ptr::write_unaligned(pin.as_mut_ptr().add(off) as *mut T, node) }
            }
        }
        Ok(())
    }

    pub fn resident_pages(&self) -> usize {
        self.pages.iter().filter(|p| matches!(p, ArenaPage::Mem(..))).count()
    }
}

/// Append-only on-disk list of itemsets (phase-1 candidates).
pub struct ItemsetSpool {
    store: Arc<dyn ChunkStore + Send + Sync>,
    guard: Arc<MemoryGuard>,
    pages: Vec<(PageId, u32)>, // (page, itemsets in page)
    buf: Vec<u8>,
    buf_count: u32,
    segment: usize,
    _buf_res: Reservation,
    pub count: u64,
    pub item_total: u64,
}

impl ItemsetSpool {
    pub fn new(store: Arc<dyn ChunkStore + Send + Sync>, guard: &Arc<MemoryGuard>) -> Self {
        let segment = super::tx_spool::spool_segment_bytes(guard);
        let res = guard.reserve_force(segment);
        Self { store, guard: Arc::clone(guard), pages: Vec::new(), buf: Vec::with_capacity(segment), buf_count: 0, segment,
               _buf_res: res, count: 0, item_total: 0 }
    }

    pub fn push(&mut self, items: &[ItemId]) -> io::Result<()> {
        self.push_est(items, Utility::MAX)
    }

    /// Append a candidate with an over-estimate of its utility (used to skip candidates
    /// that can no longer qualify, e.g. TKU's SE strategy).
    pub fn push_est(&mut self, items: &[ItemId], est: Utility) -> io::Result<()> {
        let rec = 12 + 4 * items.len();
        if !self.buf.is_empty() && self.buf.len() + rec > self.segment { self.flush()?; }
        self.buf.reserve(rec);
        if self.buf.capacity() > self._buf_res.bytes() { self._buf_res.resize_force(self.buf.capacity()); }
        self.buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&est.to_le_bytes());
        for i in items { self.buf.extend_from_slice(&i.to_le_bytes()); }
        self.buf_count += 1;
        self.count += 1;
        self.item_total += items.len() as u64;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() { return Ok(()); }
        let id = self.store.next_page_id();
        self.store.write_page(id, &self.buf, PageFlags::empty())?;
        self.pages.push((id, self.buf_count));
        self.buf.clear();
        self.buf_count = 0;
        Ok(())
    }

    /// Visit itemsets with index in [from, to).
    fn scan_range(&self, from: u64, to: u64, mut f: impl FnMut(&[ItemId], Utility)) -> io::Result<()> {
        let mut page_res = self.guard.reserve_force(self.segment);
        let mut idx = 0u64;
        let mut page = Vec::new();
        let mut items = Vec::new();
        for &(id, n) in &self.pages {
            if idx + n as u64 <= from { idx += n as u64; continue; }
            if idx >= to { break; }
            self.store.read_page(id, &mut page)?;
            if page.capacity() > page_res.bytes() { page_res.resize_force(page.capacity()); }
            let mut pos = 0;
            for _ in 0..n {
                let len = u32::from_le_bytes(page[pos..pos + 4].try_into().unwrap()) as usize;
                let est = i64::from_le_bytes(page[pos + 4..pos + 12].try_into().unwrap());
                pos += 12;
                if idx >= from && idx < to {
                    items.clear();
                    for k in 0..len {
                        items.push(u32::from_le_bytes(page[pos + 4 * k..pos + 4 * k + 4].try_into().unwrap()));
                    }
                    f(&items, est);
                }
                pos += 4 * len;
                idx += 1;
            }
        }
        Ok(())
    }
}

impl Drop for ItemsetSpool {
    fn drop(&mut self) {
        for &(id, _) in &self.pages { let _ = self.store.delete_page(id); }
    }
}

/// Phase 2: exact utilities of all spooled candidates; writes those >= min_utility.
pub fn verify_candidates(
    cands: &mut ItemsetSpool,
    db: &TxSpool,
    twu: &HashMap<ItemId, Utility>,
    ctx: &MiningContext,
) -> io::Result<u64> {
    let min = ctx.min_utility;
    verify_with(cands, db, twu, ctx, &|| min, &|items, u, w| {
        if u >= min {
            w.write_hui(items, u)?;
            ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    })?;
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}

/// Exact utilities of spooled candidates, in batches sized from the free budget (one DB scan
/// per batch, batches in parallel), each candidate indexed by its rarest item (`twu`).
/// Candidates whose estimate is below `border()` when their batch is loaded are skipped.
pub fn verify_with(
    cands: &mut ItemsetSpool,
    db: &TxSpool,
    twu: &HashMap<ItemId, Utility>,
    ctx: &MiningContext,
    border: &(dyn Fn() -> Utility + Sync),
    on_result: &(dyn Fn(&[ItemId], Utility, &mut crate::mining::core::context::WriterProxy) -> io::Result<()> + Sync),
) -> io::Result<()> {
    cands.flush()?;
    let n = cands.count;
    if n == 0 {
        ctx.execute_tasks(Vec::<()>::new(), |_, _| {});
        return Ok(());
    }
    // ~bytes per candidate in a batch: items + offset + utility + index slot.
    let avg_len = (cands.item_total / n).max(1) as usize;
    let per_cand = 4 * avg_len + 8 + 8 + 4 + 16;
    let threads = ctx.threads.max(1) as u64;
    let share = (ctx.guard.native_remaining() / (2 * threads as usize)).max(min_batch_share(&ctx.guard));
    let batch = ((share / per_cand) as u64).max(64).min(n.div_ceil(threads).max(1));
    let ranges: Vec<(u64, u64)> = (0..n).step_by(batch as usize).map(|s| (s, (s + batch).min(n))).collect();
    ctx.progress.set_stage(&format!("Phase 2: verifying {} candidates in {} batches", n, ranges.len()));

    let cands_ref = &*cands;
    ctx.execute_tasks(ranges, |(from, to), w| {
        let mut run = || -> io::Result<()> {
            let mut res = ctx.guard.reserve_force(((to - from) as usize) * per_cand);
            let mut flat: Vec<ItemId> = Vec::new();
            let mut offs: Vec<u32> = vec![0];
            let b = border();
            cands_ref.scan_range(from, to, |items, est| {
                if est < b { return; }
                flat.extend_from_slice(items);
                offs.push(flat.len() as u32);
            })?;
            let m = offs.len() - 1;
            let mut index: HashMap<ItemId, Vec<u32>> = HashMap::new();
            for c in 0..m {
                let items = &flat[offs[c] as usize..offs[c + 1] as usize];
                let key = *items.iter().min_by_key(|i| (twu.get(i).copied().unwrap_or(0), **i)).unwrap();
                index.entry(key).or_default().push(c as u32);
            }
            res.resize_force(res.bytes() + map_bytes::<ItemId, Vec<u32>>(index.capacity()));
            let mut utils: Vec<Utility> = vec![0; m];
            db.scan(|items, us, _| {
                for &it in items {
                    let Some(list) = index.get(&it) else { continue };
                    'cand: for &c in list {
                        let cand = &flat[offs[c as usize] as usize..offs[c as usize + 1] as usize];
                        let mut u = 0;
                        for x in cand {
                            match items.iter().position(|y| y == x) {
                                Some(p) => u += us[p],
                                None => continue 'cand,
                            }
                        }
                        utils[c as usize] += u;
                    }
                }
                true
            })?;
            for c in 0..m {
                on_result(&flat[offs[c] as usize..offs[c + 1] as usize], utils[c], w)?;
            }
            Ok(())
        };
        if let Err(e) = run() { eprintln!("phase 2 batch failed: {}", e); }
    });
    Ok(())
}

/// Admission control for the tree miners (see `MiningContext::admit`). What cannot be spilled:
/// the TWU map of all items; per promising item, the header table, root index, the transient
/// rank/local maps of one conditional level, miu and the item order; the DB and candidate
/// spool write buffers, two pinned node pages and the output queue; per phase-2 worker, the
/// smallest candidate batch with its index and the spool read buffers. Node pages,
/// conditional trees and candidates spill.
fn admit_tree(ctx: &mut MiningContext, name: &str, twu: &HashMap<ItemId, Utility>, min: Utility,
              extra_fixed: &dyn Fn(usize) -> usize) -> io::Result<()> {
    // The TWU map covers every item; the other maps only the promising ones.
    let n_all = twu.len().max(1);
    let n = twu.values().filter(|&&t| t >= min).count().max(1);
    let multi = ctx.threads > 1;
    let estimate = |b: usize| -> (usize, usize) {
        let seg = (b / 32).clamp(1024, 1 << 20);
        let queue = if multi { (b / 64).clamp(64 * 96, 100_000 * 96) } else { 0 };
        let page = (b / 64).clamp(4 * 1024, 64 * 1024); // arena_page_bytes
        let fixed = map_bytes::<ItemId, Utility>(n_all) + 4 * map_bytes::<ItemId, Utility>(n) + n * 8
            + 2 * seg + 2 * (page + 16) + queue + extra_fixed(b);
        (fixed, (b / 16).clamp(8 * 1024, 128 * 1024) + 2 * seg)
    };
    ctx.admit(name, &estimate).map(|_| ())
}

/// Smallest per-thread share of the free budget for a phase-2 candidate batch.
fn min_batch_share(guard: &MemoryGuard) -> usize {
    (guard.budget() / 16).clamp(8 * 1024, 128 * 1024)
}

/// Phase 0 shared by the tree miners: TWU of every item over the raw database.
pub fn item_twus(path: &std::path::Path) -> io::Result<HashMap<ItemId, Utility>> {
    use crate::preprocessing::db_reader::DbReader;
    let mut twu = HashMap::new();
    for tx in DbReader::new(io::BufReader::new(std::fs::File::open(path)?)) {
        let tx = tx?;
        for e in &tx.items {
            *twu.entry(e.item).or_insert(0) += tx.transaction_utility;
        }
    }
    Ok(twu)
}

// ---------------------------------------------------------------------------------------
// IHUP / HUP-Tree / HUI-Trie: prefix tree of transaction TWUs.
// ---------------------------------------------------------------------------------------

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct TwuNode {
    item: ItemId,
    twu: Utility,
    parent: u32,
    first_child: u32,
    next_sibling: u32,
    node_link: u32,
}

struct TwuTree {
    root: u32,
    header: HashMap<ItemId, u32>,
    order: Vec<ItemId>,
    /// Index of the root's children: the root can have one child per item, and a linear
    /// sibling scan there dominated tree construction on sparse data.
    root_children: HashMap<ItemId, u32>,
    _res: Reservation,
}

impl TwuTree {
    fn new(arena: &mut NodeArena<TwuNode>, order: Vec<ItemId>, guard: &Arc<MemoryGuard>) -> io::Result<Self> {
        let header: HashMap<ItemId, u32> = order.iter().map(|&i| (i, NULL)).collect();
        let res = guard.reserve_force(map_bytes::<ItemId, u32>(header.capacity()) + order.len() * 4);
        let root = arena.alloc(TwuNode { item: 0, twu: 0, parent: NULL, first_child: NULL, next_sibling: NULL, node_link: NULL })?;
        Ok(Self { root, header, order, root_children: HashMap::new(), _res: res })
    }

    fn insert(&mut self, arena: &mut NodeArena<TwuNode>, path: &[ItemId], twu: Utility) -> io::Result<()> {
        let mut cur = self.root;
        for &item in path {
            let mut found = NULL;
            let mut child = if cur == self.root {
                match self.root_children.get(&item) {
                    Some(&c) => {
                        let mut n = arena.get(c)?;
                        n.twu += twu;
                        arena.set(c, n)?;
                        found = c;
                    }
                    None => {}
                }
                NULL // either found via the index, or known absent
            } else {
                arena.get(cur)?.first_child
            };
            while child != NULL {
                let mut c = arena.get(child)?;
                if c.item == item {
                    c.twu += twu;
                    arena.set(child, c)?;
                    found = child;
                    break;
                }
                child = c.next_sibling;
            }
            if found == NULL {
                let parent = arena.get(cur)?;
                let link = self.header[&item];
                found = arena.alloc(TwuNode { item, twu, parent: cur, first_child: NULL, next_sibling: parent.first_child, node_link: link })?;
                let mut p = parent;
                p.first_child = found;
                arena.set(cur, p)?;
                self.header.insert(item, found);
                if cur == self.root {
                    let before = self.root_children.capacity();
                    self.root_children.insert(item, found);
                    if self.root_children.capacity() != before {
                        self._res.resize_force(map_bytes::<ItemId, u32>(self.header.capacity()) + self.order.len() * 4
                            + map_bytes::<ItemId, u32>(self.root_children.capacity()));
                    }
                }
            }
            cur = found;
        }
        Ok(())
    }
}

fn mine_twu_tree(
    tree: &TwuTree,
    arena: &mut NodeArena<TwuNode>,
    prefix: &mut Vec<ItemId>,
    out: &mut ItemsetSpool,
    ctx: &MiningContext,
) -> io::Result<()> {
    let min = ctx.min_utility;
    ctx.progress.set_active_prefix(prefix);
    ctx.progress.current_depth.store(prefix.len(), Ordering::Relaxed);
    let mut path: Vec<ItemId> = Vec::new();
    for &item in tree.order.iter().rev() {
        // Pass A: sum of TWU and local TWU of the items on the prefix paths.
        let mut sum = 0;
        let mut local: HashMap<ItemId, Utility> = HashMap::new();
        let mut cur = tree.header.get(&item).copied().unwrap_or(NULL);
        while cur != NULL {
            let node = arena.get(cur)?;
            sum += node.twu;
            let mut p = node.parent;
            while p != tree.root && p != NULL {
                let pn = arena.get(p)?;
                *local.entry(pn.item).or_insert(0) += node.twu;
                p = pn.parent;
            }
            cur = node.node_link;
        }
        if sum < min { continue; }
        prefix.push(item);
        out.push(prefix)?;

        let _res = ctx.guard.reserve_force(map_bytes::<ItemId, Utility>(local.capacity()));
        let mut local_items: Vec<ItemId> = local.iter().filter(|&(_, &t)| t >= min).map(|(&i, _)| i).collect();
        if local_items.is_empty() { prefix.pop(); continue; }
        let rank: HashMap<ItemId, usize> = tree.order.iter().enumerate().map(|(k, &i)| (i, k)).collect();
        local_items.sort_by_key(|i| rank[i]);
        drop(rank);

        // Pass B: build the conditional tree straight from the node-links.
        let saved = arena.next;
        let mut cond = TwuTree::new(arena, local_items, &ctx.guard)?;
        let mut cur = tree.header.get(&item).copied().unwrap_or(NULL);
        while cur != NULL {
            let node = arena.get(cur)?;
            path.clear();
            let mut p = node.parent;
            while p != tree.root && p != NULL {
                let pn = arena.get(p)?;
                let it = pn.item;
                if local.get(&it).copied().unwrap_or(0) >= min { path.push(it); }
                p = pn.parent;
            }
            path.reverse();
            if !path.is_empty() {
                cond.insert(arena, &path, node.twu)?;
            }
            cur = node.node_link;
        }
        drop(local);
        mine_twu_tree(&cond, arena, prefix, out, ctx)?;
        drop(cond);
        arena.next = saved;
        prefix.pop();
    }
    Ok(())
}

/// Phase-2 verifier for the two-phase TWU-tree miners.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Verifier {
    /// Candidates indexed by their rarest item (IHUP).
    RarestItemIndex,
    /// Candidates stored in a prefix trie, counted by walking the trie per transaction (HUI-Trie).
    Trie,
}

/// IHUP (rarest-item index verification) / HUI-Trie (trie verification).
pub fn run_twu_tree_miner(name: &str, path: &std::path::Path, ctx: &mut MiningContext) -> io::Result<u64> {
    run_twu_tree_miner_with(name, path, ctx, Verifier::RarestItemIndex)
}

pub fn run_twu_tree_miner_with(name: &str, path: &std::path::Path, ctx: &mut MiningContext, verifier: Verifier) -> io::Result<u64> {
    use crate::preprocessing::db_reader::DbReader;
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;
    ctx.progress.set_stage("Phase 1: Computing 1-itemset TWUs");
    let twu = item_twus(path)?;
    let mut valid: Vec<ItemId> = twu.iter().filter(|&(_, &t)| t >= min).map(|(&i, _)| i).collect();
    valid.sort_by(|a, b| twu[b].cmp(&twu[a]).then_with(|| a.cmp(b)));
    admit_tree(ctx, name, &twu, min, &|_| 0)?;

    ctx.progress.set_stage(&format!("Phase 1: Building {} tree", name));
    let mut arena: NodeArena<TwuNode> = NodeArena::new(&ctx.pool, &ctx.guard);
    let mut tree = TwuTree::new(&mut arena, valid, &ctx.guard)?;
    let mut db = TxSpool::new(Arc::clone(&ctx.store), &ctx.guard);
    let mut row: Vec<(ItemId, Utility)> = Vec::new();
    let mut items: Vec<ItemId> = Vec::new();
    for tx in DbReader::new(io::BufReader::new(std::fs::File::open(path)?)) {
        let tx = tx?;
        row.clear();
        row.extend(tx.items.iter().filter(|e| twu.get(&e.item).copied().unwrap_or(0) >= min).map(|e| (e.item, e.utility)));
        if row.is_empty() { continue; }
        db.push(&row, tx.transaction_utility)?;
        items.clear();
        items.extend(row.iter().map(|r| r.0));
        items.sort_by(|a, b| twu[b].cmp(&twu[a]).then_with(|| a.cmp(b)));
        tree.insert(&mut arena, &items, tx.transaction_utility)?;
    }
    db.seal()?;

    ctx.progress.set_stage(&format!("Phase 1: Mining {} tree ({} node pages in RAM)", name, arena.resident_pages()));
    let mut cands = ItemsetSpool::new(Arc::clone(&ctx.store), &ctx.guard);
    mine_twu_tree(&tree, &mut arena, &mut Vec::new(), &mut cands, ctx)?;
    drop(tree);
    drop(arena);
    crate::mining::core::memory_guard::release_free_memory();

    match verifier {
        Verifier::RarestItemIndex => verify_candidates(&mut cands, &db, &twu, ctx),
        Verifier::Trie => verify_trie(&mut cands, &db, ctx),
    }
}

/// Phase 2 with a candidate trie: candidates (sorted itemsets) are inserted into a prefix trie;
/// each transaction (items sorted) is matched against it by a depth-first walk that follows only
/// children present in the transaction, accumulating utilities. Batches sized from the budget.
pub fn verify_trie(cands: &mut ItemsetSpool, db: &TxSpool, ctx: &MiningContext) -> io::Result<u64> {
    #[derive(Clone)]
    struct TNode { children: Vec<(ItemId, u32)>, cand: u32 }
    const NONE: u32 = u32::MAX;
    cands.flush()?;
    let n = cands.count;
    let min = ctx.min_utility;
    if n == 0 {
        ctx.execute_tasks(Vec::<()>::new(), |_, _| {});
        return Ok(0);
    }
    let avg_len = (cands.item_total / n).max(1) as usize;
    let per_cand = avg_len * (std::mem::size_of::<TNode>() + 8) + 16;
    let threads = ctx.threads.max(1) as u64;
    let share = (ctx.guard.native_remaining() / (2 * threads as usize)).max(min_batch_share(&ctx.guard));
    let batch = ((share / per_cand) as u64).max(64).min(n.div_ceil(threads).max(1));
    let ranges: Vec<(u64, u64)> = (0..n).step_by(batch as usize).map(|s| (s, (s + batch).min(n))).collect();
    ctx.progress.set_stage(&format!("Phase 2 (trie): verifying {} candidates in {} batches", n, ranges.len()));
    let cands_ref = &*cands;
    ctx.execute_tasks(ranges, |(from, to), w| {
        let mut run = || -> io::Result<()> {
            let mut res = ctx.guard.reserve_force(0);
            let mut trie: Vec<TNode> = vec![TNode { children: Vec::new(), cand: NONE }];
            let mut sets: Vec<Vec<ItemId>> = Vec::new();
            cands_ref.scan_range(from, to, |items, _| {
                let mut sorted = items.to_vec();
                sorted.sort_unstable();
                let mut cur = 0usize;
                for &it in &sorted {
                    let next = match trie[cur].children.binary_search_by_key(&it, |c| c.0) {
                        Ok(k) => trie[cur].children[k].1 as usize,
                        Err(k) => {
                            trie.push(TNode { children: Vec::new(), cand: NONE });
                            let id = (trie.len() - 1) as u32;
                            trie[cur].children.insert(k, (it, id));
                            id as usize
                        }
                    };
                    cur = next;
                }
                trie[cur].cand = sets.len() as u32;
                sets.push(sorted);
            })?;
            res.resize_force(trie.len() * (std::mem::size_of::<TNode>() + 8) + sets.iter().map(|s| s.len() * 4 + 24).sum::<usize>());
            let mut utils: Vec<Utility> = vec![0; sets.len()];
            let mut tx: Vec<(ItemId, Utility)> = Vec::new();
            fn walk(trie: &[TNode], node: usize, tx: &[(ItemId, Utility)], start: usize, acc: Utility, utils: &mut [Utility]) {
                for &(it, child) in &trie[node].children {
                    if let Ok(p) = tx[start..].binary_search_by_key(&it, |e| e.0) {
                        let a = acc + tx[start + p].1;
                        let c = &trie[child as usize];
                        if c.cand != u32::MAX { utils[c.cand as usize] += a; }
                        if !c.children.is_empty() { walk(trie, child as usize, tx, start + p + 1, a, utils); }
                    }
                }
            }
            db.scan(|items, us, _| {
                tx.clear();
                tx.extend(items.iter().copied().zip(us.iter().copied()));
                tx.sort_unstable_by_key(|e| e.0);
                walk(&trie, 0, &tx, 0, 0, &mut utils);
                true
            })?;
            for (c, set) in sets.iter().enumerate() {
                if utils[c] >= min {
                    w.write_hui(set, utils[c])?;
                    ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(())
        };
        if let Err(e) = run() { eprintln!("phase 2 (trie) batch failed: {}", e); }
    });
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}

// ---------------------------------------------------------------------------------------
// HUP-Tree / HUP-Growth: one-phase mining on a tree whose nodes store utility vectors.
// ---------------------------------------------------------------------------------------

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct HupNode {
    item: ItemId,
    /// Sum of transaction utilities of the patterns through this node (TWU pruning).
    twu: Utility,
    /// Sum of the suffix ("base") utilities carried by those patterns.
    base: Utility,
    /// First slot of this node's utility vector (one slot per item from the top of the path
    /// down to this node: summed utilities of those items in the patterns through the node).
    vec: u32,
    depth: u32,
    parent: u32,
    first_child: u32,
    next_sibling: u32,
    node_link: u32,
}

struct HupTreeS {
    root: u32,
    header: HashMap<ItemId, u32>,
    /// Item order of this tree (descending TWU); mining goes bottom-up (reverse).
    order: Vec<ItemId>,
    root_children: HashMap<ItemId, u32>,
    _res: Reservation,
}

impl HupTreeS {
    fn new(arena: &mut NodeArena<HupNode>, order: Vec<ItemId>, guard: &Arc<MemoryGuard>) -> io::Result<Self> {
        let header: HashMap<ItemId, u32> = order.iter().map(|&i| (i, NULL)).collect();
        let res = guard.reserve_force(map_bytes::<ItemId, u32>(header.capacity()) + order.len() * 4);
        let root = arena.alloc(HupNode { item: 0, twu: 0, base: 0, vec: NULL, depth: 0, parent: NULL,
                                         first_child: NULL, next_sibling: NULL, node_link: NULL })?;
        Ok(Self { root, header, order, root_children: HashMap::new(), _res: res })
    }

    /// Insert one pattern: `path` items with their utilities, carried suffix utility and TWU.
    fn insert(&mut self, arena: &mut NodeArena<HupNode>, vals: &mut NodeArena<Utility>,
              path: &[ItemId], utils: &[Utility], base: Utility, twu: Utility) -> io::Result<()> {
        let mut cur = self.root;
        for (k, &item) in path.iter().enumerate() {
            let mut found = NULL;
            let mut child = if cur == self.root {
                if let Some(&c) = self.root_children.get(&item) { found = c; }
                NULL
            } else {
                arena.get(cur)?.first_child
            };
            while child != NULL {
                let c = arena.get(child)?;
                if c.item == item { found = child; break; }
                child = c.next_sibling;
            }
            if found != NULL {
                let mut c = arena.get(found)?;
                c.twu += twu;
                c.base += base;
                arena.set(found, c)?;
                let v = c.vec;
                for d in 0..=k {
                    let x = vals.get(v + d as u32)?;
                    vals.set(v + d as u32, x + utils[d])?;
                }
            } else {
                let v = vals.alloc_run(k + 1, |d| utils[d])?;
                let mut parent = arena.get(cur)?;
                let link = self.header[&item];
                found = arena.alloc(HupNode { item, twu, base, vec: v, depth: (k + 1) as u32, parent: cur,
                                              first_child: NULL, next_sibling: parent.first_child, node_link: link })?;
                parent.first_child = found;
                arena.set(cur, parent)?;
                self.header.insert(item, found);
                if cur == self.root {
                    self.root_children.insert(item, found);
                }
            }
            cur = found;
        }
        Ok(())
    }
}

fn mine_hup(tree: &HupTreeS, arena: &mut NodeArena<HupNode>, vals: &mut NodeArena<Utility>,
            prefix: &mut Vec<ItemId>, w: &mut crate::mining::core::result_writer::ResultWriter,
            ctx: &MiningContext) -> io::Result<()> {
    let min = ctx.min_utility;
    ctx.progress.set_active_prefix(prefix);
    ctx.progress.current_depth.store(prefix.len(), Ordering::Relaxed);
    let mut pu: Vec<Utility> = Vec::new();
    let mut pi: Vec<ItemId> = Vec::new();
    for &item in tree.order.iter().rev() {
        // Exact utility and TWU of prefix + item, straight from the nodes' vectors.
        let mut util = 0;
        let mut twu = 0;
        let mut local: HashMap<ItemId, Utility> = HashMap::new();
        let mut cur = tree.header.get(&item).copied().unwrap_or(NULL);
        while cur != NULL {
            let n = arena.get(cur)?;
            let d = n.depth as u32;
            util += vals.get(n.vec + d - 1)? + n.base;
            twu += n.twu;
            let mut p = n.parent;
            while p != tree.root && p != NULL {
                let pn = arena.get(p)?;
                *local.entry(pn.item).or_insert(0) += n.twu;
                p = pn.parent;
            }
            cur = n.node_link;
        }
        if twu < min { continue; }
        prefix.push(item);
        if util >= min {
            let mut out = prefix.clone();
            out.sort_unstable();
            w.write_hui(&out, util)?;
            ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
        }
        let _res = ctx.guard.reserve_force(map_bytes::<ItemId, Utility>(local.capacity()));
        let mut keep: Vec<ItemId> = local.iter().filter(|&(_, &t)| t >= min).map(|(&i, _)| i).collect();
        if !keep.is_empty() {
            let rank: HashMap<ItemId, usize> = tree.order.iter().enumerate().map(|(k, &i)| (i, k)).collect();
            keep.sort_by_key(|i| rank[i]);
            drop(rank);
            let (saved_n, saved_v) = (arena.next, vals.next);
            let mut cond = HupTreeS::new(arena, keep, &ctx.guard)?;
            let mut cur = tree.header.get(&item).copied().unwrap_or(NULL);
            while cur != NULL {
                let n = arena.get(cur)?;
                let d = n.depth as usize;
                let base = vals.get(n.vec + d as u32 - 1)? + n.base;
                // Ancestors top-down with their utilities from this node's vector.
                pi.clear();
                pu.clear();
                let mut chain: Vec<ItemId> = Vec::with_capacity(d);
                let mut p = n.parent;
                while p != tree.root && p != NULL {
                    let pn = arena.get(p)?;
                    chain.push(pn.item);
                    p = pn.parent;
                }
                chain.reverse();
                for (k, &a) in chain.iter().enumerate() {
                    if local.get(&a).copied().unwrap_or(0) >= min {
                        pi.push(a);
                        pu.push(vals.get(n.vec + k as u32)?);
                    }
                }
                if !pi.is_empty() {
                    cond.insert(arena, vals, &pi, &pu, base, n.twu)?;
                }
                cur = n.node_link;
            }
            drop(local);
            mine_hup(&cond, arena, vals, prefix, w, ctx)?;
            drop(cond);
            arena.next = saved_n;
            vals.next = saved_v;
        }
        prefix.pop();
    }
    Ok(())
}

/// HUP-Tree / HUP-Growth (Lin, Hong & Lu, 2011): one-phase exact mining. Utilities of every
/// itemset are read from the per-node utility vectors, so no candidate phase or rescan is needed.
pub fn run_hup_tree(path: &std::path::Path, ctx: &mut MiningContext) -> io::Result<u64> {
    use crate::preprocessing::db_reader::DbReader;
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;
    ctx.progress.set_stage("HUP-Tree: TWU");
    let twu = item_twus(path)?;
    let mut order: Vec<ItemId> = twu.iter().filter(|&(_, &t)| t >= min).map(|(&i, _)| i).collect();
    order.sort_by(|a, b| twu[b].cmp(&twu[a]).then_with(|| a.cmp(b)));
    // One-phase, single-threaded: also the per-node utility vectors being read (one value
    // page; a page holds a whole path, i.e. up to one value per promising item).
    let max_path = order.len().min(8192);
    admit_tree(ctx, "HUP-Tree", &twu, min, &|b| ((b / 64).clamp(4 * 1024, 64 * 1024)).max(max_path * 8) + 16)?;
    ctx.progress.set_stage("HUP-Tree: building tree");
    let mut arena: NodeArena<HupNode> = NodeArena::new(&ctx.pool, &ctx.guard);
    let mut vals: NodeArena<Utility> = NodeArena::with_min_slots(&ctx.pool, &ctx.guard, max_path);
    let mut tree = HupTreeS::new(&mut arena, order, &ctx.guard)?;
    let mut row: Vec<(ItemId, Utility)> = Vec::new();
    let mut items: Vec<ItemId> = Vec::new();
    let mut utils: Vec<Utility> = Vec::new();
    for tx in DbReader::new(io::BufReader::new(std::fs::File::open(path)?)) {
        let tx = tx?;
        row.clear();
        row.extend(tx.items.iter().filter(|e| twu.get(&e.item).copied().unwrap_or(0) >= min).map(|e| (e.item, e.utility)));
        if row.is_empty() { continue; }
        row.sort_by(|a, b| twu[&b.0].cmp(&twu[&a.0]).then_with(|| a.0.cmp(&b.0)));
        items.clear();
        utils.clear();
        for &(i, u) in &row { items.push(i); utils.push(u); }
        tree.insert(&mut arena, &mut vals, &items, &utils, 0, tx.transaction_utility)?;
    }
    ctx.progress.set_stage(&format!("HUP-Tree: mining ({} node pages, {} vector pages in RAM)",
                                    arena.resident_pages(), vals.resident_pages()));
    let mut w = ctx.open_writer()?;
    mine_hup(&tree, &mut arena, &mut vals, &mut Vec::new(), &mut w, ctx)?;
    w.finalize()?;
    Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
}

// ---------------------------------------------------------------------------------------
// UP-Growth / UP-Growth+ / TKU: UP-Tree with node utilities.
// ---------------------------------------------------------------------------------------

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct UpNode {
    item: ItemId,
    nu: Utility,
    count: u32,
    /// Minimal node utility: smallest utility of `item` among the transactions through this node.
    mnu: Utility,
    parent: u32,
    first_child: u32,
    next_sibling: u32,
    node_link: u32,
}

#[derive(Clone, Copy)]
struct Header { twu: Utility, head: u32, tail: u32 }

struct UpTree {
    root: u32,
    header: HashMap<ItemId, Header>,
    /// Index of the root's children (see `TwuTree::root_children`).
    root_children: HashMap<ItemId, u32>,
    _res: Reservation,
}

/// Which UP-Tree variant: the decrements DLU/DLN use global minimum item utilities (miu,
/// UP-Growth) or per-node minimal node utilities (mnu, UP-Growth+).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum UpVariant { Growth, GrowthPlus }

impl UpTree {
    fn new(arena: &mut NodeArena<UpNode>, items: &[(ItemId, Utility)], guard: &Arc<MemoryGuard>) -> io::Result<Self> {
        let header: HashMap<ItemId, Header> = items.iter().map(|&(i, t)| (i, Header { twu: t, head: NULL, tail: NULL })).collect();
        let res = guard.reserve_force(map_bytes::<ItemId, Header>(header.capacity()));
        let root = arena.alloc(UpNode { item: 0, nu: 0, count: 0, mnu: 0, parent: NULL, first_child: NULL, next_sibling: NULL, node_link: NULL })?;
        Ok(Self { root, header, root_children: HashMap::new(), _res: res })
    }

    /// Insert a path of `count` transactions; element k adds `nu_at(k)` to its node's utility and
    /// lowers its minimal node utility to `mnu_at(k)`.
    fn insert(&mut self, arena: &mut NodeArena<UpNode>, path: &[ItemId], count: u32,
              nu_at: impl Fn(usize) -> Utility, mnu_at: impl Fn(usize) -> Utility) -> io::Result<()> {
        let mut cur = self.root;
        for (k, &item) in path.iter().enumerate() {
            let (nu, mnu) = (nu_at(k), mnu_at(k));
            let mut found = NULL;
            let mut child = if cur == self.root {
                if let Some(&c) = self.root_children.get(&item) { found = c; }
                NULL
            } else {
                arena.get(cur)?.first_child
            };
            while child != NULL {
                let c = arena.get(child)?;
                if c.item == item { found = child; break; }
                child = c.next_sibling;
            }
            if found != NULL {
                let mut c = arena.get(found)?;
                c.nu += nu;
                c.count += count;
                let m = c.mnu;
                c.mnu = m.min(mnu);
                arena.set(found, c)?;
            } else {
                let mut parent = arena.get(cur)?;
                found = arena.alloc(UpNode { item, nu, count, mnu, parent: cur, first_child: NULL,
                                             next_sibling: parent.first_child, node_link: NULL })?;
                parent.first_child = found;
                arena.set(cur, parent)?;
                if cur == self.root {
                    let before = self.root_children.capacity();
                    self.root_children.insert(item, found);
                    if self.root_children.capacity() != before {
                        self._res.resize_force(map_bytes::<ItemId, Header>(self.header.capacity())
                            + map_bytes::<ItemId, u32>(self.root_children.capacity()));
                    }
                }
                if let Some(h) = self.header.get_mut(&item) {
                    if h.tail != NULL {
                        let mut t = arena.get(h.tail)?;
                        t.node_link = found;
                        arena.set(h.tail, t)?;
                    } else {
                        h.head = found;
                    }
                    h.tail = found;
                }
            }
            cur = found;
        }
        Ok(())
    }

    /// Items in mining order: ascending TWU, ties by descending item id.
    fn mining_order(&self) -> Vec<ItemId> {
        let mut v: Vec<ItemId> = self.header.keys().copied().collect();
        v.sort_by_key(|&i| (self.header[&i].twu, std::cmp::Reverse(i)));
        v
    }
}

struct UpMiner<'a> {
    variant: UpVariant,
    /// Global minimum item utilities (UP-Growth's DLU/DLN).
    miu: &'a HashMap<ItemId, Utility>,
    ctx: &'a MiningContext,
    min: Utility,
}

impl UpMiner<'_> {
    /// Decrement weight of an item on a path: miu (UP-Growth) or the node's mnu (UP-Growth+).
    fn weight(&self, item: ItemId, mnu: Utility) -> Utility {
        match self.variant { UpVariant::Growth => self.miu.get(&item).copied().unwrap_or(0), UpVariant::GrowthPlus => mnu }
    }

    fn mine(&self, tree: &UpTree, arena: &mut NodeArena<UpNode>, item: ItemId, prefix: &mut Vec<ItemId>,
            out: &mut ItemsetSpool) -> io::Result<()> {
        let min = self.min;
        let ctx = self.ctx;
        ctx.progress.set_active_prefix(prefix);
        ctx.progress.current_depth.store(prefix.len(), Ordering::Relaxed);
        let head = tree.header[&item].head;

        // Estimated utility of prefix + item: sum of node utilities.
        let mut item_util = 0;
        let mut cur = head;
        while cur != NULL {
            let node = arena.get(cur)?;
            item_util += node.nu;
            cur = node.node_link;
        }
        if item_util < min { return Ok(()); }

        prefix.push(item);
        let mut sorted = prefix.clone();
        sorted.sort_unstable();
        out.push_est(&sorted, item_util)?;

        // Pass A: local TWU of the items above `item` (path utility = node utility).
        let mut local: HashMap<ItemId, Utility> = HashMap::new();
        let mut cur = head;
        while cur != NULL {
            let node = arena.get(cur)?;
            let mut p = node.parent;
            while p != tree.root && p != NULL {
                let pn = arena.get(p)?;
                *local.entry(pn.item).or_insert(0) += node.nu;
                p = pn.parent;
            }
            cur = node.node_link;
        }
        let _res = ctx.guard.reserve_force(map_bytes::<ItemId, Utility>(local.capacity()));
        let mut valid: Vec<(ItemId, Utility)> = local.iter().filter(|&(_, &t)| t >= min).map(|(&i, &t)| (i, t)).collect();
        valid.sort_by_key(|&(i, t)| (std::cmp::Reverse(t), i));

        // Pass B: conditional UP-tree. DLU: a path loses the minimum utility of each discarded
        // (unpromising) item; DLN: a node's utility excludes the minimum utilities of the
        // items below it on the reorganised path.
        let saved = arena.next;
        let mut cond = UpTree::new(arena, &valid, &ctx.guard)?;
        let mut path: Vec<(ItemId, Utility)> = Vec::new(); // (item, mnu of its node)
        let mut cur = head;
        while cur != NULL {
            let node = arena.get(cur)?;
            let cnt = node.count as Utility;
            let mut pu = node.nu;
            path.clear();
            let mut p = node.parent;
            while p != tree.root && p != NULL {
                let pn = arena.get(p)?;
                let (it, m) = (pn.item, pn.mnu);
                if local.get(&it).copied().unwrap_or(0) >= min {
                    path.push((it, m));
                } else {
                    pu -= self.weight(it, m) * cnt; // DLU
                }
                p = pn.parent;
            }
            if !path.is_empty() {
                path.sort_by_key(|&(i, _)| (std::cmp::Reverse(local[&i]), i));
                let items: Vec<ItemId> = path.iter().map(|e| e.0).collect();
                // suffix[k] = sum of weights of the elements after k (DLN).
                let mut suffix = vec![0 as Utility; path.len()];
                for k in (0..path.len().saturating_sub(1)).rev() {
                    suffix[k] = suffix[k + 1] + self.weight(path[k + 1].0, path[k + 1].1) * cnt;
                }
                cond.insert(arena, &items, node.count, |k| pu - suffix[k], |k| path[k].1)?;
            }
            cur = node.node_link;
        }
        drop(local);
        for child in cond.mining_order() {
            self.mine(&cond, arena, child, prefix, out)?;
        }
        drop(cond);
        arena.next = saved;
        prefix.pop();
        Ok(())
    }
}

/// Build the global UP-tree over items with TWU >= `min` from a spool of transactions.
fn build_up_tree(db: &TxSpool, twu: &HashMap<ItemId, Utility>, min: Utility, ctx: &MiningContext)
                 -> io::Result<(NodeArena<UpNode>, UpTree, HashMap<ItemId, Utility>)> {
    let mut valid: Vec<(ItemId, Utility)> = twu.iter().filter(|&(_, &t)| t >= min).map(|(&i, &t)| (i, t)).collect();
    valid.sort_by_key(|&(i, t)| (std::cmp::Reverse(t), i));
    let mut arena: NodeArena<UpNode> = NodeArena::new(&ctx.pool, &ctx.guard);
    let mut tree = UpTree::new(&mut arena, &valid, &ctx.guard)?;
    drop(valid);
    let mut miu: HashMap<ItemId, Utility> = HashMap::new();
    let mut row: Vec<(ItemId, Utility)> = Vec::new();
    let mut items: Vec<ItemId> = Vec::new();
    let mut prefix_nu: Vec<Utility> = Vec::new();
    let mut utils: Vec<Utility> = Vec::new();
    let mut err = None;
    db.scan(|its, us, _| {
        row.clear();
        row.extend(its.iter().zip(us).filter(|(i, _)| tree.header.contains_key(i)).map(|(&i, &u)| (i, u)));
        if row.is_empty() { return true; }
        row.sort_by_key(|&(i, _)| (std::cmp::Reverse(tree.header[&i].twu), i));
        items.clear();
        prefix_nu.clear();
        utils.clear();
        let mut acc = 0;
        for &(i, u) in &row {
            acc += u;
            items.push(i);
            prefix_nu.push(acc);
            utils.push(u);
            let m = miu.entry(i).or_insert(u);
            *m = (*m).min(u);
        }
        if let Err(e) = tree.insert(&mut arena, &items, 1, |k| prefix_nu[k], |k| utils[k]) {
            err = Some(e);
            return false;
        }
        true
    })?;
    if let Some(e) = err { return Err(e); }
    Ok((arena, tree, miu))
}

/// Spool every transaction restricted to items with TWU >= `min` (all items when min = 0).
fn spool_db(path: &std::path::Path, twu: &HashMap<ItemId, Utility>, min: Utility, ctx: &MiningContext) -> io::Result<TxSpool> {
    use crate::preprocessing::db_reader::DbReader;
    let mut db = TxSpool::new(Arc::clone(&ctx.store), &ctx.guard);
    let mut row: Vec<(ItemId, Utility)> = Vec::new();
    for tx in DbReader::new(io::BufReader::new(std::fs::File::open(path)?)) {
        let tx = tx?;
        row.clear();
        row.extend(tx.items.iter().filter(|e| twu.get(&e.item).copied().unwrap_or(0) >= min).map(|e| (e.item, e.utility)));
        if !row.is_empty() { db.push(&row, tx.transaction_utility)?; }
    }
    db.seal()?;
    Ok(db)
}

/// UP-Growth (variant Growth) / UP-Growth+ (variant GrowthPlus).
pub fn run_up_growth(path: &std::path::Path, ctx: &mut MiningContext, variant: UpVariant) -> io::Result<u64> {
    ctx.apply_os_safety_net();
    let min = ctx.min_utility;
    ctx.progress.set_stage("Phase 1: TWU Calculation (DGU)");
    let twu = item_twus(path)?;
    admit_tree(ctx, if variant == UpVariant::Growth { "UP-Growth" } else { "UP-Growth+" }, &twu, min, &|_| 0)?;
    let db = spool_db(path, &twu, min, ctx)?;
    ctx.progress.set_stage("Phase 2: Global UP-Tree Construction (DGN)");
    let (mut arena, tree, miu) = build_up_tree(&db, &twu, min, ctx)?;

    ctx.progress.set_stage(&format!("Phase 3: CPB Tree Mining ({} node pages in RAM)", arena.resident_pages()));
    let mut cands = ItemsetSpool::new(Arc::clone(&ctx.store), &ctx.guard);
    let miner = UpMiner { variant, miu: &miu, ctx, min };
    for item in tree.mining_order() {
        miner.mine(&tree, &mut arena, item, &mut Vec::new(), &mut cands)?;
    }
    drop(tree);
    drop(arena);
    crate::mining::core::memory_guard::release_free_memory();

    ctx.progress.set_stage("Phase 4: Exact Utility Computation");
    verify_candidates(&mut cands, &db, &twu, ctx)
}

/// TKU (Wu, Shie, Tseng & Yu, KDD 2012): Top-K utility mining on the UP-Tree.
/// * PE (pre-evaluation): the border starts at the K-th best exact utility among all 1- and
///   2-itemsets (a valid lower bound on the K-th best utility).
/// * Phase 1 mines potential top-K itemsets with UP-Growth+ at the border.
/// * Phase 2 computes exact utilities; SE: candidates whose estimated utility is below the
///   current border (raised as exact results arrive) are skipped.
pub fn run_tku(path: &std::path::Path, ctx: &mut MiningContext) -> io::Result<u64> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use std::sync::{Mutex, atomic::AtomicI64};
    ctx.apply_os_safety_net();
    let k = ctx.k.unwrap_or(100) as usize;
    let heap_res = ctx.guard.reserve(k.saturating_add(1).saturating_mul(64)).ok_or_else(|| io::Error::new(
        io::ErrorKind::OutOfMemory, format!("Top-K with K={} does not fit the memory budget", k)))?;

    ctx.progress.set_stage("TKU: TWU and pre-evaluation (PE)");
    let twu = item_twus(path)?;
    let db_all = spool_db(path, &twu, 0, ctx)?;
    let mut item_u: HashMap<ItemId, Utility> = HashMap::new();
    let mut pair_occ = 0usize;
    db_all.scan(|items, us, _| {
        for (i, u) in items.iter().zip(us) { *item_u.entry(*i).or_insert(0) += u; }
        pair_occ += items.len() * items.len().saturating_sub(1) / 2;
        true
    })?;
    let border0 = super::pair_util::kth_best_small_itemset_utility(
        &db_all, item_u.values().copied(), k, &ctx.guard, ctx.guard.native_remaining() / 2, pair_occ)?;
    drop(item_u);
    // Admission once the border is known (items with TWU below it are not promising). The
    // pre-evaluation above is itself bounded by half of the free budget.
    admit_tree(ctx, "TKU", &twu, border0, &|_| 0)?;

    ctx.progress.set_stage(&format!("TKU: Phase 1 (border {})", border0));
    let (mut arena, tree, miu) = build_up_tree(&db_all, &twu, border0, ctx)?;
    let mut cands = ItemsetSpool::new(Arc::clone(&ctx.store), &ctx.guard);
    let miner = UpMiner { variant: UpVariant::GrowthPlus, miu: &miu, ctx, min: border0 };
    for item in tree.mining_order() {
        miner.mine(&tree, &mut arena, item, &mut Vec::new(), &mut cands)?;
    }
    drop(tree);
    drop(arena);

    ctx.progress.set_stage("TKU: Phase 2 (exact utilities, SE)");
    let border = AtomicI64::new(border0);
    let heap: Mutex<BinaryHeap<Reverse<(Utility, Vec<ItemId>)>>> = Mutex::new(BinaryHeap::new());
    verify_with(&mut cands, &db_all, &twu, ctx, &|| border.load(Ordering::Relaxed), &|items, u, _w| {
        if u < border.load(Ordering::Relaxed) { return Ok(()); }
        let mut h = heap.lock().unwrap();
        h.push(Reverse((u, items.to_vec())));
        if h.len() > k { h.pop(); }
        if h.len() == k { border.fetch_max(h.peek().unwrap().0 .0, Ordering::Relaxed); }
        Ok(())
    })?;
    let mut results: Vec<(Utility, Vec<ItemId>)> = heap.into_inner().unwrap().into_iter().map(|r| r.0).collect();
    results.sort_by(|a, b| b.0.cmp(&a.0));
    let mut w = crate::mining::core::result_writer::ResultWriter::new(&ctx.output_path)?;
    for (u, items) in &results { w.write_hui(items, *u)?; }
    w.finalize()?;
    drop(heap_res);
    ctx.progress.huis_found.store(results.len() as u64, Ordering::Relaxed);
    Ok(results.len() as u64)
}
