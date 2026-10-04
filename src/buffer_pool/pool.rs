use std::{
    collections::VecDeque,
    io,
    sync::{atomic::{AtomicUsize, Ordering}, Arc, OnceLock},
};
use parking_lot::Mutex;
use dashmap::{DashMap, mapref::entry::Entry};
use crate::{
    mining::core::memory_guard::MemoryGuard,
    storage::{chunk_store::ChunkStore, page_layout::PageFlags},
    types::PageId,
};
use super::{eviction::policy::EvictionPolicy, frame::{Frame, PinGuard}, metrics::BufferPoolMetrics};

/// RAM cost of a resident frame beyond its data (Frame + PageMeta + map/queue slots).
/// Charged to the shared ledger so many small pages cannot hide memory from it.
pub const FRAME_OVERHEAD: usize = 192;

/// Number of eviction candidates sampled per eviction (approximate policy over a sample,
/// instead of scanning every frame — O(1) per eviction regardless of pool size).
const EVICTION_SAMPLE: usize = 16;

pub struct BufferPool {
    pub budget_bytes: std::sync::atomic::AtomicUsize,
    used_bytes: AtomicUsize,
    frames: DashMap<PageId, Frame>,
    eviction: Mutex<Box<dyn EvictionPolicy>>,
    /// Insertion-ordered queue of resident page ids; sampled by `evict_one`.
    /// May contain stale ids (already evicted/discarded) — they are skipped lazily.
    clock: Mutex<VecDeque<PageId>>,
    store: Arc<dyn ChunkStore>,
    pub metrics: Arc<BufferPoolMetrics>,
    tick: AtomicUsize,
    /// When attached, every resident byte is also charged to this shared ledger.
    guard: OnceLock<Arc<MemoryGuard>>,
}

impl BufferPool {
    pub fn new(budget_bytes: usize, store: Arc<dyn ChunkStore>, eviction: Box<dyn EvictionPolicy>) -> Self {
        Self {
            budget_bytes: AtomicUsize::new(budget_bytes),
            used_bytes: AtomicUsize::new(0),
            frames: DashMap::new(),
            eviction: Mutex::new(eviction),
            clock: Mutex::new(VecDeque::new()),
            store,
            metrics: Arc::new(BufferPoolMetrics::new()),
            tick: AtomicUsize::new(0),
            guard: OnceLock::new(),
        }
    }

    pub fn new_arc(budget_bytes: usize, store: Arc<dyn ChunkStore>, eviction: Box<dyn EvictionPolicy>) -> Arc<Self> {
        Arc::new(Self::new(budget_bytes, store, eviction))
    }

    /// Share the process-wide memory ledger with this pool. After this, pool frames and
    /// native algorithm structures draw from the same budget.
    pub fn attach_guard(self: &Arc<Self>, guard: Arc<MemoryGuard>) {
        let resident = self.used_bytes() + self.frames.len() * FRAME_OVERHEAD;
        if self.guard.set(Arc::clone(&guard)).is_err() { return; }
        if resident > 0 { guard.force_alloc(resident); }
        // Cached pages are re-loadable, native structures are not: let the ledger evict us.
        let weak = Arc::downgrade(self);
        guard.set_reclaimer(Box::new(move |need| {
            let Some(pool) = weak.upgrade() else { return 0 };
            let before = pool.used_bytes();
            while before - pool.used_bytes().min(before) < need {
                match pool.evict_one() {
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            before.saturating_sub(pool.used_bytes())
        }));
    }

    pub fn guard(&self) -> Option<&Arc<MemoryGuard>> { self.guard.get() }

    pub fn used_bytes(&self) -> usize { self.used_bytes.load(Ordering::Relaxed) }
    pub fn budget_bytes(&self) -> usize { self.budget_bytes.load(Ordering::Relaxed) }
    pub fn budget_remaining(&self) -> usize { self.budget_bytes().saturating_sub(self.used_bytes()) }

    pub fn set_budget(&self, bytes: usize) {
        self.budget_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Try to account `size` new resident bytes against the pool budget and the shared ledger.
    fn try_charge(&self, size: usize) -> bool {
        let mut cur = self.used_bytes();
        loop {
            if cur + size > self.budget_bytes() { return false; }
            match self.used_bytes.compare_exchange_weak(cur, cur + size, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(a) => cur = a,
            }
        }
        if let Some(g) = self.guard.get() {
            if !g.try_alloc(size + FRAME_OVERHEAD) {
                self.used_bytes.fetch_sub(size, Ordering::Relaxed);
                return false;
            }
        }
        true
    }

    fn force_charge(&self, size: usize) {
        self.used_bytes.fetch_add(size, Ordering::Relaxed);
        if let Some(g) = self.guard.get() { g.force_alloc(size + FRAME_OVERHEAD); }
    }

    fn uncharge(&self, size: usize) {
        self.used_bytes.fetch_sub(size, Ordering::Relaxed);
        if let Some(g) = self.guard.get() { g.free(size + FRAME_OVERHEAD); }
    }

    /// Make room for `size` bytes by evicting. Returns false if nothing evictable remains.
    fn make_room(&self, size: usize) -> io::Result<bool> {
        loop {
            if self.try_charge(size) { return Ok(true); }
            if self.evict_one()?.is_none() { return Ok(false); }
        }
    }

    fn next_tick(&self) -> u64 { self.tick.fetch_add(1, Ordering::Relaxed) as u64 }

    pub fn pin(self: &Arc<Self>, page_id: PageId) -> io::Result<PinGuard> {
        let (ptr, len) = self.pin_raw(page_id)?;
        Ok(PinGuard::new(page_id, ptr as *const u8, len, Arc::clone(self)))
    }

    pub fn pin_mut(self: &Arc<Self>, page_id: PageId) -> io::Result<crate::buffer_pool::frame::PinMutGuard> {
        let (ptr, len) = self.pin_raw(page_id)?;
        Ok(crate::buffer_pool::frame::PinMutGuard::new(page_id, ptr, len, Arc::clone(self)))
    }

    fn pin_raw(self: &Arc<Self>, page_id: PageId) -> io::Result<(*mut u8, usize)> {
        if let Some(r) = self.try_pin_resident(page_id) {
            self.metrics.record_hit();
            return Ok(r);
        }
        self.metrics.record_miss();

        let mut buf = Vec::new();
        let t0 = std::time::Instant::now();
        self.store.read_page(page_id, &mut buf)?;
        let load_ns = t0.elapsed().as_nanos().min(u32::MAX as u128) as u32;
        let page_size = buf.len();
        self.metrics.record_bytes_read(page_size as u64);

        // A page that is being read must be resident while pinned. If every resident
        // frame is pinned we overcommit by this one page (bounded by threads × page size).
        if !self.make_room(page_size)? {
            self.force_charge(page_size);
        }

        let mut frame = Frame::new(page_id, buf.into_boxed_slice());
        frame.meta.pin_count = 1;
        frame.meta.access_count = 1;
        frame.meta.last_access_tick = self.next_tick();
        frame.meta.reload_cost_ns = load_ns;

        // Another thread may have loaded the same page meanwhile: use theirs, drop ours.
        let result = match self.frames.entry(page_id) {
            Entry::Occupied(mut e) => {
                self.uncharge(page_size);
                let f = e.get_mut();
                f.meta.pin_count += 1;
                f.meta.access_count += 1;
                (f.data.as_mut_ptr(), f.data.len())
            }
            Entry::Vacant(e) => {
                let r = e.insert(frame);
                let out = (r.data.as_ptr() as *mut u8, r.data.len());
                drop(r);
                self.metrics.update_peak(self.used_bytes() as u64);
                self.clock.lock().push_back(page_id);
                self.eviction.lock().on_insert(page_id);
                out
            }
        };
        Ok(result)
    }

    fn try_pin_resident(&self, page_id: PageId) -> Option<(*mut u8, usize)> {
        let tick = self.next_tick();
        let r = {
            let mut frame = self.frames.get_mut(&page_id)?;
            frame.meta.pin_count += 1;
            frame.meta.access_count += 1;
            frame.meta.last_access_tick = tick;
            (frame.data.as_mut_ptr(), frame.data.len())
        };
        self.eviction.lock().on_access(page_id);
        Some(r)
    }

    /// Insert a newly created page directly into the BufferPool.
    /// If the pool cannot make room (everything pinned, or the shared budget is taken by
    /// native structures), the page is written straight through to the ChunkStore instead
    /// of being cached — it is never held in RAM outside the budget.
    pub fn insert_page(&self, page_id: PageId, data: Vec<u8>) -> io::Result<()> {
        let page_size = data.len();

        if !self.make_room(page_size)? {
            self.store.write_page(page_id, &data, PageFlags::empty())?;
            self.metrics.record_dirty_flush();
            self.metrics.record_bytes_written(page_size as u64);
            return Ok(());
        }

        let mut frame = Frame::new(page_id, data.into_boxed_slice());
        frame.meta.pin_count = 0;
        frame.meta.access_count = 1;
        frame.meta.last_access_tick = self.next_tick();
        frame.meta.dirty = true; // MUST be dirty so it flushes to disk upon eviction!
        frame.meta.reload_cost_ns = 50_000; // estimated reload cost since it hasn't been loaded

        if let Some(old) = self.frames.insert(page_id, frame) {
            self.uncharge(old.data.len());
        }
        self.metrics.update_peak(self.used_bytes() as u64);
        self.clock.lock().push_back(page_id);
        self.eviction.lock().on_insert(page_id);
        Ok(())
    }

    /// Drop a page that will never be read again: removed from RAM without being
    /// flushed, and deleted from the ChunkStore if it was ever written there.
    pub fn discard(&self, page_id: PageId) {
        // A pinned page is still being read by someone; leave it to normal eviction.
        if let Some((_, f)) = self.frames.remove_if(&page_id, |_, f| f.meta.pin_count == 0) {
            self.uncharge(f.data.len());
            self.eviction.lock().on_evict(page_id);
        } else if self.frames.contains_key(&page_id) {
            return;
        }
        let _ = self.store.delete_page(page_id);
    }

    pub fn unpin(&self, page_id: PageId) {
        if let Some(mut frame) = self.frames.get_mut(&page_id) {
            frame.meta.pin_count = frame.meta.pin_count.saturating_sub(1);
        }
    }

    pub fn mark_dirty(&self, page_id: PageId) {
        if let Some(mut f) = self.frames.get_mut(&page_id) {
            f.meta.dirty = true;
        }
    }

    pub fn flush(&self, page_id: PageId) -> io::Result<()> {
        // Hold the shard read lock while writing so the frame cannot be removed under us.
        let len = {
            let Some(f) = self.frames.get(&page_id) else { return Ok(()) };
            if !f.meta.dirty { return Ok(()); }
            self.store.write_page(page_id, &f.data, PageFlags::empty())?;
            f.data.len()
        };
        self.metrics.record_dirty_flush();
        self.metrics.record_bytes_written(len as u64);
        if let Some(mut f) = self.frames.get_mut(&page_id) {
            f.meta.dirty = false;
        }
        Ok(())
    }

    /// Evict one unpinned page chosen by the eviction policy from a small sample of the
    /// oldest resident pages. Returns None if no page could be evicted.
    pub fn evict_one(&self) -> io::Result<Option<PageId>> {
        loop {
            // Sample up to EVICTION_SAMPLE live, unpinned candidates from the front of the clock.
            let mut sample: Vec<(PageId, crate::types::PageMeta)> = Vec::with_capacity(EVICTION_SAMPLE);
            {
                let mut clock = self.clock.lock();
                let mut scanned = 0;
                let limit = clock.len();
                while sample.len() < EVICTION_SAMPLE && scanned < limit {
                    let Some(id) = clock.pop_front() else { break };
                    scanned += 1;
                    match self.frames.get(&id) {
                        None => {} // stale id: already gone
                        Some(f) if f.meta.is_pinned() => clock.push_back(id),
                        Some(f) => sample.push((id, f.meta.clone())),
                    }
                }
                if sample.is_empty() { return Ok(None); }
                // Return the sample to the front so non-victims keep their age.
                for (id, _) in sample.iter().rev() { clock.push_front(*id); }
            }

            let victim = {
                let refs: Vec<(PageId, &crate::types::PageMeta)> = sample.iter().map(|(k, v)| (*k, v)).collect();
                self.eviction.lock().pick_victim(&refs)
            };
            let Some(victim_id) = victim.or_else(|| sample.first().map(|s| s.0)) else { return Ok(None) };

            self.flush(victim_id)?;
            // Only remove if still unpinned and clean (it may have been re-pinned or re-dirtied).
            let removed = self.frames.remove_if(&victim_id, |_, f| f.meta.pin_count == 0 && !f.meta.dirty);
            if let Some((_, f)) = removed {
                self.uncharge(f.data.len());
                self.eviction.lock().on_evict(victim_id);
                self.metrics.record_eviction();
                return Ok(Some(victim_id));
            }
            // Lost a race for this victim; try again with a fresh sample.
        }
    }

    pub fn set_predicted_prob(&self, page_id: PageId, prob: f32) {
        if let Some(mut frame) = self.frames.get_mut(&page_id) {
            frame.meta.predicted_access_prob = prob;
        }
    }

    pub fn store(&self) -> &Arc<dyn ChunkStore> { &self.store }
}

/// Owning handle to a page created by an algorithm. Dropping it discards the page
/// (from the pool and from disk), so dead DFS branches don't accumulate on disk or
/// get flushed by eviction.
pub struct OwnedPage {
    pub id: PageId,
    pool: Arc<BufferPool>,
}

impl OwnedPage {
    /// Allocate a page id whose contents will be inserted later (see `SpillArena`).
    pub fn reserve_id(pool: &Arc<BufferPool>) -> Self {
        Self { id: pool.store.next_page_id(), pool: Arc::clone(pool) }
    }

    /// Store `data` as a new page owned by the returned handle.
    pub fn create(pool: &Arc<BufferPool>, data: Vec<u8>) -> io::Result<Self> {
        let id = pool.store.next_page_id();
        pool.insert_page(id, data)?;
        Ok(Self { id, pool: Arc::clone(pool) })
    }

    pub fn pin(&self) -> io::Result<PinGuard> { self.pool.pin(self.id) }
}

impl Drop for OwnedPage {
    fn drop(&mut self) { self.pool.discard(self.id); }
}

impl std::fmt::Debug for OwnedPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OwnedPage({})", self.id)
    }
}
