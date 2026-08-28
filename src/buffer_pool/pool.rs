use std::{
    io,
    sync::{atomic::{AtomicUsize, Ordering}, Arc},
};
use parking_lot::Mutex;
use dashmap::DashMap;
use crate::{
    storage::{chunk_store::ChunkStore, page_layout::PageFlags},
    types::PageId,
};
use super::{eviction::policy::EvictionPolicy, frame::{Frame, PinGuard}, metrics::BufferPoolMetrics};

pub struct BufferPool {
    pub budget_bytes: std::sync::atomic::AtomicUsize,
    used_bytes: AtomicUsize,
    frames: DashMap<PageId, Frame>,
    eviction: Mutex<Box<dyn EvictionPolicy>>,
    store: Arc<dyn ChunkStore>,
    pub metrics: Arc<BufferPoolMetrics>,
    tick: AtomicUsize,
}

impl BufferPool {
    pub fn new(budget_bytes: usize, store: Arc<dyn ChunkStore>, eviction: Box<dyn EvictionPolicy>) -> Self {
        Self {
            budget_bytes: AtomicUsize::new(budget_bytes),
            used_bytes: AtomicUsize::new(0),
            frames: DashMap::new(),
            eviction: Mutex::new(eviction),
            store,
            metrics: Arc::new(BufferPoolMetrics::new()),
            tick: AtomicUsize::new(0),
        }
    }

    pub fn new_arc(budget_bytes: usize, store: Arc<dyn ChunkStore>, eviction: Box<dyn EvictionPolicy>) -> Arc<Self> {
        Arc::new(Self::new(budget_bytes, store, eviction))
    }

    pub fn used_bytes(&self) -> usize { self.used_bytes.load(Ordering::Relaxed) }
    pub fn budget_bytes(&self) -> usize { self.budget_bytes.load(Ordering::Relaxed) }
    pub fn budget_remaining(&self) -> usize { self.budget_bytes().saturating_sub(self.used_bytes()) }
    
    pub fn set_budget(&self, bytes: usize) {
        self.budget_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn pin(self: &Arc<Self>, page_id: PageId) -> io::Result<PinGuard> {
        if let Some(mut frame) = self.frames.get_mut(&page_id) {
            frame.meta.pin_count += 1;
            frame.meta.access_count += 1;
            frame.meta.last_access_tick = self.tick.fetch_add(1, Ordering::Relaxed) as u64;
            let ptr = frame.data.as_ptr();
            let len = frame.data.len();
            self.metrics.record_hit();
            self.eviction.lock().on_access(page_id);
            return Ok(PinGuard::new(page_id, ptr, len, Arc::clone(self)));
        }

        self.metrics.record_miss();

        let mut buf = Vec::new();
        let t0 = std::time::Instant::now();
        self.store.read_page(page_id, &mut buf)?;
        let load_ns = t0.elapsed().as_nanos().min(u32::MAX as u128) as u32;
        let page_size = buf.len();
        self.metrics.record_bytes_read(page_size as u64);

        while self.used_bytes() + page_size > self.budget_bytes() {
            match self.evict_one()? {
                Some(_) => {}
                None => return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Buffer pool full ({}/{} bytes), all pages pinned", self.used_bytes(), self.budget_bytes()),
                )),
            }
        }

        let mut frame = Frame::new(page_id, buf.into_boxed_slice());
        frame.meta.pin_count = 1;
        frame.meta.access_count = 1;
        frame.meta.last_access_tick = self.tick.fetch_add(1, Ordering::Relaxed) as u64;
        frame.meta.reload_cost_ns = load_ns;

        let ptr = frame.data.as_ptr();
        let len = frame.data.len();

        self.used_bytes.fetch_add(page_size, Ordering::Relaxed);
        self.metrics.update_peak(self.used_bytes() as u64);

        self.frames.insert(page_id, frame);
        self.eviction.lock().on_insert(page_id);

        Ok(PinGuard::new(page_id, ptr, len, Arc::clone(self)))
    }

    pub fn pin_mut(self: &Arc<Self>, page_id: PageId) -> io::Result<crate::buffer_pool::frame::PinMutGuard> {
        if let Some(mut frame) = self.frames.get_mut(&page_id) {
            frame.meta.pin_count += 1;
            frame.meta.access_count += 1;
            frame.meta.last_access_tick = self.tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64;
            let ptr = frame.data.as_mut_ptr();
            let len = frame.data.len();
            self.metrics.record_hit();
            self.eviction.lock().on_access(page_id);
            return Ok(crate::buffer_pool::frame::PinMutGuard::new(page_id, ptr, len, Arc::clone(self)));
        }

        self.metrics.record_miss();

        let mut buf = Vec::new();
        let t0 = std::time::Instant::now();
        self.store.read_page(page_id, &mut buf)?;
        let load_ns = t0.elapsed().as_nanos().min(u32::MAX as u128) as u32;
        let page_size = buf.len();
        self.metrics.record_bytes_read(page_size as u64);

        while self.used_bytes() + page_size > self.budget_bytes() {
            match self.evict_one()? {
                Some(_) => {}
                None => return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Buffer pool full ({}/{} bytes), all pages pinned", self.used_bytes(), self.budget_bytes()),
                )),
            }
        }

        let mut frame = crate::buffer_pool::frame::Frame::new(page_id, buf.into_boxed_slice());
        frame.meta.pin_count = 1;
        frame.meta.access_count = 1;
        frame.meta.last_access_tick = self.tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64;
        frame.meta.reload_cost_ns = load_ns;

        let ptr = frame.data.as_mut_ptr();
        let len = frame.data.len();

        self.used_bytes.fetch_add(page_size, std::sync::atomic::Ordering::Relaxed);
        self.metrics.update_peak(self.used_bytes() as u64);

        self.frames.insert(page_id, frame);
        self.eviction.lock().on_insert(page_id);

        Ok(crate::buffer_pool::frame::PinMutGuard::new(page_id, ptr, len, Arc::clone(self)))
    }

    /// Insert a newly created page directly into the BufferPool.
    /// This is strictly required to prevent the algorithm from bypassing the BufferPool budget
    /// and writing directly to the ChunkStore.
    pub fn insert_page(&self, page_id: PageId, data: Vec<u8>) -> io::Result<()> {
        let page_size = data.len();
        
        while self.used_bytes() + page_size > self.budget_bytes() {
            match self.evict_one()? {
                Some(_) => {}
                None => return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Buffer pool full ({}/{} bytes), all pages pinned", self.used_bytes(), self.budget_bytes()),
                )),
            }
        }

        let mut frame = crate::buffer_pool::frame::Frame::new(page_id, data.into_boxed_slice());
        frame.meta.pin_count = 0;
        frame.meta.access_count = 1;
        frame.meta.last_access_tick = self.tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64;
        frame.meta.dirty = true; // MUST be dirty so it flushes to disk upon eviction!
        frame.meta.reload_cost_ns = 50_000; // estimated reload cost since it hasn't been loaded

        self.used_bytes.fetch_add(page_size, std::sync::atomic::Ordering::Relaxed);
        self.metrics.update_peak(self.used_bytes() as u64);

        self.frames.insert(page_id, frame);
        self.eviction.lock().on_insert(page_id);
        Ok(())
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
        let is_dirty = self.frames.get(&page_id).map(|f| f.meta.dirty).unwrap_or(false);
        if is_dirty {
            let (data, len) = {
                let f = self.frames.get(&page_id).unwrap();
                (f.data.as_ptr(), f.data.len())
            };
            let slice = unsafe { std::slice::from_raw_parts(data, len) };
            self.store.write_page(page_id, slice, PageFlags::empty())?;
            self.metrics.record_dirty_flush();
            self.metrics.record_bytes_written(len as u64);
            if let Some(mut f) = self.frames.get_mut(&page_id) {
                f.meta.dirty = false;
            }
        }
        Ok(())
    }

    pub fn evict_one(&self) -> io::Result<Option<PageId>> {
        let candidate = {
            let pairs: Vec<(PageId, crate::types::PageMeta)> =
                self.frames.iter().map(|f| (*f.key(), f.value().meta.clone())).collect();
            let refs: Vec<(PageId, &crate::types::PageMeta)> = pairs.iter().map(|(k, v)| (*k, v)).collect();
            self.eviction.lock().pick_victim(&refs)
        };
        let Some(victim_id) = candidate else { return Ok(None); };
        self.flush(victim_id)?;
        let size = self.frames.remove(&victim_id).map(|f| f.1.data.len()).unwrap_or(0);
        self.used_bytes.fetch_sub(size, Ordering::Relaxed);
        self.eviction.lock().on_evict(victim_id);
        self.metrics.record_eviction();
        Ok(Some(victim_id))
    }

    pub fn set_predicted_prob(&self, page_id: PageId, prob: f32) {
        if let Some(mut frame) = self.frames.get_mut(&page_id) {
            frame.meta.predicted_access_prob = prob;
        }
    }
}
