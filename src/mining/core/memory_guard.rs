use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;
use crate::types::PageId;
use crate::storage::chunk_store::ChunkStore;
use crate::storage::page_layout::PageFlags;

/// Global memory budget enforcer.
/// Shared across all threads via Arc. Tracks total native (non-BufferPool)
/// allocations and provides spill-to-disk when the budget is exceeded.
pub struct MemoryGuard {
    used: AtomicUsize,
    budget: AtomicUsize,
    store: Arc<dyn ChunkStore + Send + Sync>,
}

impl MemoryGuard {
    pub fn new(budget: usize, store: Arc<dyn ChunkStore + Send + Sync>) -> Self {
        Self {
            used: AtomicUsize::new(0),
            budget: AtomicUsize::new(budget),
            store,
        }
    }

    /// Try to reserve `bytes` of native RAM. Returns true if within budget.
    pub fn try_alloc(&self, bytes: usize) -> bool {
        let prev = self.used.fetch_add(bytes, Ordering::Relaxed);
        if prev + bytes > self.budget.load(Ordering::Relaxed) {
            self.used.fetch_sub(bytes, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    /// Force-allocate bytes (for tracking already-allocated data).
    pub fn force_alloc(&self, bytes: usize) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Release `bytes` back to the budget.
    pub fn free(&self, bytes: usize) {
        self.used.fetch_sub(
            bytes.min(self.used.load(Ordering::Relaxed)),
            Ordering::Relaxed
        );
    }

    /// How many bytes are currently allocated.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// How many bytes remain before hitting the ceiling.
    pub fn remaining(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
            .saturating_sub(self.used.load(Ordering::Relaxed))
    }

    /// The total budget.
    pub fn budget(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
    }

    /// Serialize `data` to ChunkStore and return a PageId handle.
    pub fn spill(&self, data: &[u8]) -> io::Result<PageId> {
        let page_id = self.store.next_page_id();
        self.store.write_page(page_id, data, PageFlags::empty())?;
        Ok(page_id)
    }

    /// Load spilled data back from ChunkStore.
    pub fn load(&self, page_id: PageId) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.store.read_page(page_id, &mut buf)?;
        Ok(buf)
    }

    /// Load and delete — one-shot retrieval.
    pub fn load_and_delete(&self, page_id: PageId) -> io::Result<Vec<u8>> {
        let buf = self.load(page_id)?;
        let _ = self.store.delete_page(page_id);
        Ok(buf)
    }
}
