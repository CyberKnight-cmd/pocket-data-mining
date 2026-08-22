/// Unique identifier for a page in the ChunkStore.
pub type PageId = u64;

/// Metadata for one page frame in the buffer pool.
/// Kept in RAM at all times — body bytes may be evicted.
#[derive(Debug, Clone)]
pub struct PageMeta {
    pub page_id: PageId,
    /// Byte size of the page payload (excluding page header).
    pub size_bytes: u32,
    /// True if the in-RAM frame has been modified since last flush.
    pub dirty: bool,
    /// Number of active pins on this page. Eviction is blocked while > 0.
    pub pin_count: u16,
    /// Monotonic tick of last access (for recency scoring).
    pub last_access_tick: u64,
    /// Number of times this page has been accessed.
    pub access_count: u32,
    /// Predicted probability of access in the near future (set by Prefetcher).
    pub predicted_access_prob: f32,
    /// DFS depth at which this page was created.
    pub traversal_depth: u16,
    /// Estimated I/O cost to reload from disk (nanoseconds).
    pub reload_cost_ns: u32,
    /// Estimated CPU cost to recompute from parent structures (nanoseconds).
    pub recompute_cost_ns: u32,
    /// Page ID of the parent page (for dependency tracking).
    pub parent_page: Option<PageId>,
}

impl PageMeta {
    pub fn new(page_id: PageId, size_bytes: u32) -> Self {
        Self {
            page_id,
            size_bytes,
            dirty: false,
            pin_count: 0,
            last_access_tick: 0,
            access_count: 0,
            predicted_access_prob: 0.0,
            traversal_depth: 0,
            reload_cost_ns: 0,
            recompute_cost_ns: 0,
            parent_page: None,
        }
    }

    /// True if the page is currently pinned and must not be evicted.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    /// Cheaper to recompute than to reload from disk.
    #[inline]
    pub fn prefer_recompute(&self) -> bool {
        self.recompute_cost_ns > 0 && self.recompute_cost_ns < self.reload_cost_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_meta_new() {
        let m = PageMeta::new(99, 65536);
        assert_eq!(m.page_id, 99);
        assert_eq!(m.size_bytes, 65536);
        assert!(!m.dirty);
        assert_eq!(m.pin_count, 0);
        assert!(!m.is_pinned());
    }

    #[test]
    fn page_meta_pinned() {
        let mut m = PageMeta::new(1, 4096);
        m.pin_count = 1;
        assert!(m.is_pinned());
        m.pin_count = 0;
        assert!(!m.is_pinned());
    }

    #[test]
    fn prefer_recompute_logic() {
        let mut m = PageMeta::new(1, 4096);
        m.reload_cost_ns = 1_000_000;   // 1ms reload
        m.recompute_cost_ns = 100_000;  // 0.1ms recompute → prefer recompute
        assert!(m.prefer_recompute());

        m.recompute_cost_ns = 5_000_000; // 5ms recompute → prefer reload
        assert!(!m.prefer_recompute());
    }
}
