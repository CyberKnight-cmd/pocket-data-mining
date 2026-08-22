use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, Mutex},
    task,
};
use crate::{
    buffer_pool::pool::BufferPool,
    storage::chunk_store::ChunkStore,
    types::PageId,
};

/// A prefetch request with a priority (higher = more urgent).
#[derive(Debug, Clone)]
struct PrefetchRequest {
    page_id: PageId,
    priority: f32,
}

impl PartialEq for PrefetchRequest {
    fn eq(&self, other: &Self) -> bool { self.priority == other.priority }
}
impl Eq for PrefetchRequest {}
impl PartialOrd for PrefetchRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for PrefetchRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so BinaryHeap (max-heap) gives us highest priority first
        self.priority.partial_cmp(&other.priority)
            .unwrap_or(Ordering::Equal)
    }
}

/// Async prefetch queue. Accepts (page_id, priority) signals and issues
/// background reads into the buffer pool ChunkStore.
///
/// Prefetch is best-effort: it ONLY loads the raw bytes into a buffer;
/// the actual `BufferPool::pin` is called by the mining engine on demand.
/// The prefetch worker pre-warms the OS disk cache and tracks issued vs. useful prefetches.
///
/// Design note: we deliberately do NOT pin into the BufferPool from the prefetch thread
/// because pin() requires Arc<BufferPool> and managing concurrent pins across threads
/// would require complex lifetime coordination. Instead, the prefetch worker reads
/// from the ChunkStore directly (warming disk cache) and the mining thread's pin()
/// will hit the OS cache. For a future phase, the prefetch worker can write directly
/// into BufferPool frames using a dedicated API.
pub struct PrefetchQueue {
    sender: mpsc::UnboundedSender<(PageId, f32)>,
    /// Count of useful prefetches (page was accessed after prefetch).
    pub prefetch_issued: Arc<std::sync::atomic::AtomicU64>,
}

impl PrefetchQueue {
    /// Spawn the background prefetch worker and return the queue handle.
    pub fn new(store: Arc<dyn ChunkStore + Send + Sync>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<(PageId, f32)>();
        let issued = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let issued_clone = Arc::clone(&issued);

        tokio::spawn(async move {
            // Priority heap, protected by async Mutex for the async context.
            let heap: Arc<Mutex<BinaryHeap<PrefetchRequest>>> =
                Arc::new(Mutex::new(BinaryHeap::new()));

            while let Some((page_id, priority)) = rx.recv().await {
                heap.lock().await.push(PrefetchRequest { page_id, priority });

                // Drain heap greedily
                loop {
                    let req = { heap.lock().await.pop() };
                    let Some(req) = req else { break; };

                    let store2 = Arc::clone(&store);
                    let issued2 = Arc::clone(&issued_clone);
                    task::spawn_blocking(move || {
                        let mut buf = Vec::new();
                        if store2.read_page(req.page_id, &mut buf).is_ok() {
                            issued2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        // buf is dropped here; this is a cache-warming read
                    });
                }
            }
        });

        Self { sender: tx, prefetch_issued: issued }
    }

    /// Submit a prefetch hint. Non-blocking.
    pub fn submit(&self, page_id: PageId, priority: f32) {
        // If channel is closed (worker dropped), silently ignore.
        let _ = self.sender.send((page_id, priority));
    }

    /// Submit all predictions from a predictor.
    pub fn submit_predictions(&self, predictions: Vec<(PageId, f32)>) {
        for (page_id, priority) in predictions {
            self.submit(page_id, priority);
        }
    }
}
