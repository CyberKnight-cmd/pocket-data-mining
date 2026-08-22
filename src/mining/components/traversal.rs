use smallvec::SmallVec;
use crate::types::{ItemId, Utility, PageId};

/// Information about one candidate extension at the current DFS node.
/// This is the mining engine's view of what to process next.
#[derive(Debug, Clone)]
pub struct CandidateExtension {
    pub item: ItemId,
    /// Page ID of this extension's utility-list body in the ChunkStore.
    pub ul_page_id: PageId,
    /// sum_iutils + sum_rutils (TWU upper bound for this UL).
    pub twu: Utility,
    /// sum_iutils only (actual utility if no further extensions).
    pub sum_iutils: Utility,
    /// Estimated page load cost (from PageMeta, or 0 if unknown).
    pub load_cost_ns: u32,
}

/// Live DFS traversal context passed to the Prefetcher at each node.
#[derive(Debug)]
pub struct TraversalContext {
    /// Current itemset prefix on the DFS path.
    pub prefix: SmallVec<[ItemId; 16]>,
    /// DFS depth (== prefix.len()).
    pub depth: u16,
    /// Remaining candidates at this level.
    pub candidates: Vec<CandidateExtension>,
    /// The minimum utility threshold.
    pub min_utility: Utility,
}

impl TraversalContext {
    pub fn new(min_utility: Utility) -> Self {
        Self { prefix: SmallVec::new(), depth: 0, candidates: Vec::new(), min_utility }
    }

    /// Convert to a prefetch::TraversalState for the Prefetcher.
    pub fn to_prefetch_state(&self) -> crate::prefetch::predictor::TraversalState {
        use crate::prefetch::predictor::{TraversalState, ExtensionInfo};
        let total_twu: Utility = self.candidates.iter().map(|c| c.twu).sum::<Utility>().max(1);
        TraversalState {
            prefix: self.prefix.clone(),
            depth: self.depth,
            extensions: self.candidates.iter().map(|c| ExtensionInfo {
                item: c.item,
                ul_page_id: c.ul_page_id,
                twu_ratio: (c.twu as f64 / total_twu as f64).clamp(0.0, 1.0) as f32,
                estimated_utility: c.sum_iutils,
                estimated_load_cost_ns: c.load_cost_ns,
            }).collect(),
        }
    }
}
