use smallvec::SmallVec;
use crate::types::{ItemId, Utility, PageId};

/// Information about one candidate itemset extension at the current DFS node.
#[derive(Debug, Clone)]
pub struct ExtensionInfo {
    pub item: ItemId,
    /// Page ID where this extension's utility-list body is stored.
    pub ul_page_id: PageId,
    /// Normalized TWU ratio in [0, 1] — probability proxy.
    pub twu_ratio: f32,
    /// Estimated total utility of this extension's subtree.
    pub estimated_utility: Utility,
    /// Estimated I/O cost to load this page (nanoseconds).
    pub estimated_load_cost_ns: u32,
}

/// Snapshot of the DFS traversal state at a given point in the search.
#[derive(Debug, Clone)]
pub struct TraversalState {
    /// Current itemset prefix (items already committed on this DFS path).
    pub prefix: SmallVec<[ItemId; 16]>,
    /// Depth in the DFS tree (== prefix.len()).
    pub depth: u16,
    /// Candidate extensions at this node that haven't been processed yet.
    pub extensions: Vec<ExtensionInfo>,
}

impl TraversalState {
    pub fn new() -> Self {
        Self { prefix: SmallVec::new(), depth: 0, extensions: Vec::new() }
    }
}

impl Default for TraversalState {
    fn default() -> Self { Self::new() }
}

/// Predicts which pages will be needed next given the current traversal state.
/// Returns a list of (page_id, priority) pairs, highest priority = most urgent.
pub trait AccessPredictor: Send + Sync {
    fn predict(&self, state: &TraversalState) -> Vec<(PageId, f32)>;
}
