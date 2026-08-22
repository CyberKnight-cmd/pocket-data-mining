use crate::types::PageId;
use super::predictor::{AccessPredictor, TraversalState};

/// DFS-order predictor: assigns priority by position in the extension list.
/// Extensions processed earlier in DFS get higher priority.
pub struct DfsPredictor;

impl AccessPredictor for DfsPredictor {
    fn predict(&self, state: &TraversalState) -> Vec<(PageId, f32)> {
        state.extensions.iter().enumerate().map(|(i, ext)| {
            // Priority decays: first extension is most urgently needed
            let priority = 1.0 / (1.0 + i as f32);
            (ext.ul_page_id, priority)
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prefetch::predictor::{TraversalState, ExtensionInfo};

    fn make_ext(page_id: u64) -> ExtensionInfo {
        ExtensionInfo { item: page_id as u32, ul_page_id: page_id, twu_ratio: 0.5, estimated_utility: 100, estimated_load_cost_ns: 1000 }
    }

    #[test]
    fn first_extension_highest_priority() {
        let mut state = TraversalState::new();
        state.extensions = vec![make_ext(10), make_ext(20), make_ext(30)];
        let preds = DfsPredictor.predict(&state);
        assert_eq!(preds.len(), 3);
        // First must have highest priority
        assert!(preds[0].1 > preds[1].1);
        assert!(preds[1].1 > preds[2].1);
        assert_eq!(preds[0].0, 10);
    }

    #[test]
    fn empty_extensions_returns_empty() {
        let state = TraversalState::new();
        let preds = DfsPredictor.predict(&state);
        assert!(preds.is_empty());
    }

    #[test]
    fn single_extension_priority_is_one() {
        let mut state = TraversalState::new();
        state.extensions = vec![make_ext(99)];
        let preds = DfsPredictor.predict(&state);
        assert_eq!(preds.len(), 1);
        assert!((preds[0].1 - 1.0).abs() < 1e-6);
    }
}
