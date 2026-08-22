use crate::types::PageId;
use super::predictor::{AccessPredictor, TraversalState};

/// Utility-aware predictor: prioritizes pages that are both
/// likely to be needed AND expected to yield high-utility itemsets,
/// relative to their I/O load cost.
///
/// Formula: ExpectedBenefit = P(access) * ExpectedMiningValue / StorageLoadCost
pub struct UtilityPredictor;

impl AccessPredictor for UtilityPredictor {
    fn predict(&self, state: &TraversalState) -> Vec<(PageId, f32)> {
        state.extensions.iter().map(|ext| {
            let p_access = ext.twu_ratio.clamp(0.0, 1.0);
            let expected_value = ext.estimated_utility.max(0) as f32;
            let load_cost_secs = ext.estimated_load_cost_ns as f32 * 1e-9;
            let benefit = p_access * expected_value / (1.0 + load_cost_secs);
            (ext.ul_page_id, benefit)
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prefetch::predictor::{TraversalState, ExtensionInfo};

    fn ext(page_id: u64, twu: f32, utility: i64, load_ns: u32) -> ExtensionInfo {
        ExtensionInfo { item: page_id as u32, ul_page_id: page_id, twu_ratio: twu, estimated_utility: utility, estimated_load_cost_ns: load_ns }
    }

    #[test]
    fn high_utility_high_probability_wins() {
        let mut state = TraversalState::new();
        state.extensions = vec![
            ext(1, 0.9, 1000, 1000),   // high prob, high utility
            ext(2, 0.1, 10, 1000),     // low prob, low utility
        ];
        let preds = UtilityPredictor.predict(&state);
        // Page 1 should have much higher benefit
        let b1 = preds.iter().find(|(p,_)| *p==1).unwrap().1;
        let b2 = preds.iter().find(|(p,_)| *p==2).unwrap().1;
        assert!(b1 > b2, "High-value page should have higher benefit score");
    }

    #[test]
    fn expensive_load_reduces_benefit() {
        let mut state = TraversalState::new();
        state.extensions = vec![
            ext(1, 1.0, 1000, 0),            // free to load
            ext(2, 1.0, 1000, 1_000_000_000), // 1 second load
        ];
        let preds = UtilityPredictor.predict(&state);
        let b1 = preds.iter().find(|(p,_)| *p==1).unwrap().1;
        let b2 = preds.iter().find(|(p,_)| *p==2).unwrap().1;
        assert!(b1 > b2, "Cheaper-to-load page should rank higher");
    }

    #[test]
    fn zero_prob_gives_zero_benefit() {
        let mut state = TraversalState::new();
        state.extensions = vec![ext(5, 0.0, 9999, 0)];
        let preds = UtilityPredictor.predict(&state);
        assert_eq!(preds.len(), 1);
        assert!((preds[0].1 - 0.0).abs() < 1e-6);
    }
}
