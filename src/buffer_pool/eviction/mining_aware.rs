use crate::types::{PageId, PageMeta};
use super::policy::EvictionPolicy;

#[derive(Clone, Debug)]
pub struct EvictionWeights {
    pub w_recency: f32,
    pub w_freq: f32,
    pub w_future: f32,
    pub w_reload: f32,
}

impl Default for EvictionWeights {
    fn default() -> Self {
        Self {
            w_recency: 1.0,
            w_freq: 1.0,
            w_future: 1.0,
            w_reload: 1.0,
        }
    }
}

pub struct MiningAwarePolicy {
    weights: EvictionWeights,
    clock: u64,
}

impl MiningAwarePolicy {
    pub fn new(weights: EvictionWeights) -> Self {
        Self { weights, clock: 0 }
    }
}

impl EvictionPolicy for MiningAwarePolicy {
    fn on_access(&mut self, _id: PageId) {
        self.clock += 1;
    }

    fn on_insert(&mut self, _id: PageId) {
        self.clock += 1;
    }

    fn on_evict(&mut self, _id: PageId) {}

    fn pick_victim<'a>(&self, frames: &'a [(PageId, &'a PageMeta)]) -> Option<PageId> {
        let mut max_score = f32::NEG_INFINITY;
        let mut victim = None;
        
        for (id, meta) in frames {
            if meta.is_pinned() {
                continue;
            }
            let recency = (self.clock.saturating_sub(meta.last_access_tick)) as f32;
            let freq = meta.access_count as f32;
            let p_future = meta.predicted_access_prob;
            let reload_penalty = meta.reload_cost_ns as f32;
            
            let score = self.weights.w_recency * recency
                - self.weights.w_freq * freq
                - self.weights.w_future * p_future
                - self.weights.w_reload * reload_penalty;
                
            if score > max_score {
                max_score = score;
                victim = Some(*id);
            }
        }
        victim
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_highest_score() {
        let p = MiningAwarePolicy::new(EvictionWeights::default());
        let mut m1 = PageMeta::new(1, 0);
        m1.last_access_tick = 0;
        m1.access_count = 10;
        
        let mut m2 = PageMeta::new(2, 0);
        m2.last_access_tick = 0;
        m2.access_count = 5;
        
        let frames = vec![(1, &m1), (2, &m2)];
        // m1 has access_count=10, m2 has access_count=5.
        // Because of the inverted math bug fix, m2 (lower freq) should get evicted instead of m1.
        assert_eq!(p.pick_victim(&frames), Some(2));
    }

    #[test]
    fn pinned_page_not_evicted() {
        let p = MiningAwarePolicy::new(EvictionWeights::default());
        let mut m1 = PageMeta::new(1, 0);
        m1.access_count = 10;
        m1.pin_count = 1;
        
        let mut m2 = PageMeta::new(2, 0);
        m2.access_count = 5;
        
        let frames = vec![(1, &m1), (2, &m2)];
        assert_eq!(p.pick_victim(&frames), Some(2));
    }

    #[test]
    fn all_pinned_returns_none() {
        let p = MiningAwarePolicy::new(EvictionWeights::default());
        let mut m1 = PageMeta::new(1, 0);
        m1.pin_count = 1;
        let frames = vec![(1, &m1)];
        assert_eq!(p.pick_victim(&frames), None);
    }
}
