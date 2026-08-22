use std::collections::HashMap;
use crate::types::{PageId, PageMeta};
use super::policy::EvictionPolicy;

#[derive(Default)]
pub struct LruPolicy {
    clock: u64,
    last_access: HashMap<PageId, u64>,
}

impl LruPolicy {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EvictionPolicy for LruPolicy {
    fn on_access(&mut self, id: PageId) {
        self.clock += 1;
        self.last_access.insert(id, self.clock);
    }

    fn on_insert(&mut self, id: PageId) {
        self.clock += 1;
        self.last_access.insert(id, self.clock);
    }

    fn on_evict(&mut self, id: PageId) {
        self.last_access.remove(&id);
    }

    fn pick_victim<'a>(&self, frames: &'a [(PageId, &'a PageMeta)]) -> Option<PageId> {
        let mut min_time = u64::MAX;
        let mut victim = None;
        for (id, meta) in frames {
            if meta.is_pinned() {
                continue;
            }
            if let Some(&time) = self.last_access.get(id) {
                if time < min_time {
                    min_time = time;
                    victim = Some(*id);
                }
            }
        }
        victim
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used() {
        let mut p = LruPolicy::new();
        p.on_insert(1);
        p.on_insert(2);
        p.on_access(1);
        
        let m1 = PageMeta::new(1, 0);
        let m2 = PageMeta::new(2, 0);
        
        let frames = vec![(1, &m1), (2, &m2)];
        assert_eq!(p.pick_victim(&frames), Some(2));
    }

    #[test]
    fn pinned_page_not_evicted() {
        let mut p = LruPolicy::new();
        p.on_insert(1);
        p.on_insert(2);
        p.on_access(1);
        
        let m1 = PageMeta::new(1, 0);
        let mut m2 = PageMeta::new(2, 0);
        m2.pin_count = 1; // pinned!
        
        let frames = vec![(1, &m1), (2, &m2)];
        assert_eq!(p.pick_victim(&frames), Some(1));
    }

    #[test]
    fn all_pinned_returns_none() {
        let mut p = LruPolicy::new();
        p.on_insert(1);
        
        let mut m1 = PageMeta::new(1, 0);
        m1.pin_count = 1;
        
        let frames = vec![(1, &m1)];
        assert_eq!(p.pick_victim(&frames), None);
    }
}
