use crate::types::{PageId, PageMeta};
pub trait EvictionPolicy: Send + Sync {
    fn on_access(&mut self, id: PageId);
    fn on_insert(&mut self, id: PageId);
    fn on_evict(&mut self, id: PageId);
    fn pick_victim<'a>(&self, frames: &'a [(PageId, &'a PageMeta)]) -> Option<PageId>;
}
