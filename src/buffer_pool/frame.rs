use std::{ops::Deref, sync::Arc};
use crate::types::{PageId, PageMeta};

pub struct Frame {
    pub data: Box<[u8]>,
    pub meta: PageMeta,
}
impl Frame {
    pub fn new(page_id: PageId, data: Box<[u8]>) -> Self {
        let size = data.len() as u32;
        Self { data, meta: PageMeta::new(page_id, size) }
    }
}

/// RAII guard. Calls pool.unpin() on drop via Arc.
pub struct PinGuard {
    pub(crate) page_id: PageId,
    data_ptr: *const u8,
    len: usize,
    pool: Arc<super::pool::BufferPool>,
}
impl PinGuard {
    pub(crate) fn new(page_id: PageId, data_ptr: *const u8, len: usize, pool: Arc<super::pool::BufferPool>) -> Self {
        Self { page_id, data_ptr, len, pool }
    }
    pub fn page_id(&self) -> PageId { self.page_id }
}
impl Deref for PinGuard {
    type Target = [u8];
    fn deref(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.data_ptr, self.len) } }
}
impl Drop for PinGuard {
    fn drop(&mut self) { self.pool.unpin(self.page_id); }
}
unsafe impl Send for PinGuard {}

pub struct PinMutGuard {
    pub(crate) page_id: PageId,
    data_ptr: *mut u8,
    len: usize,
    pool: Arc<super::pool::BufferPool>,
}
impl PinMutGuard {
    pub(crate) fn new(page_id: PageId, data_ptr: *mut u8, len: usize, pool: Arc<super::pool::BufferPool>) -> Self {
        Self { page_id, data_ptr, len, pool }
    }
    pub fn page_id(&self) -> PageId { self.page_id }
}
impl Deref for PinMutGuard {
    type Target = [u8];
    fn deref(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.data_ptr, self.len) } }
}
impl std::ops::DerefMut for PinMutGuard {
    fn deref_mut(&mut self) -> &mut [u8] { unsafe { std::slice::from_raw_parts_mut(self.data_ptr, self.len) } }
}
impl Drop for PinMutGuard {
    fn drop(&mut self) { 
        self.pool.mark_dirty(self.page_id);
        self.pool.unpin(self.page_id); 
    }
}
unsafe impl Send for PinMutGuard {}
