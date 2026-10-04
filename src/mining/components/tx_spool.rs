//! Compact on-disk copy of the TWU-filtered database, for algorithms that rescan it.
//!
//! Re-parsing the SPMF text file costs ~1 s per scan on large datasets. Partitioned
//! structures (EUCS) and batched candidate verification may need many scans when the
//! budget is small, so the filtered transactions are written once, as binary segments in
//! the ChunkStore (on disk, never cached in RAM), and replayed from there.

use std::io;
use std::sync::Arc;
use crate::mining::core::memory_guard::{MemoryGuard, Reservation};
use crate::storage::{chunk_store::ChunkStore, page_layout::PageFlags};
use crate::types::{ItemId, PageId, Utility};

/// Segment size for on-disk spools: 1/32 of the budget, 1 KB to 1 MB. The write buffer and
/// each scanning thread's read buffer hold one segment (accounted to the ledger).
pub fn spool_segment_bytes(guard: &MemoryGuard) -> usize {
    (guard.budget() / 32).clamp(1024, 1 << 20)
}

pub struct TxSpool {
    store: Arc<dyn ChunkStore + Send + Sync>,
    guard: Arc<MemoryGuard>,
    pages: Vec<PageId>,
    buf: Vec<u8>,
    segment: usize,
    _buf_res: Option<Reservation>,
}

impl TxSpool {
    pub fn new(store: Arc<dyn ChunkStore + Send + Sync>, guard: &Arc<MemoryGuard>) -> Self {
        let segment = spool_segment_bytes(guard);
        let res = guard.reserve_force(segment);
        Self { store, guard: Arc::clone(guard), pages: Vec::new(), buf: Vec::with_capacity(segment), segment, _buf_res: Some(res) }
    }

    /// Append one transaction: items with their utilities, and the transaction utility.
    /// The buffer is flushed before it would outgrow the segment, so it grows beyond the
    /// segment only for a single transaction larger than that (accounted).
    pub fn push(&mut self, items: &[(ItemId, Utility)], tu: Utility) -> io::Result<()> {
        let rec = 12 + 12 * items.len();
        if !self.buf.is_empty() && self.buf.len() + rec > self.segment {
            self.flush()?;
        }
        self.buf.reserve(rec);
        if let Some(r) = self._buf_res.as_mut() {
            if self.buf.capacity() > r.bytes() { r.resize_force(self.buf.capacity()); }
        }
        self.buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&tu.to_le_bytes());
        for &(item, u) in items {
            self.buf.extend_from_slice(&item.to_le_bytes());
            self.buf.extend_from_slice(&u.to_le_bytes());
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() { return Ok(()); }
        let id = self.store.next_page_id();
        self.store.write_page(id, &self.buf, PageFlags::empty())?;
        self.pages.push(id);
        self.buf.clear();
        Ok(())
    }

    /// Finish writing; after this the spool is read-only.
    pub fn seal(&mut self) -> io::Result<()> {
        self.flush()?;
        self.buf = Vec::new();
        self._buf_res = None;
        Ok(())
    }

    /// Replay all transactions in order. `f(items, utilities, tu)` returns false to stop.
    pub fn scan(&self, mut f: impl FnMut(&[ItemId], &[Utility], Utility) -> bool) -> io::Result<()> {
        let mut page_res = self.guard.reserve_force(self.segment);
        let mut page = Vec::new();
        let mut items: Vec<ItemId> = Vec::new();
        let mut utils: Vec<Utility> = Vec::new();
        for &id in &self.pages {
            self.store.read_page(id, &mut page)?;
            if page.capacity() > page_res.bytes() { page_res.resize_force(page.capacity()); }
            let mut pos = 0;
            while pos < page.len() {
                let n = u32::from_le_bytes(page[pos..pos + 4].try_into().unwrap()) as usize;
                let tu = i64::from_le_bytes(page[pos + 4..pos + 12].try_into().unwrap());
                pos += 12;
                items.clear();
                utils.clear();
                for _ in 0..n {
                    items.push(u32::from_le_bytes(page[pos..pos + 4].try_into().unwrap()));
                    utils.push(i64::from_le_bytes(page[pos + 4..pos + 12].try_into().unwrap()));
                    pos += 12;
                }
                if !f(&items, &utils, tu) { return Ok(()); }
            }
        }
        Ok(())
    }
}

impl Drop for TxSpool {
    fn drop(&mut self) {
        for &id in &self.pages {
            let _ = self.store.delete_page(id);
        }
    }
}
