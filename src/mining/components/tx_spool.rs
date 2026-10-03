//! Compact on-disk copy of the TWU-filtered database, for algorithms that rescan it.
//!
//! Re-parsing the SPMF text file costs ~1 s per scan on large datasets. Partitioned
//! structures (EUCS) and batched candidate verification may need many scans when the
//! budget is small, so the filtered transactions are written once, as binary segments in
//! the ChunkStore (on disk, never cached in RAM), and replayed from there.

use std::io;
use std::sync::Arc;
use crate::storage::{chunk_store::ChunkStore, page_layout::PageFlags};
use crate::types::{ItemId, PageId, Utility};

const SEGMENT_BYTES: usize = 1 << 20;

pub struct TxSpool {
    store: Arc<dyn ChunkStore + Send + Sync>,
    pages: Vec<PageId>,
    buf: Vec<u8>,
}

impl TxSpool {
    pub fn new(store: Arc<dyn ChunkStore + Send + Sync>) -> Self {
        Self { store, pages: Vec::new(), buf: Vec::with_capacity(SEGMENT_BYTES) }
    }

    /// Append one transaction: items with their utilities, and the transaction utility.
    pub fn push(&mut self, items: &[(ItemId, Utility)], tu: Utility) -> io::Result<()> {
        self.buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&tu.to_le_bytes());
        for &(item, u) in items {
            self.buf.extend_from_slice(&item.to_le_bytes());
            self.buf.extend_from_slice(&u.to_le_bytes());
        }
        if self.buf.len() >= SEGMENT_BYTES {
            self.flush()?;
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
        Ok(())
    }

    /// Replay all transactions in order. `f(items, utilities, tu)` returns false to stop.
    pub fn scan(&self, mut f: impl FnMut(&[ItemId], &[Utility], Utility) -> bool) -> io::Result<()> {
        let mut page = Vec::new();
        let mut items: Vec<ItemId> = Vec::new();
        let mut utils: Vec<Utility> = Vec::new();
        for &id in &self.pages {
            self.store.read_page(id, &mut page)?;
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
