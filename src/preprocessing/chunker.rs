use std::{collections::BTreeMap, io, path::PathBuf};
use serde::{Deserialize, Serialize};
use crate::{
    storage::chunk_store::ChunkStore,
    storage::page_layout::PageFlags,
    types::{ItemEntry, PageId},
};
use super::twu_filter::FilteredTransaction;

/// Maps a transaction ID range [start_tid, end_tid] to the page that contains them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageDirectoryEntry {
    pub start_tid: u32,
    pub end_tid: u32,     // inclusive
    pub page_id: PageId,
    pub byte_offset: u32, // offset within the page where this tx starts (for future use)
}

/// Lookup structure: given a tid, find the page_id that contains it.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PageDirectory {
    /// BTreeMap keyed by start_tid for O(log n) lookup.
    entries: BTreeMap<u32, PageDirectoryEntry>,
}

impl PageDirectory {
    pub fn add(&mut self, entry: PageDirectoryEntry) {
        self.entries.insert(entry.start_tid, entry);
    }

    /// Find the page containing the given tid.
    pub fn find_page(&self, tid: u32) -> Option<&PageDirectoryEntry> {
        // Find the entry with the largest start_tid <= tid
        self.entries.range(..=tid).next_back().and_then(|(_, e)| {
            if tid <= e.end_tid { Some(e) } else { None }
        })
    }

    pub fn entry_count(&self) -> usize { self.entries.len() }
}

/// Binary serialization format for a transaction chunk page.
/// Layout:
///   4 bytes: u32 num_transactions
///   For each transaction:
///     4 bytes: u32 tid
///     8 bytes: i64 transaction_utility
///     4 bytes: u32 num_items
///     num_items * 12 bytes: ItemEntry[] (item: u32 + padding(4) + utility: i64)
///     NOTE: ItemEntry is #[repr(C)] with size 16 (u32 + 4 pad + i64)
pub struct Chunker<'s> {
    store: &'s dyn ChunkStore,
    page_size_bytes: usize,
    current_page_buf: Vec<u8>,
    current_page_id: PageId,
    current_page_start_tid: u32,
    current_page_end_tid: u32,
    tx_count_in_page: u32,
    pub directory: PageDirectory,
}

impl<'s> Chunker<'s> {
    pub fn new(store: &'s dyn ChunkStore, page_size_bytes: usize) -> Self {
        let page_id = store.next_page_id();
        let mut buf = Vec::with_capacity(page_size_bytes);
        // Reserve 4 bytes for tx count at start of page
        buf.extend_from_slice(&0u32.to_le_bytes());
        Self {
            store,
            page_size_bytes,
            current_page_buf: buf,
            current_page_id: page_id,
            current_page_start_tid: 0,
            current_page_end_tid: 0,
            tx_count_in_page: 0,
            directory: PageDirectory::default(),
        }
    }

    /// Serialize a single FilteredTransaction to bytes.
    fn serialize_tx(tx: &FilteredTransaction) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&tx.tid.to_le_bytes());                         // 4 bytes
        buf.extend_from_slice(&tx.transaction_utility.to_le_bytes());         // 8 bytes
        buf.extend_from_slice(&(tx.items.len() as u32).to_le_bytes());        // 4 bytes
        for item in &tx.items {
            buf.extend_from_slice(&item.item.to_le_bytes());                   // 4 bytes
            buf.extend_from_slice(&[0u8; 4]);                                 // 4 bytes padding (for alignment)
            buf.extend_from_slice(&item.utility.to_le_bytes());               // 8 bytes
        }
        buf
    }

    /// Add a transaction to the current page. Flushes and starts a new page if needed.
    pub fn add_transaction(&mut self, tx: &FilteredTransaction) -> io::Result<()> {
        let tx_bytes = Self::serialize_tx(tx);

        // If adding this tx would exceed page size, flush current page first
        if self.tx_count_in_page > 0 && self.current_page_buf.len() + tx_bytes.len() > self.page_size_bytes {
            self.flush_page()?;
        }

        // Update start_tid tracking
        if self.tx_count_in_page == 0 {
            self.current_page_start_tid = tx.tid;
        }
        self.current_page_end_tid = tx.tid;
        self.tx_count_in_page += 1;

        self.current_page_buf.extend_from_slice(&tx_bytes);
        Ok(())
    }

    /// Flush the current page to the ChunkStore and start a fresh page.
    pub fn flush_page(&mut self) -> io::Result<()> {
        if self.tx_count_in_page == 0 { return Ok(()); }

        // Write the tx count into the first 4 bytes
        let count_bytes = self.tx_count_in_page.to_le_bytes();
        self.current_page_buf[0..4].copy_from_slice(&count_bytes);

        self.store.write_page(
            self.current_page_id,
            &self.current_page_buf,
            PageFlags::TX_CHUNK,
        )?;

        self.directory.add(PageDirectoryEntry {
            start_tid: self.current_page_start_tid,
            end_tid: self.current_page_end_tid,
            page_id: self.current_page_id,
            byte_offset: 0,
        });

        // Prepare next page
        let new_page_id = self.store.next_page_id();
        self.current_page_id = new_page_id;
        self.current_page_buf.clear();
        self.current_page_buf.extend_from_slice(&0u32.to_le_bytes()); // reserve tx count
        self.tx_count_in_page = 0;
        Ok(())
    }

    /// Finalize: flush any remaining data. Returns the completed directory.
    pub fn finalize(mut self) -> io::Result<PageDirectory> {
        self.flush_page()?;
        Ok(self.directory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ItemEntry;

    fn filtered_tx(tid: u32, tu: i64, items: &[(u32, i64)]) -> FilteredTransaction {
        FilteredTransaction {
            tid,
            transaction_utility: tu,
            items: items.iter().map(|(item, util)| ItemEntry { item: *item, utility: *util }).collect(),
        }
    }

    #[test]
    fn directory_find_page() {
        let mut dir = PageDirectory::default();
        dir.add(PageDirectoryEntry { start_tid: 0, end_tid: 9, page_id: 1, byte_offset: 0 });
        dir.add(PageDirectoryEntry { start_tid: 10, end_tid: 19, page_id: 2, byte_offset: 0 });
        assert_eq!(dir.find_page(5).unwrap().page_id, 1);
        assert_eq!(dir.find_page(10).unwrap().page_id, 2);
        assert_eq!(dir.find_page(15).unwrap().page_id, 2);
        assert!(dir.find_page(20).is_none());
    }

    #[test]
    fn chunker_serializes_and_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::storage::FileChunkStore::new(dir.path(), false).unwrap();
        let mut chunker = Chunker::new(&store, 4096);
        for i in 0..5u32 {
            let tx = filtered_tx(i, 100, &[(1, 50), (2, 50)]);
            chunker.add_transaction(&tx).unwrap();
        }
        let page_dir = chunker.finalize().unwrap();
        // All 5 tids fit in one 4KB page, so 1 directory entry
        assert_eq!(page_dir.entry_count(), 1);
        assert_eq!(page_dir.find_page(0).unwrap().start_tid, 0);
        assert_eq!(page_dir.find_page(4).unwrap().end_tid, 4);
    }

    #[test]
    fn chunker_splits_pages_when_full() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::storage::FileChunkStore::new(dir.path(), false).unwrap();
        // Very small page: 100 bytes. Each tx is ~4+8+4+1*(4+4+8)=36 bytes + header items
        // 100 / ~36 ≈ 2 per page
        let mut chunker = Chunker::new(&store, 100);
        for i in 0..6u32 {
            let tx = filtered_tx(i, 100, &[(i+1, 50)]);
            chunker.add_transaction(&tx).unwrap();
        }
        let page_dir = chunker.finalize().unwrap();
        // Should have split into multiple pages
        assert!(page_dir.entry_count() > 1, "Should have split into multiple pages");
    }
}
