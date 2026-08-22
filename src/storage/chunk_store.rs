use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::types::PageId;
use crate::storage::page_layout::{PageHeader, PAGE_MAGIC, PAGE_HEADER_SIZE, PageFlags};
use crate::storage::compression;

/// Core storage abstraction. Every page write/read routes through this.
pub trait ChunkStore: Send + Sync {
    fn write_page(&self, id: PageId, data: &[u8], flags: PageFlags) -> io::Result<()>;
    fn read_page(&self, id: PageId, buf: &mut Vec<u8>) -> io::Result<PageFlags>;
    fn delete_page(&self, id: PageId) -> io::Result<()>;
    fn page_exists(&self, id: PageId) -> bool;
    fn page_byte_size(&self, id: PageId) -> Option<u64>;
    fn next_page_id(&self) -> PageId;
}

/// File-per-chunk store. Layout: {root}/{shard}/{page_id}.chunk
/// Shard = page_id >> 16 (two-level dirs, avoids inode explosion).
pub struct FileChunkStore {
    root: PathBuf,
    counter: AtomicU64,
    compress: bool,
}

impl FileChunkStore {
    pub fn new(root: impl AsRef<Path>, compress: bool) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root, counter: AtomicU64::new(1), compress })
    }

    fn page_path(&self, id: PageId) -> PathBuf {
        let shard = id >> 16;
        self.root.join(format!("{shard}")).join(format!("{id}.chunk"))
    }

    fn ensure_shard_dir(&self, id: PageId) -> io::Result<()> {
        let shard = id >> 16;
        fs::create_dir_all(self.root.join(format!("{shard}")))
    }
}

impl ChunkStore for FileChunkStore {
    fn write_page(&self, id: PageId, payload: &[u8], flags: PageFlags) -> io::Result<()> {
        self.ensure_shard_dir(id)?;
        let path = self.page_path(id);

        // Optionally compress
        let (final_payload, final_flags) = if self.compress {
            let compressed = compression::compress(payload);
            if compressed.len() < (payload.len() * 85 / 100) {
                (compressed, flags | PageFlags::COMPRESSED)
            } else {
                (payload.to_vec(), flags)
            }
        } else {
            (payload.to_vec(), flags)
        };

        let crc = crc32fast::hash(&final_payload);
        let header = PageHeader {
            magic: PAGE_MAGIC,
            page_id: id,
            payload_len: final_payload.len() as u32,
            payload_crc32: crc,
            flags: final_flags,
        };

        let mut file = fs::File::create(&path)?;
        file.write_all(&header.to_bytes())?;
        file.write_all(&final_payload)?;
        file.flush()?;
        Ok(())
    }

    fn read_page(&self, id: PageId, buf: &mut Vec<u8>) -> io::Result<PageFlags> {
        let path = self.page_path(id);
        let mut file = fs::File::open(&path)?;
        let mut header_bytes = [0u8; PAGE_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let header = PageHeader::from_bytes(&header_bytes)?;
        
        if header.magic != PAGE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad page magic"));
        }
        if header.page_id != id {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "page_id mismatch"));
        }

        let mut compressed_payload = vec![0u8; header.payload_len as usize];
        file.read_exact(&mut compressed_payload)?;

        // Verify CRC
        let actual_crc = crc32fast::hash(&compressed_payload);
        if actual_crc != header.payload_crc32 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        // Decompress if needed
        if header.flags.contains(PageFlags::COMPRESSED) {
            *buf = compression::decompress(&compressed_payload)?;
        } else {
            *buf = compressed_payload;
        }
        Ok(header.flags)
    }

    fn delete_page(&self, id: PageId) -> io::Result<()> {
        let path = self.page_path(id);
        if path.exists() { fs::remove_file(path)?; }
        Ok(())
    }

    fn page_exists(&self, id: PageId) -> bool {
        self.page_path(id).exists()
    }

    fn page_byte_size(&self, id: PageId) -> Option<u64> {
        fs::metadata(self.page_path(id)).ok().map(|m| m.len())
    }

    fn next_page_id(&self) -> PageId {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}
