use pocket_data_mining::storage::{FileChunkStore, ChunkStore};
use pocket_data_mining::storage::page_layout::PageFlags;

#[test]
fn test_page_layout_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir.path(), false).unwrap();
    let id = store.next_page_id();
    let payload = b"hello air-huim page";
    store.write_page(id, payload, PageFlags::TX_CHUNK).unwrap();
    let mut buf = Vec::new();
    let flags = store.read_page(id, &mut buf).unwrap();
    assert_eq!(buf, payload);
    assert!(flags.contains(PageFlags::TX_CHUNK));
    assert!(!flags.contains(PageFlags::COMPRESSED));
}

#[test]
fn test_page_exists_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir.path(), false).unwrap();
    let id = store.next_page_id();
    assert!(!store.page_exists(id));
    store.write_page(id, b"data", PageFlags::empty()).unwrap();
    assert!(store.page_exists(id));
    store.delete_page(id).unwrap();
    assert!(!store.page_exists(id));
}

#[test]
fn test_crc_corruption_detected() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir.path(), false).unwrap();
    let id = store.next_page_id();
    store.write_page(id, b"important data", PageFlags::UL_BODY).unwrap();
    
    // Corrupt the file by flipping a byte in the payload area
    use std::io::{Read, Write, Seek, SeekFrom};
    let path = dir.path().join(format!("{}/", id >> 16)).join(format!("{id}.chunk"));
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(24)).unwrap(); // jump past header to payload
    let mut byte = [0u8];
    f.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xFF;
    f.seek(SeekFrom::Start(24)).unwrap();
    f.write_all(&byte).unwrap();
    drop(f);
    
    let mut buf = Vec::new();
    let result = store.read_page(id, &mut buf);
    assert!(result.is_err(), "CRC corruption should be detected");
}

#[test]
fn test_compression_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir.path(), true).unwrap(); // compress=true
    let id = store.next_page_id();
    // Highly compressible data
    let payload: Vec<u8> = vec![42u8; 4096];
    store.write_page(id, &payload, PageFlags::UL_BODY).unwrap();
    let mut buf = Vec::new();
    let flags = store.read_page(id, &mut buf).unwrap();
    assert_eq!(buf, payload);
    // Highly repetitive data should be compressed
    assert!(flags.contains(PageFlags::COMPRESSED), "Repetitive data should compress");
}

#[test]
fn test_multiple_pages_sharding() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir.path(), false).unwrap();
    for _ in 0..5 {
        let id = store.next_page_id();
        let data = format!("page-{id}").into_bytes();
        store.write_page(id, &data, PageFlags::empty()).unwrap();
        let mut buf = Vec::new();
        store.read_page(id, &mut buf).unwrap();
        assert_eq!(buf, data);
    }
}

#[test]
fn test_page_byte_size() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir.path(), false).unwrap();
    let id = store.next_page_id();
    assert!(store.page_byte_size(id).is_none());
    store.write_page(id, b"abc", PageFlags::empty()).unwrap();
    let sz = store.page_byte_size(id).unwrap();
    // File = header (24 bytes) + payload (3 bytes) = 27
    assert_eq!(sz, 27);
}
