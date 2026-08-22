use std::sync::Arc;
use tempfile::tempdir;
use pocket_data_mining::{
    buffer_pool::{BufferPool, LruPolicy},
    storage::{chunk_store::{ChunkStore, FileChunkStore}, page_layout::PageFlags},
    types::PageId,
};

fn make_store_pool(budget: usize) -> (Arc<BufferPool>, Arc<dyn ChunkStore>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let store = Arc::new(FileChunkStore::new(dir.path(), false).unwrap()) as Arc<dyn ChunkStore>;
    let pool = BufferPool::new_arc(budget, store.clone(), Box::new(LruPolicy::new()));
    (pool, store, dir)
}

#[test]
fn test_pin_loads_page() {
    let (pool, store, _dir) = make_store_pool(1000);
    // write a page to store
    let data = b"hello world 123";
    store.write_page(1, data, PageFlags::empty()).unwrap();
    
    let guard = pool.pin(1).unwrap();
    assert_eq!(&*guard, data);
}

#[test]
fn test_budget_enforcement() {
    let (pool, store, _dir) = make_store_pool(250);
    for i in 1..=5 {
        let page = i as u64;
        let data = vec![0u8; 100];
        store.write_page(page, &data, PageFlags::empty()).unwrap();
        let guard = pool.pin(page).unwrap();
        assert_eq!(&*guard, data.as_slice());
        drop(guard);
        assert!(pool.used_bytes() <= 250);
    }
}

#[test]
fn test_pin_prevents_eviction() {
    let (pool, store, _dir) = make_store_pool(200);
    let data = vec![0u8; 150];
    
    store.write_page(1, &data, PageFlags::empty()).unwrap();
    store.write_page(2, &data, PageFlags::empty()).unwrap();
    
    let _guard1 = pool.pin(1).unwrap();
    // try to pin page 2, should fail due to lack of budget and no evictable pages
    let res = pool.pin(2);
    assert!(res.is_err());
}

#[test]
fn test_metrics_hit_miss() {
    let (pool, store, _dir) = make_store_pool(1000);
    let data = vec![0u8; 100];
    store.write_page(1, &data, PageFlags::empty()).unwrap();
    
    {
        let _g = pool.pin(1).unwrap();
    }
    {
        let _g = pool.pin(1).unwrap();
    }
    
    let misses = pool.metrics.misses.load(std::sync::atomic::Ordering::Relaxed);
    let hits = pool.metrics.hits.load(std::sync::atomic::Ordering::Relaxed);
    
    assert_eq!(misses, 1);
    assert_eq!(hits, 1);
}

#[test]
fn test_eviction_triggered() {
    let (pool, store, _dir) = make_store_pool(150);
    let data = vec![0u8; 100];
    
    for i in 1..=3 {
        let page = i as u64;
        store.write_page(page, &data, PageFlags::empty()).unwrap();
    }
    
    for i in 1..=3 {
        let _g = pool.pin(i as u64).unwrap();
    }
    
    let evictions = pool.metrics.evictions.load(std::sync::atomic::Ordering::Relaxed);
    assert!(evictions >= 1);
}


