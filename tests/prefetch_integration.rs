use pocket_data_mining::prefetch::{
    predictor::{TraversalState, ExtensionInfo},
    dfs_predictor::DfsPredictor,
    utility_predictor::UtilityPredictor,
    predictor::AccessPredictor,
};

fn make_state(n: usize) -> TraversalState {
    let mut s = TraversalState::new();
    for i in 0..n {
        s.extensions.push(ExtensionInfo {
            item: i as u32,
            ul_page_id: (i + 1) as u64,
            twu_ratio: 1.0 / (i as f32 + 1.0),
            estimated_utility: (1000 / (i + 1)) as i64,
            estimated_load_cost_ns: 1000,
        });
    }
    s
}

#[test]
fn dfs_predictor_ordering() {
    let state = make_state(5);
    let preds = DfsPredictor.predict(&state);
    assert_eq!(preds.len(), 5);
    // Must be monotonically decreasing priority
    for w in preds.windows(2) {
        assert!(w[0].1 >= w[1].1, "DFS priorities must be non-increasing");
    }
}

#[test]
fn utility_predictor_no_zero_utility_wins() {
    let mut state = TraversalState::new();
    state.extensions = vec![
        ExtensionInfo { item: 1, ul_page_id: 1, twu_ratio: 1.0, estimated_utility: 0, estimated_load_cost_ns: 0 },
        ExtensionInfo { item: 2, ul_page_id: 2, twu_ratio: 0.1, estimated_utility: 500, estimated_load_cost_ns: 0 },
    ];
    let preds = UtilityPredictor.predict(&state);
    let b1 = preds.iter().find(|(p,_)| *p==1).unwrap().1;
    let b2 = preds.iter().find(|(p,_)| *p==2).unwrap().1;
    assert!(b2 > b1, "Non-zero utility page should beat zero-utility page");
}

#[test]
fn both_predictors_return_same_page_count() {
    let state = make_state(4);
    let d = DfsPredictor.predict(&state);
    let u = UtilityPredictor.predict(&state);
    assert_eq!(d.len(), 4);
    assert_eq!(u.len(), 4);
}

#[test]
fn dfs_and_utility_prioritize_different_pages() {
    // DFS prioritizes by position; utility by value*prob/cost
    // Page 1 is first in list (DFS wins) but has very low utility
    // Page 3 has high utility but is 3rd in list
    let mut state = TraversalState::new();
    state.extensions = vec![
        ExtensionInfo { item: 1, ul_page_id: 1, twu_ratio: 0.01, estimated_utility: 1, estimated_load_cost_ns: 0 },
        ExtensionInfo { item: 2, ul_page_id: 2, twu_ratio: 0.5, estimated_utility: 500, estimated_load_cost_ns: 0 },
        ExtensionInfo { item: 3, ul_page_id: 3, twu_ratio: 1.0, estimated_utility: 9999, estimated_load_cost_ns: 0 },
    ];
    let dfs_top = DfsPredictor.predict(&state)[0].0;
    let util_preds = UtilityPredictor.predict(&state);
    let util_top = util_preds.iter().max_by(|a,b| a.1.partial_cmp(&b.1).unwrap()).unwrap().0;
    // DFS picks first (page 1); utility picks highest value (page 3)
    assert_eq!(dfs_top, 1);
    assert_eq!(util_top, 3);
}

#[tokio::test]
async fn prefetch_queue_submit_does_not_panic() {
    use pocket_data_mining::{
        prefetch::prefetch_queue::PrefetchQueue,
        storage::{FileChunkStore, ChunkStore},
        storage::page_layout::PageFlags,
    };
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileChunkStore::new(dir.path(), false).unwrap());
    let id = store.next_page_id();
    store.write_page(id, b"test", PageFlags::empty()).unwrap();
    let store_dyn: Arc<dyn pocket_data_mining::storage::chunk_store::ChunkStore + Send + Sync> = store;
    let queue = PrefetchQueue::new(store_dyn);
    queue.submit(id, 1.0);
    queue.submit_predictions(vec![(id, 0.5)]);
    // Give async task time to run
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    // Should have issued at least 1 prefetch
    // (Note: may issue 2 since we submitted same page_id twice)
    let issued = queue.prefetch_issued.load(std::sync::atomic::Ordering::Relaxed);
    assert!(issued >= 1, "Expected at least 1 prefetch to complete, got {issued}");
}
