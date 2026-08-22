use std::io::Cursor;
use pocket_data_mining::{
    preprocessing::{
        db_reader::DbReader,
        twu_filter::TwuFilter,
        chunker::Chunker,
    },
    storage::{FileChunkStore, ChunkStore},
};

const SAMPLE_DB: &str = "\
1 3 4:100:20 50 30\n\
1 2 4:150:60 40 50\n\
1 3:80:40 40\n\
2 3 4:120:10 50 60\n\
";

#[test]
fn full_pipeline_produces_pages() {
    // 1. Parse
    let reader = DbReader::new(Cursor::new(SAMPLE_DB));
    let txs: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert_eq!(txs.len(), 4);

    // 2. TWU filter (min_utility = 100)
    let filter = TwuFilter::new(100);
    let filter_result = filter.compute(txs.iter().cloned());
    // All items appear in high-utility txs so they should mostly pass
    // TWU(1) = 100+150+80 = 330; TWU(2) = 150+120 = 270; TWU(3) = 100+80+120 = 300; TWU(4) = 100+150+120 = 370
    assert!(filter_result.passes(1));
    assert!(filter_result.passes(2));
    assert!(filter_result.passes(3));
    assert!(filter_result.passes(4));

    // 3. Apply filter to get filtered transactions
    let filtered: Vec<_> = txs.iter()
        .filter_map(|tx| filter_result.apply(tx))
        .collect();
    assert_eq!(filtered.len(), 4);

    // 4. Chunk into pages
    let dir_tmp = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir_tmp.path(), false).unwrap();
    let mut chunker = Chunker::new(&store, 4096);
    for tx in &filtered {
        chunker.add_transaction(tx).unwrap();
    }
    let page_dir = chunker.finalize().unwrap();

    // All 4 tids fit in one 4KB page
    assert_eq!(page_dir.entry_count(), 1);
    let entry = page_dir.find_page(0).unwrap();
    assert_eq!(entry.start_tid, 0);
    assert_eq!(entry.end_tid, 3);
    assert!(store.page_exists(entry.page_id));
}

#[test]
fn twu_ascending_order_maintained() {
    let reader = DbReader::new(Cursor::new(SAMPLE_DB));
    let txs: Vec<_> = reader.map(|r| r.unwrap()).collect();
    let filter = TwuFilter::new(0);
    let result = filter.compute(txs.into_iter());
    // ordered_items must be sorted by ascending TWU
    for w in result.ordered_items.windows(2) {
        assert!(w[0].1 <= w[1].1, "Items must be in ascending TWU order");
    }
}

#[test]
fn min_utility_filter_works() {
    let reader = DbReader::new(Cursor::new(SAMPLE_DB));
    let txs: Vec<_> = reader.map(|r| r.unwrap()).collect();
    // Very high min_utility: nothing passes
    let filter = TwuFilter::new(10_000);
    let result = filter.compute(txs.into_iter());
    assert!(result.ordered_items.is_empty());
}

#[test]
fn db_reader_counts_lines() {
    let reader = DbReader::new(Cursor::new(SAMPLE_DB));
    let count = reader.count();
    assert_eq!(count, 4);
}

#[test]
fn chunker_page_directory_is_queryable() {
    let dir_tmp = tempfile::tempdir().unwrap();
    let store = FileChunkStore::new(dir_tmp.path(), false).unwrap();
    // 10 transactions, tiny page size to force splits
    let mut chunker = Chunker::new(&store, 64);
    use pocket_data_mining::preprocessing::twu_filter::FilteredTransaction;
    use pocket_data_mining::types::ItemEntry;
    for i in 0..10u32 {
        let tx = FilteredTransaction {
            tid: i,
            transaction_utility: 100,
            items: vec![ItemEntry { item: 1, utility: 100 }],
        };
        chunker.add_transaction(&tx).unwrap();
    }
    let page_dir = chunker.finalize().unwrap();
    // Every tid 0..9 must be findable
    for tid in 0..10u32 {
        assert!(page_dir.find_page(tid).is_some(), "tid {tid} not found in directory");
    }
}
