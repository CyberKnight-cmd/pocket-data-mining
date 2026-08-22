use std::io::Write;
use pocket_data_mining::{
    mining::algorithms::fhm::Fhm,
    mining::algorithms::two_phase::TwoPhase,
    mining::core::{HuimAlgorithm, MiningContext, DataSource},
    storage::FileChunkStore,
    buffer_pool::pool::BufferPool,
    buffer_pool::eviction::LruPolicy,
};
use std::sync::Arc;
use tempfile::NamedTempFile;

const TINY_DB: &str = "\
1 2:40:20 20\n\
1 3:60:30 30\n\
2 3:50:25 25\n\
";

const MIN_UTILITY: i64 = 45;

fn write_tiny_db() -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(TINY_DB.as_bytes()).unwrap();
    file
}

fn make_store() -> (Arc<FileChunkStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileChunkStore::new(dir.path(), false).unwrap());
    (store, dir)
}

fn read_huis(path: &std::path::Path) -> Vec<(Vec<u32>, i64)> {
    let content = std::fs::read_to_string(path).unwrap();
    content.lines().filter(|l| !l.is_empty()).map(|line| {
        let parts: Vec<&str> = line.split("#UTIL:").collect();
        let utility: i64 = parts[1].trim().parse().unwrap();
        let mut items: Vec<u32> = parts[0].trim().split_whitespace()
            .map(|s| s.parse().unwrap()).collect();
        items.sort();
        (items, utility)
    }).collect()
}

fn create_ctx(store: Arc<FileChunkStore>, out_path: std::path::PathBuf, min_utility: i64) -> MiningContext {
    let pool = BufferPool::new_arc(
        1024 * 1024,
        store.clone() as Arc<dyn pocket_data_mining::storage::chunk_store::ChunkStore + Send + Sync>,
        Box::new(LruPolicy::new()),
    );
    MiningContext::new(
        pool,
        store as Arc<dyn pocket_data_mining::storage::chunk_store::ChunkStore + Send + Sync>,
        std::sync::Arc::new(pocket_data_mining::progress::MiningProgress::new()),
        min_utility,
        out_path,
        None,
        1,
    )
}

#[test]
fn fhm_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output.txt");

    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut fhm = Fhm::new(false);
    let count = fhm.run(DataSource::file(db_file.path()), &mut ctx).unwrap();

    assert_eq!(count, 5, "Expected exactly 5 HUIs");
    
    let huis = read_huis(&out_path);
    let hui_set: std::collections::HashSet<_> = huis.iter().cloned().collect();
    assert!(hui_set.contains(&(vec![1], 50)));
    assert!(hui_set.contains(&(vec![2], 45)));
    assert!(hui_set.contains(&(vec![3], 55)));
    assert!(hui_set.contains(&(vec![1,3], 60)));
    assert!(hui_set.contains(&(vec![2,3], 50)));
}

#[test]
fn twophase_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output2.txt");

    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut twophase = TwoPhase::new();
    let count = twophase.run(DataSource::file(db_file.path()), &mut ctx).unwrap();

    assert_eq!(count, 5, "Expected exactly 5 HUIs");
    
    let huis = read_huis(&out_path);
    let hui_set: std::collections::HashSet<_> = huis.iter().cloned().collect();
    assert!(hui_set.contains(&(vec![1], 50)));
    assert!(hui_set.contains(&(vec![2], 45)));
    assert!(hui_set.contains(&(vec![3], 55)));
    assert!(hui_set.contains(&(vec![1,3], 60)));
    assert!(hui_set.contains(&(vec![2,3], 50)));
}

#[test]
fn fhm_no_huis_when_min_utility_too_high() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output.txt");

    let mut ctx = create_ctx(store, out_path, 10_000);
    let mut fhm = Fhm::new(false);
    let count = fhm.run(DataSource::file(db_file.path()), &mut ctx).unwrap();
    assert_eq!(count, 0);
}
