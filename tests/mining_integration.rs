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
    let guard = Arc::new(pocket_data_mining::mining::MemoryGuard::new(
        1024 * 1024 * 1024,
        store.clone() as Arc<dyn pocket_data_mining::storage::chunk_store::ChunkStore + Send + Sync>,
    ));
    let stats = pocket_data_mining::mining::DatasetStats {
        num_transactions: 0,
        num_unique_items: 0,
        avg_transaction_length: 0.0,
        max_transaction_length: 0,
        total_utility: 0,
        density: 0.0,
        file_size_bytes: 0,
        estimated_db_ram_bytes: 0,
    };
    MiningContext::new(
        pool,
        store as Arc<dyn pocket_data_mining::storage::chunk_store::ChunkStore + Send + Sync>,
        std::sync::Arc::new(pocket_data_mining::progress::MiningProgress::new()),
        min_utility,
        out_path,
        None,
        1,
        1,
        usize::MAX,
        guard,
        stats,
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

    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
    
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

    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
    
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

#[test]
fn tko_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_tko.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    ctx.k = Some(5);
    let mut tko = pocket_data_mining::mining::algorithms::tko::Tko::new(false);
    let count = tko.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn huptree_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_huptree.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::hup_tree::HupTree::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn ihup_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_ihup.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::ihup::Ihup::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn efim_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_efim.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::efim::Efim::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn upgrowth_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_upgrowth.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::up_growth::UpGrowth::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}


#[test]
fn hupminer_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_hupminer.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::hup_miner::HupMiner::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn mhuiminer_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_mhuiminer.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::mhui_miner::MHuiMiner::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn huitrie_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_huitrie.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::hui_trie::HuiTrie::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}



#[test]
fn efimclosed_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_efimclosed.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::efim_closed::EfimClosed::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn hauiminer_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_hauiminer.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::haui_miner::HauiMiner::new();
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}


#[test]
fn rept_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_rept.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    ctx.k = Some(5);
    let mut algo = pocket_data_mining::mining::algorithms::rept::Rept::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}


#[test]
fn tku_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_tku.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    ctx.k = Some(5);
    let mut algo = pocket_data_mining::mining::algorithms::tku::Tku::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    println!("count: {}", count); let huis = read_huis(&out_path); println!("huis: {:?}", huis); assert_eq!(count, 5, "Expected exactly 5 HUIs");
}
#[test]
fn huim_mmu_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_huimmmu.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::huim_mmu::HuimMmu::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn shuim_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_shuim.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::shuim::Shuim::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    assert_eq!(count, 5, "Expected exactly 5 HUIs");
}

#[test]
fn incfhm_exact_tiny_database() {
    let db_file = write_tiny_db();
    let (store, _dir) = make_store();
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output_incfhm.txt");
    let mut ctx = create_ctx(store, out_path.clone(), MIN_UTILITY);
    let mut algo = pocket_data_mining::mining::algorithms::inc_fhm::IncFhm::new(false);
    let count = algo.run(pocket_data_mining::mining::core::data_source::DataSource::file(db_file.path()), &mut ctx).unwrap();
    assert_eq!(count, 5, "Expected exactly 5 HUIs");
}
