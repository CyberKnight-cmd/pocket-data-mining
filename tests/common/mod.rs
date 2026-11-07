//! Shared helpers for the integration tests: seeded random utility databases, a brute-force
//! oracle, and a runner that executes an algorithm under a given memory budget and reports the
//! output, the ledger peak and leaked reservations.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use pocket_data_mining::{
    buffer_pool::{pool::BufferPool, eviction::LruPolicy},
    mining::{core::{DataSource, HuimAlgorithm, MemoryGuard, MiningContext}, DatasetStats},
    storage::{chunk_store::ChunkStore, FileChunkStore},
};
use pocket_data_mining::mining::components::tree_partition::PartitionMode;
use rand::{rngs::StdRng, Rng, SeedableRng};

pub type Itemset = Vec<u32>;

pub struct Db {
    pub txs: Vec<Vec<(u32, i64)>>,
}

pub fn random_db(seed: u64, n_tx: usize, n_items: u32, max_len: usize) -> Db {
    random_db_scaled(seed, n_tx, n_items, max_len, false)
}

/// `scaled`: each item has its own utility range (some cheap-but-frequent items, some
/// expensive-but-rare ones), like retail data. This produces items with high TWU but a
/// low own utility, which uniform utilities never do.
pub fn random_db_scaled(seed: u64, n_tx: usize, n_items: u32, max_len: usize, scaled: bool) -> Db {
    let mut rng = StdRng::seed_from_u64(seed);
    let txs = (0..n_tx).map(|_| {
        let len = rng.gen_range(1..=max_len);
        let mut items: BTreeSet<u32> = BTreeSet::new();
        while items.len() < len {
            // Skewed item popularity so some items are frequent.
            let i = (rng.gen_range(0.0f64..1.0).powi(2) * n_items as f64) as u32 + 1;
            items.insert(i);
        }
        items.into_iter().map(|i| {
            let hi = if scaled { 1 + (i as i64 * 37 % 60) } else { 20 };
            (i, rng.gen_range(1..=hi))
        }).collect()
    }).collect();
    Db { txs }
}

pub fn write_db(db: &Db, path: &std::path::Path) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    for tx in &db.txs {
        let items: Vec<String> = tx.iter().map(|e| e.0.to_string()).collect();
        let utils: Vec<String> = tx.iter().map(|e| e.1.to_string()).collect();
        let tu: i64 = tx.iter().map(|e| e.1).sum();
        writeln!(f, "{}:{}:{}", items.join(" "), tu, utils.join(" ")).unwrap();
    }
}

/// Exact utility of every itemset that occurs, by enumerating each transaction's subsets.
pub fn brute_force(db: &Db) -> BTreeMap<Itemset, i64> {
    let mut util: BTreeMap<Itemset, i64> = BTreeMap::new();
    for tx in &db.txs {
        let n = tx.len();
        for mask in 1u32..(1 << n) {
            let mut set = Vec::new();
            let mut u = 0;
            for k in 0..n {
                if mask & (1 << k) != 0 {
                    set.push(tx[k].0);
                    u += tx[k].1;
                }
            }
            *util.entry(set).or_insert(0) += u;
        }
    }
    util
}

pub fn huis(all: &BTreeMap<Itemset, i64>, min: i64) -> BTreeSet<(Itemset, i64)> {
    all.iter().filter(|e| *e.1 >= min).map(|(s, u)| (s.clone(), *u)).collect()
}

pub fn top_k(all: &BTreeMap<Itemset, i64>, k: usize) -> Vec<i64> {
    let mut u: Vec<i64> = all.values().copied().collect();
    u.sort_unstable_by(|a, b| b.cmp(a));
    u.truncate(k);
    u
}

/// Closed HUIs: HUIs with no proper superset of the same support (tid set).
pub fn closed_huis(db: &Db, all: &BTreeMap<Itemset, i64>, min: i64) -> BTreeSet<(Itemset, i64)> {
    let tidset = |s: &Itemset| -> Vec<usize> {
        db.txs.iter().enumerate()
            .filter(|(_, tx)| s.iter().all(|i| tx.iter().any(|e| e.0 == *i)))
            .map(|(t, _)| t).collect()
    };
    huis(all, min).into_iter().filter(|(s, _)| {
        let ts = tidset(s);
        // Not closed if some extra item occurs in every supporting transaction.
        let first = &db.txs[ts[0]];
        !first.iter().any(|e| !s.contains(&e.0) && ts.iter().all(|&t| db.txs[t].iter().any(|x| x.0 == e.0)))
    }).collect()
}

pub fn read_out(path: &std::path::Path) -> Vec<(Itemset, i64)> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    content.lines().filter(|l| !l.is_empty()).map(|line| {
        let parts: Vec<&str> = line.split("#UTIL:").collect();
        let mut items: Vec<u32> = parts[0].split_whitespace().map(|s| s.parse().unwrap()).collect();
        items.sort();
        (items, parts[1].trim().parse().unwrap())
    }).collect()
}

pub struct RunResult {
    pub out: Vec<(Itemset, i64)>,
    pub peak: usize,
    pub leaked: usize,
    /// Refused up front by admission control (only allowed at the tiny budget).
    pub refused: bool,
}

pub fn run(algo: &mut dyn HuimAlgorithm, db_path: &std::path::Path, min: i64, k: Option<u64>,
       budget: usize, threads: usize) -> RunResult {
    run_with(algo, db_path, min, k, budget, threads, None)
}

/// `run` with the tree miners' partition mode forced (None: the default, auto).
pub fn run_with(algo: &mut dyn HuimAlgorithm, db_path: &std::path::Path, min: i64, k: Option<u64>,
            budget: usize, threads: usize, partition: Option<PartitionMode>) -> RunResult {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileChunkStore::new(dir.path().join("chunks"), false).unwrap());
    let store_dyn = store.clone() as Arc<dyn ChunkStore + Send + Sync>;
    let pool = BufferPool::new_arc(budget, store.clone(), Box::new(LruPolicy::new()));
    let guard = Arc::new(MemoryGuard::new(budget, store_dyn.clone()));
    pool.attach_guard(guard.clone());
    let out = dir.path().join("out.txt");
    let stats = DatasetStats {
        num_transactions: 0, num_unique_items: 0, avg_transaction_length: 0.0,
        max_transaction_length: 0, total_utility: 0, density: 0.0,
        file_size_bytes: 0, estimated_db_ram_bytes: 0,
    };
    let mut ctx = MiningContext::new(
        pool.clone(), store_dyn, Arc::new(pocket_data_mining::progress::MiningProgress::new()),
        min, out.clone(), k, threads, 1, usize::MAX, guard.clone(), stats,
    );
    if let Some(p) = partition { ctx.tree_partition = p; }
    if let Err(e) = algo.run(DataSource::file(db_path), &mut ctx) {
        // A clean refusal by admission control (budget below what cannot be spilled) is an
        // acceptable outcome only at the tiny budget; anything else is a failure.
        assert!(e.kind() == std::io::ErrorKind::OutOfMemory && budget < (1 << 20), "{}: {e}", algo.name());
        eprintln!("refused: {e}");
        return RunResult { out: Vec::new(), peak: guard.peak(), leaked: 0, refused: true };
    }
    let peak = guard.peak();
    drop(ctx);
    // Whatever is still charged must be cached pool frames (re-loadable, evictable).
    while pool.evict_one().unwrap().is_some() {}
    let leaked = guard.used();
    RunResult { out: read_out(&out), peak, leaked, refused: false }
}

/// Generous budget, and a tiny one that forces every spill path.
pub const BUDGETS: [usize; 2] = [256 << 20, 96 << 10];

/// Forced (always-granted) reservations — transient views, merge buffers, page reads —
/// may exceed the budget by a bounded amount per thread.
pub const FORCED_SLACK: usize = 64 << 10;

/// One very frequent, very cheap item (like "plastic bag" in retail): it has the highest
/// TWU (so it is ordered last and its remaining utility is 0) but its own utility sum is
/// below the threshold, while many HUIs still contain it.
pub fn cheap_frequent_db(seed: u64, n_tx: usize) -> Db {
    let mut db = random_db_scaled(seed, n_tx, 40, 6, true);
    let mut rng = StdRng::seed_from_u64(seed ^ 0xBA6);
    for tx in db.txs.iter_mut() {
        if rng.gen_bool(0.7) && !tx.iter().any(|e| e.0 == 999) {
            tx.push((999, 1));
        }
    }
    db
}

pub fn ctx_with_budget(dir: &std::path::Path, budget: usize, threads: usize) -> MiningContext {
    let store = Arc::new(FileChunkStore::new(dir.join("chunks"), false).unwrap());
    let store_dyn = store.clone() as Arc<dyn ChunkStore + Send + Sync>;
    let pool = BufferPool::new_arc(budget, store.clone(), Box::new(LruPolicy::new()));
    let guard = Arc::new(MemoryGuard::new(budget, store_dyn.clone()));
    pool.attach_guard(guard.clone());
    let stats = DatasetStats {
        num_transactions: 0, num_unique_items: 0, avg_transaction_length: 0.0,
        max_transaction_length: 0, total_utility: 0, density: 0.0,
        file_size_bytes: 0, estimated_db_ram_bytes: 0,
    };
    MiningContext::new(pool, store_dyn, Arc::new(pocket_data_mining::progress::MiningProgress::new()),
                       400, dir.join("out.txt"), None, threads, 1, usize::MAX, guard, stats)
}
