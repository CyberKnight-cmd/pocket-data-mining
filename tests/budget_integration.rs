//! Budget-safety + exactness suite for every algorithm.
//!
//! For several seeded random databases, the exact set of high-utility itemsets is
//! computed by brute force. Every algorithm is then run under a generous budget and
//! under a tiny one (forcing spills, paged structures, partitioned EUCS), single- and
//! multi-threaded, and must:
//!   1. produce exactly the brute-force result,
//!   2. keep the memory ledger's peak within the budget (plus bounded forced slack),
//!   3. release every reservation it made (no ledger leaks).

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use pocket_data_mining::{
    buffer_pool::{pool::BufferPool, eviction::LruPolicy},
    mining::{algorithms::*, core::{DataSource, HuimAlgorithm, MemoryGuard, MiningContext}, DatasetStats},
    storage::{chunk_store::ChunkStore, FileChunkStore},
};
use rand::{rngs::StdRng, Rng, SeedableRng};

type Itemset = Vec<u32>;

struct Db {
    txs: Vec<Vec<(u32, i64)>>,
}

fn random_db(seed: u64, n_tx: usize, n_items: u32, max_len: usize) -> Db {
    random_db_scaled(seed, n_tx, n_items, max_len, false)
}

/// `scaled`: each item has its own utility range (some cheap-but-frequent items, some
/// expensive-but-rare ones), like retail data. This produces items with high TWU but a
/// low own utility, which uniform utilities never do.
fn random_db_scaled(seed: u64, n_tx: usize, n_items: u32, max_len: usize, scaled: bool) -> Db {
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

fn write_db(db: &Db, path: &std::path::Path) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    for tx in &db.txs {
        let items: Vec<String> = tx.iter().map(|e| e.0.to_string()).collect();
        let utils: Vec<String> = tx.iter().map(|e| e.1.to_string()).collect();
        let tu: i64 = tx.iter().map(|e| e.1).sum();
        writeln!(f, "{}:{}:{}", items.join(" "), tu, utils.join(" ")).unwrap();
    }
}

/// Exact utility of every itemset that occurs, by enumerating each transaction's subsets.
fn brute_force(db: &Db) -> BTreeMap<Itemset, i64> {
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

fn huis(all: &BTreeMap<Itemset, i64>, min: i64) -> BTreeSet<(Itemset, i64)> {
    all.iter().filter(|e| *e.1 >= min).map(|(s, u)| (s.clone(), *u)).collect()
}

fn top_k(all: &BTreeMap<Itemset, i64>, k: usize) -> Vec<i64> {
    let mut u: Vec<i64> = all.values().copied().collect();
    u.sort_unstable_by(|a, b| b.cmp(a));
    u.truncate(k);
    u
}

/// Closed HUIs: HUIs with no proper superset of the same support (tid set).
fn closed_huis(db: &Db, all: &BTreeMap<Itemset, i64>, min: i64) -> BTreeSet<(Itemset, i64)> {
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

fn read_out(path: &std::path::Path) -> Vec<(Itemset, i64)> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    content.lines().filter(|l| !l.is_empty()).map(|line| {
        let parts: Vec<&str> = line.split("#UTIL:").collect();
        let mut items: Vec<u32> = parts[0].split_whitespace().map(|s| s.parse().unwrap()).collect();
        items.sort();
        (items, parts[1].trim().parse().unwrap())
    }).collect()
}

struct RunResult {
    out: Vec<(Itemset, i64)>,
    peak: usize,
    leaked: usize,
    /// Refused up front by admission control (only allowed at the tiny budget).
    refused: bool,
}

fn run(algo: &mut dyn HuimAlgorithm, db_path: &std::path::Path, min: i64, k: Option<u64>,
       budget: usize, threads: usize) -> RunResult {
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
    if let Err(e) = algo.run(DataSource::file(db_path), &mut ctx) {
        // A clean refusal by admission control (budget below what cannot be spilled) is an
        // acceptable outcome only at the tiny budget; anything else is a failure.
        assert!(e.kind() == std::io::ErrorKind::OutOfMemory && budget < (1 << 20), "{}: {e}", algo.name());
        return RunResult { out: Vec::new(), peak: guard.peak(), leaked: 0, refused: true };
    }
    let peak = guard.peak();
    drop(ctx);
    // Whatever is still charged must be cached pool frames (re-loadable, evictable).
    while pool.evict_one().unwrap().is_some() {}
    let leaked = guard.used();
    RunResult { out: read_out(&out), peak, leaked, refused: false }
}

fn exact_algorithms() -> Vec<(&'static str, Box<dyn HuimAlgorithm>)> {
    vec![
        ("two-phase", Box::new(two_phase::TwoPhase::new())),
        ("ihup", Box::new(ihup::Ihup::new())),
        ("hup-tree", Box::new(hup_tree::HupTree::new())),
        ("up-growth", Box::new(up_growth::UpGrowth::new())),
        ("up-growth+", Box::new(up_growth::UpGrowthPlus::new())),
        ("hui-trie", Box::new(hui_trie::HuiTrie::new())),
        ("fhm", Box::new(fhm::Fhm::new(false))),
        ("fhm+", Box::new(fhm_plus::FhmPlus::new(false))),
        ("hui-miner", Box::new(hui_miner::HuiMiner::new(false))),
        ("hup-miner", Box::new(hup_miner::HupMiner::new(false))),
        ("mhuiminer", Box::new(mhui_miner::MHuiMiner::new(false))),
        ("efim", Box::new(efim::Efim::new())),
        ("tko", Box::new(tko::Tko::new(false))),
        ("rept", Box::new(rept::Rept::new(false))),
        ("incfhm", Box::new(inc_fhm::IncFhm::new(false))),
    ]
}

/// Generous budget, and a tiny one that forces every spill path.
const BUDGETS: [usize; 2] = [256 << 20, 96 << 10];
/// Forced (always-granted) reservations — transient views, merge buffers, page reads —
/// may exceed the budget by a bounded amount per thread.
const FORCED_SLACK: usize = 3 << 20;

fn check_db(seed: u64, n_tx: usize, n_items: u32, max_len: usize, min: i64) {
    check(random_db(seed, n_tx, n_items, max_len), seed, min);
}

fn check(db: Db, seed: u64, min: i64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.txt");
    write_db(&db, &path);
    let all = brute_force(&db);
    let expected = huis(&all, min);
    assert!(!expected.is_empty(), "test DB should have HUIs (seed {seed})");

    for &budget in &BUDGETS {
        for threads in [1, 4] {
            for (name, mut algo) in exact_algorithms() {
                let r = run(algo.as_mut(), &path, min, None, budget, threads);
                assert!(!r.refused, "{name}: refused by admission control at budget {budget} (seed {seed})");
                let got: BTreeSet<_> = r.out.iter().cloned().collect();
                assert_eq!(r.out.len(), got.len(), "{name}: duplicate output (seed {seed}, budget {budget}, threads {threads})");
                assert_eq!(got, expected, "{name}: wrong HUIs (seed {seed}, budget {budget}, threads {threads})");
                assert!(r.peak <= budget + threads * FORCED_SLACK,
                    "{name}: ledger peak {} over budget {} (seed {seed}, threads {threads})", r.peak, budget);
                assert_eq!(r.leaked, 0, "{name}: {} bytes still reserved after run (seed {seed}, budget {budget})", r.leaked);
            }

            // Heuristics (approximate): every reported itemset must be a true HUI with its exact
            // utility (precision 100%); recall may be < 100%, but they must find something.
            for (name, mut algo) in [
                ("huim-ga", Box::new(huim_ga::HuimGa::new(false)) as Box<dyn HuimAlgorithm>),
                ("huim-bpso", Box::new(huim_bpso::HuimBpso::new(false))),
                ("mhui-aco", Box::new(mhui_aco::MhuiAco::new(false))),
            ] {
                let r = run(algo.as_mut(), &path, min, None, budget, threads);
                let got: BTreeSet<_> = r.out.iter().cloned().collect();
                assert_eq!(r.out.len(), got.len(), "{name}: duplicate output (seed {seed})");
                assert!(got.is_subset(&expected), "{name}: reported a non-HUI or a wrong utility (seed {seed}): {:?}",
                        got.difference(&expected).take(3).collect::<Vec<_>>());
                assert!(!got.is_empty(), "{name}: found no HUI at all (seed {seed}, {} exist)", expected.len());
                assert!(r.peak <= budget + threads * FORCED_SLACK, "{name}: ledger peak {} over budget {}", r.peak, budget);
                assert_eq!(r.leaked, 0, "{name} leaked (seed {seed})");
                eprintln!("{name} seed {seed} budget {budget}: recall {}/{}", got.len(), expected.len());
            }

            // HUIM-MMU: u(X) >= min over x in X of mu(x), mu(x) = max(min, floor(0.1 * u(x))).
            {
                let mu = |i: u32| -> i64 { min.max((0.1 * all.get(&vec![i]).copied().unwrap_or(0) as f64) as i64) };
                let want: BTreeSet<_> = all.iter()
                    .filter(|(s, u)| **u >= s.iter().map(|&i| mu(i)).min().unwrap())
                    .map(|(s, u)| (s.clone(), *u)).collect();
                let r = run(&mut huim_mmu::HuimMmu::new(false), &path, min, None, budget, threads);
                assert!(!r.refused, "refused by admission control at budget {budget} (seed {seed})");
                let got: BTreeSet<_> = r.out.iter().cloned().collect();
                assert_eq!(r.out.len(), got.len(), "huim-mmu duplicates (seed {seed})");
                assert_eq!(got, want, "huim-mmu (seed {seed}, budget {budget}, threads {threads})");
                assert_eq!(r.leaked, 0, "huim-mmu leaked (seed {seed})");
            }

            // SHUIM: windows of 1000 transactions sliding by 500; each window's HUIs in turn.
            {
                let mut want: Vec<(Itemset, i64)> = Vec::new();
                let mut window: std::collections::VecDeque<usize> = Default::default();
                let mine = |w: &std::collections::VecDeque<usize>, want: &mut Vec<(Itemset, i64)>| {
                    let sub = Db { txs: w.iter().map(|&t| db.txs[t].clone()).collect() };
                    want.extend(huis(&brute_force(&sub), min));
                };
                for t in 0..db.txs.len() {
                    window.push_back(t);
                    if window.len() >= 1000 {
                        mine(&window, &mut want);
                        for _ in 0..500 { window.pop_front(); }
                    }
                }
                if !window.is_empty() { mine(&window, &mut want); }
                let r = run(&mut shuim::Shuim::new(false), &path, min, None, budget, threads);
                // SHUIM's window (1000 transactions) is algorithm state that cannot spill; at
                // the tiny budget it does not fit and the run must be refused cleanly.
                if !r.refused {
                    let mut got = r.out.clone();
                    got.sort();
                    want.sort();
                    assert_eq!(got, want, "shuim (seed {seed}, budget {budget}, threads {threads})");
                    assert_eq!(r.leaked, 0, "shuim leaked (seed {seed})");
                }
            }

            // High average-utility itemsets: u(X) / |X| >= min.
            let r = run(&mut haui_miner::HauiMiner::new(), &path, min, None, budget, threads);
                assert!(!r.refused, "refused by admission control at budget {budget} (seed {seed})");
            let got: BTreeSet<_> = r.out.iter().cloned().collect();
            let want: BTreeSet<_> = all.iter().filter(|(s, u)| **u >= min * s.len() as i64)
                .map(|(s, u)| (s.clone(), *u)).collect();
            assert_eq!(got, want, "haui-miner (seed {seed}, budget {budget}, threads {threads})");
            assert_eq!(r.leaked, 0, "haui-miner leaked (seed {seed})");

            // Closed HUIs.
            let r = run(&mut efim_closed::EfimClosed::new(), &path, min, None, budget, threads);
                assert!(!r.refused, "refused by admission control at budget {budget} (seed {seed})");
            let got: BTreeSet<_> = r.out.iter().cloned().collect();
            assert_eq!(got, closed_huis(&db, &all, min), "efim-closed (seed {seed}, budget {budget})");
            assert_eq!(r.leaked, 0, "efim-closed leaked (seed {seed})");

            // Top-K: the K best utilities must match.
            for (name, mut algo) in [
                ("tko", Box::new(tko::Tko::new(false)) as Box<dyn HuimAlgorithm>),
                ("tku", Box::new(tku::Tku::new(false))),
                ("rept", Box::new(rept::Rept::new(false))),
            ] {
                let k = 25;
                let r = run(algo.as_mut(), &path, 0, Some(k as u64), budget, threads);
                assert!(!r.refused, "refused by admission control at budget {budget} (seed {seed})");
                let mut got: Vec<i64> = r.out.iter().map(|x| x.1).collect();
                got.sort_unstable_by(|a, b| b.cmp(a));
                assert_eq!(got, top_k(&all, k), "{name} top-k (seed {seed}, budget {budget})");
                for (s, u) in &r.out {
                    assert_eq!(all.get(s), Some(u), "{name}: reported utility wrong for {s:?}");
                }
                assert_eq!(r.leaked, 0, "{name} top-k leaked (seed {seed})");
            }
        }
    }
}

#[test]
fn all_algorithms_exact_and_within_budget_sparse() {
    check_db(1, 1500, 60, 8, 400);
}

#[test]
fn all_algorithms_exact_and_within_budget_dense() {
    check_db(2, 400, 14, 10, 900);
}

#[test]
fn all_algorithms_exact_and_within_budget_low_threshold() {
    check_db(3, 800, 30, 7, 120);
}

#[test]
fn all_algorithms_exact_and_within_budget_skewed_utilities() {
    for seed in 10..14 {
        check(random_db_scaled(seed, 1000, 40, 8, true), seed, 600);
    }
}

/// One very frequent, very cheap item (like "plastic bag" in retail): it has the highest
/// TWU (so it is ordered last and its remaining utility is 0) but its own utility sum is
/// below the threshold, while many HUIs still contain it.
fn cheap_frequent_db(seed: u64, n_tx: usize) -> Db {
    let mut db = random_db_scaled(seed, n_tx, 40, 6, true);
    let mut rng = StdRng::seed_from_u64(seed ^ 0xBA6);
    for tx in db.txs.iter_mut() {
        if rng.gen_bool(0.7) && !tx.iter().any(|e| e.0 == 999) {
            tx.push((999, 1));
        }
    }
    db
}

#[test]
fn all_algorithms_exact_and_within_budget_cheap_frequent_item() {
    for seed in 20..23 {
        let db = cheap_frequent_db(seed, 1000);
        let cheap_total: i64 = db.txs.iter().flatten().filter(|e| e.0 == 999).map(|e| e.1).sum();
        let min = cheap_total + 50;
        check(db, seed, min);
    }
}

fn ctx_with_budget(dir: &std::path::Path, budget: usize, threads: usize) -> MiningContext {
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

/// Below the floor (what cannot be spilled), the run is refused up front with a clear error
/// naming a workable budget — it neither crashes mid-run nor silently exceeds the budget.
#[test]
fn admission_refuses_budget_below_floor() {
    let db = random_db(5, 1500, 400, 8);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.txt");
    write_db(&db, &path);
    let mut ctx = ctx_with_budget(dir.path(), 8 << 10, 4);
    let err = fhm::Fhm::new(false).run(DataSource::file(&path), &mut ctx).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::OutOfMemory);
    assert!(err.to_string().contains("Use at least -b"), "{err}");
    assert_eq!(std::fs::read_to_string(dir.path().join("out.txt")).unwrap_or_default(), "", "no partial output");
}

/// When per-thread working sets do not fit, admission lowers the thread count instead of
/// overshooting — and the result is still exact.
#[test]
fn admission_reduces_threads_and_stays_exact() {
    let db = random_db(6, 1200, 60, 8);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.txt");
    write_db(&db, &path);
    let mut ctx = ctx_with_budget(dir.path(), 160 << 10, 16);
    fhm::Fhm::new(false).run(DataSource::file(&path), &mut ctx).unwrap();
    assert!(ctx.threads < 16, "expected fewer threads at a tiny budget, got {}", ctx.threads);
    let got: BTreeSet<_> = read_out(&dir.path().join("out.txt")).into_iter().collect();
    assert_eq!(got, huis(&brute_force(&db), 400));
}

/// Rematerialisation forced on at a tiny budget: lists that do not fit are dropped and later
/// recomputed from their parents (lazy join streams) instead of being spilled. Results must
/// stay exact for every utility-list variant (plain, PU-prune, average utility, Top-K,
/// incremental), and drops must actually happen.
#[test]
fn rematerialisation_is_exact() {
    use pocket_data_mining::mining::components::ul_join::RematMode;
    let mut dropped_all = 0;
    for seed in [1u64, 3, 20] {
        let db = if seed == 20 { cheap_frequent_db(seed, 1000) } else { random_db(seed, 1500, 60, 8) };
        let min = if seed == 20 {
            db.txs.iter().flatten().filter(|e| e.0 == 999).map(|e| e.1).sum::<i64>() + 50
        } else if seed == 3 { 120 } else { 400 };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.txt");
        write_db(&db, &path);
        let all = brute_force(&db);
        let expected = huis(&all, min);
        let mut dropped_total = 0;
        for (name, mut algo) in [
            ("fhm", Box::new(fhm::Fhm::new(false)) as Box<dyn HuimAlgorithm>),
            ("hui-miner", Box::new(hui_miner::HuiMiner::new(false))),
            ("hup-miner", Box::new(hup_miner::HupMiner::new(false))),
            ("incfhm", Box::new(inc_fhm::IncFhm::new(false))),
        ] {
            for threads in [1, 4] {
                let sub = dir.path().join(format!("{name}_{threads}"));
                std::fs::create_dir_all(&sub).unwrap();
                let mut ctx = ctx_with_budget(&sub, 160 << 10, threads);
                ctx.min_utility = min;
                ctx.remat = RematMode::Always;
                algo.run(DataSource::file(&path), &mut ctx).unwrap();
                dropped_total += ctx.progress.remat_dropped.load(std::sync::atomic::Ordering::Relaxed);
                let got: BTreeSet<_> = read_out(&sub.join("out.txt")).into_iter().collect();
                assert_eq!(got, expected, "{name} with rematerialisation (seed {seed}, threads {threads})");
            }
        }
        // Average utility and Top-K under rematerialisation.
        let sub = dir.path().join("haui");
        std::fs::create_dir_all(&sub).unwrap();
        let mut ctx = ctx_with_budget(&sub, 160 << 10, 2);
        ctx.min_utility = min;
        ctx.remat = RematMode::Always;
        haui_miner::HauiMiner::new().run(DataSource::file(&path), &mut ctx).unwrap();
        let want: BTreeSet<_> = all.iter().filter(|(s, u)| **u >= min * s.len() as i64).map(|(s, u)| (s.clone(), *u)).collect();
        let got: BTreeSet<_> = read_out(&sub.join("out.txt")).into_iter().collect();
        assert_eq!(got, want, "haui-miner with rematerialisation (seed {seed})");

        let sub = dir.path().join("tko");
        std::fs::create_dir_all(&sub).unwrap();
        let mut ctx = ctx_with_budget(&sub, 160 << 10, 2);
        ctx.min_utility = 0;
        ctx.k = Some(25);
        ctx.remat = RematMode::Always;
        tko::Tko::new(false).run(DataSource::file(&path), &mut ctx).unwrap();
        let mut got: Vec<i64> = read_out(&sub.join("out.txt")).iter().map(|x| x.1).collect();
        got.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(got, top_k(&all, 25), "tko with rematerialisation (seed {seed})");

        dropped_all += dropped_total;
    }
    assert!(dropped_all > 0, "expected lists to be dropped (rematerialised) at this budget");
}
