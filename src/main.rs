use clap::{Parser, Subcommand};
use std::path::PathBuf;
use dialoguer::{theme::ColorfulTheme, Select, Input};
use pocket_data_mining::mining::{Fhm, TwoPhase, Ihup, HupTree, UpGrowth, Efim, HuimAlgorithm, HuiTrie};
use pocket_data_mining::mining::algorithms::{efim_closed::EfimClosed, haui_miner::HauiMiner, huim_mmu::HuimMmu, shuim::Shuim, inc_fhm::IncFhm};

#[derive(Parser)]
#[command(name = "air-huim", about = "Adaptive Out-of-core Resource-aware High-Utility Itemset Mining", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run HUIM mining on a dataset
    Mine {
        #[arg(short, long)]
        dataset: Option<PathBuf>,
        #[arg(short, long)]
        algorithm: Option<String>,
        #[arg(short, long)]
        min_utility: Option<i64>,
        #[arg(short, long)]
        budget_mb: Option<usize>,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(short, long)]
        chunk_store: Option<PathBuf>,
        #[arg(long)]
        prefetch: Option<bool>,
        #[arg(long)]
        threads: Option<usize>,
        #[arg(long)]
        top_k: Option<u64>,
        #[arg(long)]
        min_length: Option<usize>,
        #[arg(long)]
        max_length: Option<usize>,
    },
    /// Run experiments across multiple memory budgets
    Experiment {
        #[arg(short, long)]
        dataset: PathBuf,
        #[arg(short, long, default_value = "1000")]
        min_utility: i64,
        #[arg(long, default_value = "1024")]
        budgets: String,
        #[arg(short, long, default_value = "results")]
        output_dir: PathBuf,
        #[arg(long)]
        reference: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Mine { dataset, algorithm, min_utility, budget_mb, output, chunk_store, prefetch, threads, top_k, min_length, max_length }) => {
            run_interactive_wizard(dataset, algorithm, min_utility, budget_mb, output, chunk_store, prefetch, threads, top_k, min_length, max_length);
        }
        Some(Commands::Experiment { dataset, min_utility, budgets, output_dir, reference }) => {
            let budget_list: Vec<usize> = budgets.split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .map(|mb| mb * 1024 * 1024)
                .collect();
            let runner = pocket_data_mining::experiment::runner::ExperimentRunner {
                budgets: budget_list,
                dataset_path: dataset,
                min_utility,
                output_root: output_dir.clone(),
                chunk_store_root: output_dir.join("chunks"),
                reference_path: reference,
            };
            match runner.run_all() {
                Ok(results) => {
                    if let Err(e) = pocket_data_mining::experiment::report::emit_report(&results, &output_dir) {
                        eprintln!("Report error: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        None => {
            run_interactive_wizard(None, None, None, None, None, None, None, None, None, None, None);
        }
    }
}

fn run_interactive_wizard(
    cli_dataset: Option<PathBuf>,
    cli_algorithm: Option<String>,
    cli_min_utility: Option<i64>,
    cli_budget_mb: Option<usize>,
    cli_output: Option<PathBuf>,
    cli_chunk_store: Option<PathBuf>,
    cli_prefetch: Option<bool>,
    cli_threads: Option<usize>,
    cli_top_k: Option<u64>,
    cli_min_length: Option<usize>,
    cli_max_length: Option<usize>,
) {
    println!("==========================================================");
    println!("     Air-HUIM Interactive Mining Wizard");
    println!("==========================================================\n");

    let theme = ColorfulTheme::default();

    let algorithm = match cli_algorithm {
        Some(a) => {
            println!("Algorithm: {} (auto-selected from CLI)", a);
            a.to_lowercase()
        },
        None => {
            let algorithms = [
                "── Family 1: Level-Wise ──",
                "  Two-Phase (Level-wise Candidate Generation)",
                "  IHUP (Incremental HUP-Tree)",
                "  HUP-Tree (Header-Table Utility Prefix)",
                "── Family 2: Tree-Based ──",
                "  UP-Growth (UP-Tree + DGU/DGN Pruning)",
                "  UP-Growth+ (Improved DLU/DLN Bounds)",
                "  HUI-Trie (Trie-based Exact Mining)",
                "── Family 3: Utility-List ──",
                "  FHM (Fastest — EUCS Pruning)               ★ Budget-Safe",
                "  FHM+ (FHM + Length Constraints)             ★ Budget-Safe",
                "  HUI-Miner (Classic, No EUCS)                ★ Budget-Safe",
                "  HUP-Miner (Parallel Utility Lists)          ★ Budget-Safe",
                "  mHUIMiner (Memory-Adaptive Utility Lists)   ★ Budget-Safe",
                "── Family 4: Database Projection ──",
                "  EFIM (Transaction Merging + Projection)",
                "  EFIM-Closed (Closed Itemset Projection)",
                "  HAUI-Miner (Approximate Utilities via Projection)",
                "── Family 5: Top-K ──",
                "  TKU (Top-K Utility Tree)",
                "  TKO (Top-K in One phase)",
                "  REPT (Top-K with Early Pruning)",
                "── Family 6: Streaming / Incremental ──",
                "  HUIM-MMU (Sliding Window MMU)",
                "  SHUIM (Streaming HUIM)",
                "  IncFHM (Incremental FHM)",
                "── Family 7: Heuristic / AI-Based ──",
                "  HUIM-GA (Genetic Algorithm)",
                "  HUIM-BPSO (Particle Swarm Optimization)",
                "  MHUI-ACO (Ant Colony Optimization)",
            ];
            let algo_idx = Select::with_theme(&theme)
                .with_prompt("Select Mining Algorithm")
                .default(0)
                .items(&algorithms[..])
                .interact()
                .unwrap();
            let algo_slug = match algo_idx {
                1 => "two-phase",
                2 => "ihup",
                3 => "hup-tree",
                5 => "up-growth",
                6 => "up-growth-plus",
                7 => "hui-trie",
                9 => "fhm",
                10 => "fhm-plus",
                11 => "hui-miner",
                12 => "hup-miner",
                13 => "mhuiminer",
                15 => "efim",
                16 => "efim-closed",
                17 => "haui-miner",
                19 => "tku",
                20 => "tko",
                21 => "rept",
                23 => "huim-mmu",
                24 => "shuim",
                25 => "incfhm",
                27 => "huim-ga",
                28 => "huim-bpso",
                29 => "mhui-aco",
                _ => {
                    println!("Please select an algorithm, not a family header.");
                    std::process::exit(1);
                }
            };
            algo_slug.to_string()
        }
    };

    let dataset = match cli_dataset {
        Some(d) => {
            println!("Dataset: {} (auto-selected from CLI)", d.display());
            d
        },
        None => {
            let dataset_str: String = Input::with_theme(&theme)
                .with_prompt("Path to dataset file (e.g. chainstore.txt)")
                .interact_text()
                .unwrap();
            PathBuf::from(dataset_str)
        }
    };

    println!("\nPrecomputing dataset statistics...");
    let stats = pocket_data_mining::mining::DatasetStats::precompute(&dataset)
        .unwrap_or_else(|e| {
            println!("Error reading dataset: {}", e);
            std::process::exit(1);
        });
    stats.print_summary();

    let mut min_utility = 1000;
    let mut top_k = None;

    if let Some(k) = cli_top_k {
        println!("Threshold Mode: Top-K (K={}) (auto-selected from CLI)", k);
        top_k = Some(k);
    } else if let Some(mu) = cli_min_utility {
        println!("Threshold Mode: Minimum Utility ({}) (auto-selected from CLI)", mu);
        min_utility = mu;
    } else {
        let modes = &["Minimum Utility Threshold", "Top-K (Mine most profitable K itemsets)"];
        let mode_idx = Select::with_theme(&theme)
            .with_prompt("Threshold Mode")
            .default(0)
            .items(&modes[..])
            .interact()
            .unwrap();

        if mode_idx == 0 {
            min_utility = Input::with_theme(&theme)
                .with_prompt("Minimum Utility Threshold")
                .default(10000)
                .interact_text()
                .unwrap();
        } else {
            let k: u64 = Input::with_theme(&theme)
                .with_prompt("How many top itemsets to find (K)?")
                .default(100)
                .interact_text()
                .unwrap();
            top_k = Some(k);
        }
    }

    if top_k.is_some() && algorithm.as_str() != "tko" && algorithm.as_str() != "rept" && algorithm.as_str() != "tku" {
        println!("\n[ERROR] You selected Top-K mode, but '{}' does not support it natively.", algorithm);
        println!("Only Family 5 algorithms (like TKU, TKO and REPT) support Top-K mode. For '{}', please run again and select 'Minimum Utility Threshold'.", algorithm);
        std::process::exit(1);
    }

    let budget_mb = match cli_budget_mb {
        Some(b) => {
            println!("Memory Budget: {} MB (auto-selected from CLI)", b);
            b
        },
        None => {
            Input::with_theme(&theme)
                .with_prompt("Memory budget in MB")
                .default(1024)
                .interact_text()
                .unwrap()
        }
    };

    let file_meta = std::fs::metadata(&dataset).unwrap_or_else(|_| {
        println!("Error: Failed to read dataset file. Does it exist?");
        std::process::exit(1);
    });
    let file_mb = file_meta.len() as f64 / 1024.0 / 1024.0;
    
    let physical_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    
    // Per-thread RAM cost based on actual dataset density
    let (ram_per_thread_mb, is_lock_bound) = match algorithm.as_str() {
        "up-growth" | "up-growth-plus" | "up-growth+" | "ihup" | "hup-tree" | "hui-trie" => {
            (stats.estimated_db_ram_bytes as f64 / 1024.0 / 1024.0, true)
        },
        "efim" => {
            // EFIM per-thread cost scales with density
            let per_thread = (stats.estimated_db_ram_bytes as f64 * stats.density * 100.0).max(50.0 * 1024.0 * 1024.0);
            (per_thread / 1024.0 / 1024.0, true) // Force single-threaded as per docs
        },
        "fhm" | "fhm+" | "fhm-plus" | "hui-miner" | "hup-miner" | "mhuiminer" | "mhui-miner" => {
            // Utility-list algorithms: per-thread cost is ~2x EUCS + utility list headers
            let eucs_mb = (stats.num_unique_items * stats.num_unique_items * 8) as f64 / 1024.0 / 1024.0;
            (eucs_mb.min(200.0) + 50.0, false)
        },
        _ => (100.0, false),
    };
    let base_db_mb = stats.estimated_db_ram_bytes as f64 / 1024.0 / 1024.0;
    
    let recommended_threads = if is_lock_bound {
        1
    } else {
        let remaining_budget = (budget_mb as f64) - base_db_mb;
        if remaining_budget > 0.0 {
            let max_possible = (remaining_budget / ram_per_thread_mb).floor() as usize;
            max_possible.max(1).min(physical_cores)
        } else {
            1
        }
    };

    let user_threads = match cli_threads {
        Some(t) => {
            println!("Threads: {} (auto-selected from CLI)", if t == 0 { "Auto-detect".to_string() } else { t.to_string() });
            if is_lock_bound && t > 1 {
                println!("[WARNING] {} is lock-bound. You requested {}, but extreme BufferPool contention will occur.", algorithm, t);
            } else if t > recommended_threads {
                println!("\n[PREDICTOR WARNING]");
                println!("↳ Math suggests a maximum of {} threads for your {} MB budget.", recommended_threads, budget_mb);
                println!("↳ Risk of Out-Of-Memory or extreme paging thrashing is HIGH!\n");
            }
            t
        },
        None => {
            if is_lock_bound {
                println!("\n[Air-HUIM Concurrency Predictor]");
                println!("↳ Dataset Size:  {:.1} MB", file_mb);
                println!("↳ Algorithm is Tree-based Lock-Bound");
                println!("Threads: 1 (forced sequential for {} to prevent Lock contention)", algorithm);
                1
            } else {
                println!("\n[Air-HUIM Concurrency Predictor]");
                println!("↳ Dataset Size:  {:.1} MB", file_mb);
                println!("↳ Base RAM Cost: {:.1} MB", base_db_mb);
                println!("↳ Per-Thread:    ~{:.1} MB", ram_per_thread_mb);
                println!("↳ Safe Threads:  {} (out of {} physical cores)\n", recommended_threads, physical_cores);
                
                Input::with_theme(&theme)
                    .with_prompt("Threads to use (0 = use Safe Threads)")
                    .default(0)
                    .interact_text()
                    .unwrap()
            }
        }
    };

    let threads = if user_threads == 0 { recommended_threads } else { user_threads };

    let output = cli_output.unwrap_or_else(|| PathBuf::from("output.txt"));
    let chunk_store = cli_chunk_store.unwrap_or_else(|| PathBuf::from("chunks"));
    let prefetch = cli_prefetch.unwrap_or(false);

    let min_length = cli_min_length.unwrap_or(1);
    let max_length = cli_max_length.unwrap_or(usize::MAX);

    println!("\n[Starting Mining Process...]\n");

    run_mining(dataset, algorithm, min_utility, budget_mb, output, chunk_store, prefetch, threads, top_k, min_length, max_length, stats);
}

fn run_mining(
    dataset: PathBuf,
    algorithm: String,
    min_utility: i64,
    budget_mb: usize,
    output: PathBuf,
    chunk_store: PathBuf,
    prefetch: bool,
    threads: usize,
    top_k: Option<u64>,
    min_length: usize,
    max_length: usize,
    stats: pocket_data_mining::mining::DatasetStats,
) {
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    use pocket_data_mining::{
        buffer_pool::pool::BufferPool,
        storage::{chunk_store::ChunkStore, FileChunkStore},
        progress::MiningProgress,
        tui::run_tui,
        mining::DataSource,
    };

    std::fs::create_dir_all(&chunk_store).unwrap();
    
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap_or(());

    let store: Arc<dyn ChunkStore + Send + Sync> = Arc::new(
        FileChunkStore::new(&chunk_store, true).unwrap()
    );
    let pool = BufferPool::new_arc(
        budget_mb * 1024 * 1024,
        Arc::clone(&store) as Arc<dyn ChunkStore>,
        Box::new(pocket_data_mining::buffer_pool::eviction::MiningAwarePolicy::new(
            pocket_data_mining::buffer_pool::eviction::mining_aware::EvictionWeights::default()
        )),
    );

    let guard = Arc::new(pocket_data_mining::mining::MemoryGuard::new(
        budget_mb * 1024 * 1024,
        Arc::clone(&store) as Arc<dyn ChunkStore + Send + Sync>,
    ));

    let progress = Arc::new(MiningProgress::new());
    let mut ctx = pocket_data_mining::mining::MiningContext::new(
        Arc::clone(&pool),
        Arc::clone(&store) as Arc<dyn ChunkStore + Send + Sync>,
        Arc::clone(&progress),
        min_utility,
        output,
        top_k,
        threads,
        min_length,
        max_length,
        guard,
        stats,
    );
    
    let mut algo_box: Box<dyn HuimAlgorithm> = match algorithm.as_str() {
        "efim" => Box::new(Efim::new()),
        "efim-closed" => Box::new(EfimClosed::new()),
        "haui-miner" => Box::new(HauiMiner::new()),
        "fhm" => Box::new(Fhm::new(prefetch)),
        "tku" => Box::new(pocket_data_mining::mining::algorithms::tku::Tku::new(prefetch)),
        "tko" => Box::new(pocket_data_mining::mining::algorithms::tko::Tko::new(prefetch)),
        "rept" => Box::new(pocket_data_mining::mining::algorithms::rept::Rept::new(prefetch)),
        "fhm+" | "fhm-plus" => Box::new(pocket_data_mining::mining::algorithms::fhm_plus::FhmPlus::new(prefetch)),
        "hui-miner" => Box::new(pocket_data_mining::mining::algorithms::hui_miner::HuiMiner::new(prefetch)),
        "two-phase" => Box::new(TwoPhase::new()),
        "ihup" => Box::new(Ihup::new()),
        "hup-tree" => Box::new(HupTree::new()),
        "hui-trie" => Box::new(HuiTrie::new()),
        "up-growth" => Box::new(UpGrowth::new()),
        "up-growth-plus" | "up-growth+" => Box::new(pocket_data_mining::mining::algorithms::up_growth::UpGrowthPlus::new()),
        "hup-miner" => Box::new(pocket_data_mining::mining::algorithms::hup_miner::HupMiner::new(prefetch)),
        "mhuiminer" | "mhui-miner" => Box::new(pocket_data_mining::mining::algorithms::mhui_miner::MHuiMiner::new(prefetch)),
        "huim-mmu" => Box::new(HuimMmu::new(prefetch)),
        "shuim" => Box::new(Shuim::new(prefetch)),
        "incfhm" => Box::new(IncFhm::new(prefetch)),
        "huim-ga" => Box::new(pocket_data_mining::mining::algorithms::huim_ga::HuimGa::new(prefetch)),
        "huim-bpso" => Box::new(pocket_data_mining::mining::algorithms::huim_bpso::HuimBpso::new(prefetch)),
        "mhui-aco" => Box::new(pocket_data_mining::mining::algorithms::mhui_aco::MhuiAco::new(prefetch)),
        _ => {
            println!("Unknown algorithm: {}", algorithm);
            std::process::exit(1);
        }
    };

    let done = Arc::new(AtomicBool::new(false));
    
    let tui_progress = Arc::clone(&progress);
    let tui_pool = Arc::clone(&pool);
    let tui_done = Arc::clone(&done);
    let tui_thread = std::thread::spawn(move || {
        let _ = run_tui(tui_progress, tui_pool, tui_done);
    });

    match algo_box.run(DataSource::file(&dataset), &mut ctx) {
        Ok(count) => {
            done.store(true, Ordering::Relaxed);
            let _ = tui_thread.join();
            println!("Mining complete! Found {} HUIs.", count);
        }
        Err(e) => {
            done.store(true, Ordering::Relaxed);
            let _ = tui_thread.join();
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
