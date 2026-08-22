use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use crate::{
    buffer_pool::{pool::BufferPool, eviction::LruPolicy},
    mining::Fhm,
    preprocessing::{
        db_reader::DbReader,
        twu_filter::TwuFilter,
    },
    storage::{chunk_store::ChunkStore, FileChunkStore},
};
use super::{
    exactness_checker::{verify_exactness, ExactnessResult},
    metrics_collector::{ExperimentResult, measure_peak_rss},
};

/// Configuration for a single experiment run.
#[derive(Debug, Clone)]
pub struct ExperimentConfig {
    pub budget_bytes: usize,
    pub dataset_path: PathBuf,
    pub min_utility: i64,
    pub chunk_store_root: PathBuf,
    pub output_path: PathBuf,
    pub reference_path: Option<PathBuf>, // if set, verify exactness
    pub enable_prefetch: bool,
}

/// Run a single experiment. Returns a structured result.
pub fn run_experiment(cfg: &ExperimentConfig) -> std::io::Result<ExperimentResult> {
    // 1. Build storage
    std::fs::create_dir_all(&cfg.chunk_store_root)?;
    let store: Arc<dyn ChunkStore + Send + Sync> = Arc::new(
        FileChunkStore::new(&cfg.chunk_store_root, true)?
    );

    // 2. Build buffer pool
    let pool = BufferPool::new_arc(
        cfg.budget_bytes,
        Arc::clone(&store) as Arc<dyn ChunkStore>,
        Box::new(LruPolicy::new()),
    );

    // Precompute stats
    let stats = crate::mining::DatasetStats::precompute(&cfg.dataset_path)?;
    let guard = Arc::new(crate::mining::MemoryGuard::new(
        cfg.budget_bytes,
        Arc::clone(&store) as Arc<dyn ChunkStore + Send + Sync>,
    ));

    // 3. Run FHM
    let progress = Arc::new(crate::progress::MiningProgress::new());
    let mut ctx = crate::mining::MiningContext::new(
        Arc::clone(&pool),
        Arc::clone(&store) as Arc<dyn ChunkStore + Send + Sync>,
        Arc::clone(&progress),
        cfg.min_utility,
        cfg.output_path.clone(),
        None, // top_k
        1,    // threads
        1,    // min_length
        usize::MAX, // max_length
        guard,
        stats,
    );
    let wall_start = Instant::now();
    let mut fhm = Fhm::new(cfg.enable_prefetch);
    use crate::mining::HuimAlgorithm;
    let hui_count = fhm.run(crate::mining::DataSource::file(&cfg.dataset_path), &mut ctx)?;
    let wall_time_secs = wall_start.elapsed().as_secs_f64();

    // 6. Collect metrics
    let m = &pool.metrics;
    let hits = m.hits.load(std::sync::atomic::Ordering::Relaxed);
    let misses = m.misses.load(std::sync::atomic::Ordering::Relaxed);
    let total = hits + misses;
    let cache_hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };

    // 7. Exactness check
    let exact = if let Some(ref_path) = &cfg.reference_path {
        verify_exactness(ref_path, &cfg.output_path)?.exact
    } else {
        true // assumed exact if no reference provided
    };

    Ok(ExperimentResult {
        budget_bytes: cfg.budget_bytes,
        dataset_path: cfg.dataset_path.clone(),
        wall_time_secs,
        peak_rss_bytes: measure_peak_rss(),
        buffer_pool_bytes: pool.used_bytes(),
        cache_hit_rate,
        cache_miss_rate: 1.0 - cache_hit_rate,
        page_loads: misses, // each miss is a page load
        evictions: m.evictions.load(std::sync::atomic::Ordering::Relaxed),
        prefetch_issued: 0,
        prefetch_useful: 0,
        prefetch_wasted: 0,
        bytes_read: m.bytes_read.load(std::sync::atomic::Ordering::Relaxed),
        bytes_written: m.bytes_written.load(std::sync::atomic::Ordering::Relaxed),
        hui_count,
        exact,
    })
}

/// Multi-budget experiment runner.
pub struct ExperimentRunner {
    pub budgets: Vec<usize>,
    pub dataset_path: PathBuf,
    pub min_utility: i64,
    pub output_root: PathBuf,
    pub chunk_store_root: PathBuf,
    pub reference_path: Option<PathBuf>,
}

impl ExperimentRunner {
    pub fn run_all(&self) -> std::io::Result<Vec<ExperimentResult>> {
        let mut results = Vec::new();
        for &budget in &self.budgets {
            let out_path = self.output_root.join(format!("huis_{}mb.txt", budget / (1024*1024)));
            let chunk_root = self.chunk_store_root.join(format!("chunks_{}mb", budget / (1024*1024)));
            let cfg = ExperimentConfig {
                budget_bytes: budget,
                dataset_path: self.dataset_path.clone(),
                min_utility: self.min_utility,
                chunk_store_root: chunk_root,
                output_path: out_path,
                reference_path: self.reference_path.clone(),
                enable_prefetch: false,
            };
            results.push(run_experiment(&cfg)?);
        }
        Ok(results)
    }
}
