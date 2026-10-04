use std::sync::Arc;
use std::path::PathBuf;
use crate::{
    buffer_pool::pool::BufferPool,
    storage::chunk_store::ChunkStore,
    progress::MiningProgress,
    mining::core::result_writer::ResultWriter,
};

pub struct MiningContext {
    pub pool: Arc<BufferPool>,
    pub store: Arc<dyn ChunkStore + Send + Sync>,
    pub progress: Arc<MiningProgress>,
    pub min_utility: i64,
    pub output_path: PathBuf,
    pub k: Option<u64>,
    pub threads: usize,
    pub chunk_bytes: usize,
    pub min_length: usize,
    pub max_length: usize,
    pub guard: Arc<super::MemoryGuard>,
    pub stats: super::DatasetStats,
    /// What to do with a new utility list when the budget has no room: spill it, or drop it
    /// and recompute it from its parents (cost model). From AIR_HUIM_REMAT (auto|off|always).
    pub remat: crate::mining::components::ul_join::RematMode,
    /// Tree miners: partition projection when the global tree would not fit (auto), always,
    /// or never. From AIR_HUIM_TREE_PARTITION.
    pub tree_partition: crate::mining::components::tree_partition::PartitionMode,
}

impl MiningContext {
    pub fn new(
        pool: Arc<BufferPool>,
        store: Arc<dyn ChunkStore + Send + Sync>,
        progress: Arc<MiningProgress>,
        min_utility: i64,
        output_path: PathBuf,
        k: Option<u64>,
        threads: usize,
        min_length: usize,
        max_length: usize,
        guard: Arc<super::MemoryGuard>,
        stats: super::DatasetStats,
    ) -> Self {
        let chunk_bytes = pool.budget_bytes() / 4;
        Self { pool, store, progress, min_utility, output_path, k, threads, chunk_bytes, min_length, max_length, guard, stats,
               remat: crate::mining::components::ul_join::RematMode::from_env(),
               tree_partition: crate::mining::components::tree_partition::PartitionMode::from_env() }
    }

    /// Compute how many 1-itemset utility lists can fit in the chunk budget.
    /// avg_ul_bytes: estimated average size of one utility list body in bytes.
    pub fn items_per_chunk(&self, avg_ul_bytes: usize) -> usize {
        if avg_ul_bytes == 0 { return usize::MAX; }
        let n = self.chunk_bytes / avg_ul_bytes;
        n.max(1) // always at least 1
    }

    /// Apply OS safety net — cap the whole budget (ledger and pool) to what the OS can
    /// actually give us: (available RAM + what we already hold) minus a margin of 20% of
    /// that, at most 500 MB. (A fixed 500 MB margin made any budget impossible on a busy or
    /// small machine: with 369 MB available it capped the budget at zero.)
    pub fn apply_os_safety_net(&self) {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let available = sys.available_memory() as usize + self.guard.used();
        let safety = (available / 5).min(500 * 1024 * 1024);
        let safe = available.saturating_sub(safety);
        if safe < self.guard.budget() {
            eprintln!("[safety-net] only {:.0} MB of RAM is available; ledger budget lowered from {:.0} MB to {:.0} MB",
                      available as f64 / 1048576.0, self.guard.budget() as f64 / 1048576.0,
                      safe.max(self.guard.used()) as f64 / 1048576.0);
            self.guard.set_budget(safe.max(self.guard.used()));
        }
        if safe < self.pool.budget_bytes() {
            self.pool.set_budget(safe);
        }
    }

    /// Admission control. `fixed` = bytes the run needs whatever the budget (headers, maps,
    /// minimum buffers); `per_thread` = constant working set of one worker (streaming join
    /// chunks, spill page, scratch). Everything else is data the engine can spill.
    ///
    /// * refuses the run (clear error, before any mining) if even one thread cannot fit;
    /// * otherwise lowers the thread count until the per-thread working sets use at most half
    ///   of what is free, leaving the rest for data that would otherwise be spilled.
    /// Returns the thread count to use (also stored in `self.threads`).
    /// `estimate(ledger_budget)` returns `(fixed, per_thread)` for a given ledger budget.
    pub fn admit(&mut self, name: &str, estimate: &dyn Fn(usize) -> (usize, usize)) -> std::io::Result<usize> {
        const MB: f64 = 1048576.0;
        let (fixed, per_thread) = estimate(self.guard.budget());
        let free = self.guard.native_limit().saturating_sub(self.guard.used());
        if fixed + per_thread > free {
            // The user-facing budget that would fit (same margin and pool-reserve rules).
            let one_worker = |ledger: usize| { let (f, p) = estimate(ledger); f + p };
            let need = super::memory_guard::min_budget_for(&one_worker, self.guard.used()) as f64 * MB;
            return Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, format!(
                "{}: the memory budget is too small for this dataset: it needs about {:.0} KB that cannot be \
                 spilled ({:.0} KB fixed + {:.0} KB for one worker) but only {:.0} KB is free \
                 ({:.0} KB already in use, ledger budget {:.0} KB). Use at least -b {:.0}.",
                name, (fixed + per_thread) as f64 / 1024.0, fixed as f64 / 1024.0, per_thread as f64 / 1024.0,
                free as f64 / 1024.0, self.guard.used() as f64 / 1024.0, self.guard.budget() as f64 / 1024.0,
                (need / MB).ceil())));
        }
        let requested = self.threads.max(1);
        let room = (free / 2).saturating_sub(fixed);
        let fit = if per_thread == 0 { requested } else { (room / per_thread).max(1) };
        let threads = requested.min(fit);
        if threads < requested {
            let msg = format!("{}: budget allows {} worker thread(s) ({:.1} MB each); using {} instead of {}",
                              name, fit, per_thread as f64 / MB, threads, requested);
            eprintln!("[admission] {}", msg);
            self.progress.set_stage(&msg);
        }
        self.threads = threads;
        Ok(threads)
    }

    pub fn open_writer(&self) -> std::io::Result<ResultWriter> {
        ResultWriter::new(&self.output_path)
    }

    /// Orchestrates top-level task execution across either a sequential loop or a Rayon
    /// work-stealing thread pool, completely hiding the parallelism from the algorithm.
    pub fn execute_tasks<T, F>(&self, tasks: Vec<T>, processor: F)
    where
        T: Send + Sync,
        F: Fn(T, &mut WriterProxy) + Sync + Send,
    {
        if self.threads > 1 {
            use crossbeam_channel as mpsc;
            use rayon::prelude::*;

            // BOUNDED queue prevents RAM explosion! If the disk writer is too slow,
            // the Rayon threads will pause and wait. Capacity scales with the budget
            // (~1/64 of it, at ~96 bytes per queued itemset) and is accounted up front.
            const MSG_BYTES: usize = 96;
            let cap = (self.guard.budget() / 64 / MSG_BYTES).clamp(64, 100_000);
            let _queue_res = self.guard.reserve_force(cap * MSG_BYTES);
            let (tx_hui, rx_hui) = mpsc::bounded::<(Vec<crate::types::ItemId>, crate::types::Utility)>(cap);
            let output_path = self.output_path.clone();
            
            let writer_thread = std::thread::spawn(move || {
                let mut writer = ResultWriter::new(&output_path).unwrap();
                let mut count = 0u64;
                while let Ok((itemset, utility)) = rx_hui.recv() {
                    writer.write_hui(&itemset, utility).ok();
                    count += 1;
                }
                writer.finalize().map(|_| count)
            });

            let pool_rayon = rayon::ThreadPoolBuilder::new()
                .num_threads(self.threads)
                .build()
                .unwrap();

            pool_rayon.install(|| {
                tasks.into_par_iter().for_each(|task| {
                    let mut proxy = WriterProxy::Parallel(tx_hui.clone());
                    processor(task, &mut proxy);
                });
            });

            drop(tx_hui);
            writer_thread.join().unwrap().unwrap();
        } else {
            let mut writer = self.open_writer().unwrap();
            for task in tasks {
                let mut proxy = WriterProxy::Sequential(&mut writer);
                processor(task, &mut proxy);
            }
            writer.finalize().unwrap();
        }
    }
}

pub enum WriterProxy<'a> {
    Sequential(&'a mut ResultWriter),
    Parallel(crossbeam_channel::Sender<(Vec<crate::types::ItemId>, crate::types::Utility)>),
}

impl<'a> WriterProxy<'a> {
    pub fn write_hui(&mut self, itemset: &[crate::types::ItemId], utility: crate::types::Utility) -> std::io::Result<()> {
        match self {
            Self::Sequential(w) => w.write_hui(itemset, utility),
            Self::Parallel(tx) => {
                tx.send((itemset.to_vec(), utility)).unwrap();
                Ok(())
            }
        }
    }
}
