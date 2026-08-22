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
        Self { pool, store, progress, min_utility, output_path, k, threads, chunk_bytes, min_length, max_length, guard, stats }
    }

    /// Compute how many 1-itemset utility lists can fit in the chunk budget.
    /// avg_ul_bytes: estimated average size of one utility list body in bytes.
    pub fn items_per_chunk(&self, avg_ul_bytes: usize) -> usize {
        if avg_ul_bytes == 0 { return usize::MAX; }
        let n = self.chunk_bytes / avg_ul_bytes;
        n.max(1) // always at least 1
    }

    /// Apply OS safety net — cap Buffer Pool budget to (available_ram - 500MB).
    pub fn apply_os_safety_net(&self) {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();
        let available = sys.available_memory();
        let safety: u64 = 500 * 1024 * 1024;
        let safe = available.saturating_sub(safety) as usize;
        let requested = self.pool.budget_bytes();
        let final_budget = requested.min(safe);
        self.pool.set_budget(final_budget);
        self.progress.set_stage(&format!(
            "DFS (Budget: {:.0}MB)",
            final_budget as f64 / 1024.0 / 1024.0
        ));
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
            // the Rayon threads will pause and wait, capping RAM overhead exactly.
            let (tx_hui, rx_hui) = mpsc::bounded::<(Vec<crate::types::ItemId>, crate::types::Utility)>(100_000);
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
