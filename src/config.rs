#![allow(dead_code)]

/// Configurable memory budget for the buffer pool.
#[derive(Debug, Clone, Copy)]
pub struct MemoryBudget {
    pub bytes: usize,
}

impl MemoryBudget {
    pub fn gb(n: usize) -> Self { Self { bytes: n * 1024 * 1024 * 1024 } }
    pub fn mb(n: usize) -> Self { Self { bytes: n * 1024 * 1024 } }
}

/// Top-level experiment configuration.
#[derive(Debug, Clone)]
pub struct ExperimentConfig {
    pub budget: MemoryBudget,
    pub min_utility: i64,
    pub page_size_bytes: usize,
    pub chunk_store_root: std::path::PathBuf,
    pub output_path: std::path::PathBuf,
}

impl Default for ExperimentConfig {
    fn default() -> Self {
        Self {
            budget: MemoryBudget::gb(1),
            min_utility: 1000,
            page_size_bytes: 64 * 1024,
            chunk_store_root: std::path::PathBuf::from("chunks"),
            output_path: std::path::PathBuf::from("output.txt"),
        }
    }
}
