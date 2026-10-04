use std::path::PathBuf;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResult {
    pub budget_bytes:      usize,
    pub dataset_path:      PathBuf,
    pub wall_time_secs:    f64,
    pub peak_rss_bytes:    usize,
    pub buffer_pool_bytes: usize,
    pub cache_hit_rate:    f64,
    pub cache_miss_rate:   f64,
    pub page_loads:        u64,
    pub evictions:         u64,
    pub prefetch_issued:   u64,
    pub prefetch_useful:   u64,
    pub prefetch_wasted:   u64,
    pub bytes_read:        u64,
    pub bytes_written:     u64,
    pub hui_count:         u64,
    pub exact:             bool,
}

/// Peak process RSS (Resident Set Size) in bytes so far (Linux VmHWM; 0 elsewhere).
pub fn measure_peak_rss() -> usize {
    crate::mining::core::memory_guard::peak_rss_bytes()
}
