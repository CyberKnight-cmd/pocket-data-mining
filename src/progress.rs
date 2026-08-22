use std::sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Mutex};

pub struct MiningProgress {
    pub stage: Mutex<String>,
    pub huis_found: AtomicU64,
    pub current_depth: AtomicUsize,
    pub fast_path_reads: AtomicU64,
    pub fast_path_writes: AtomicU64,
    pub active_prefix: Mutex<String>,
}

impl MiningProgress {
    pub fn new() -> Self {
        Self {
            stage: Mutex::new("Initializing".into()),
            huis_found: AtomicU64::new(0),
            current_depth: AtomicUsize::new(0),
            fast_path_reads: AtomicU64::new(0),
            fast_path_writes: AtomicU64::new(0),
            active_prefix: Mutex::new("[]".into()),
        }
    }

    pub fn set_stage(&self, s: &str) {
        *self.stage.lock().unwrap() = s.to_string();
    }

    pub fn set_active_prefix(&self, p: &[crate::types::ItemId]) {
        *self.active_prefix.lock().unwrap() = format!("{:?}", p);
    }
}
