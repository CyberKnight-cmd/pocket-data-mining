use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct BufferPoolMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub dirty_flushes: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub peak_bytes: AtomicU64,
}

impl BufferPoolMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dirty_flush(&self) {
        self.dirty_flushes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_read(&self, n: u64) {
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_bytes_written(&self, n: u64) {
        self.bytes_written.fetch_add(n, Ordering::Relaxed);
    }

    pub fn update_peak(&self, n: u64) {
        let mut current = self.peak_bytes.load(Ordering::Relaxed);
        while n > current {
            match self.peak_bytes.compare_exchange_weak(current, n, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(val) => current = val,
            }
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        let total = h + m;
        if total == 0.0 { 0.0 } else { h / total }
    }

    pub fn miss_rate(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        let total = h + m;
        if total == 0.0 { 0.0 } else { m / total }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics() {
        let m = BufferPoolMetrics::new();
        m.record_hit();
        m.record_hit();
        m.record_miss();
        assert_eq!(m.hits.load(Ordering::Relaxed), 2);
        assert_eq!(m.misses.load(Ordering::Relaxed), 1);
        assert_eq!(m.hit_rate(), 2.0 / 3.0);
        assert_eq!(m.miss_rate(), 1.0 / 3.0);
        m.update_peak(100);
        m.update_peak(50);
        assert_eq!(m.peak_bytes.load(Ordering::Relaxed), 100);
    }
}
