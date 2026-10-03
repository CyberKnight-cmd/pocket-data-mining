use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;
use crate::types::PageId;
use crate::storage::chunk_store::ChunkStore;
use crate::storage::page_layout::PageFlags;

/// Global memory budget enforcer — the single ledger for the whole process.
///
/// Every byte that scales with the dataset is accounted here: native algorithm
/// structures (via `Reservation`s) AND buffer-pool frames (the pool charges this
/// ledger when it is attached with `BufferPool::attach_guard`). So the budget the
/// user types is the budget for everything, not "N MB for the pool + N MB native".
///
/// Native structures may only use `budget - pool_reserve` bytes. The reserve keeps
/// room for the pool to cache spilled pages; without it, native data could fill the
/// budget and force every spilled page straight to disk.
pub struct MemoryGuard {
    used: AtomicUsize,
    peak: AtomicUsize,
    budget: AtomicUsize,
    pool_reserve: AtomicUsize,
    store: Arc<dyn ChunkStore + Send + Sync>,
    /// Frees cached (re-loadable) memory on demand — the buffer pool registers itself
    /// here so active native structures take priority over cached spill pages.
    reclaimer: OnceLock<Box<dyn Fn(usize) -> usize + Send + Sync>>,
}

/// Bytes kept free for the buffer pool's working set (25% of the budget, max 256 MB).
fn default_pool_reserve(budget: usize) -> usize {
    (budget / 4).min(256 * 1024 * 1024)
}

impl MemoryGuard {
    pub fn new(budget: usize, store: Arc<dyn ChunkStore + Send + Sync>) -> Self {
        Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            budget: AtomicUsize::new(budget),
            pool_reserve: AtomicUsize::new(default_pool_reserve(budget)),
            store,
            reclaimer: OnceLock::new(),
        }
    }

    #[inline]
    fn bump_peak(&self, now: usize) {
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    #[inline]
    fn try_alloc_below(&self, bytes: usize, limit: usize) -> bool {
        let mut cur = self.used.load(Ordering::Relaxed);
        loop {
            let next = match cur.checked_add(bytes) {
                Some(n) if n <= limit => n,
                _ => return false,
            };
            match self.used.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => { self.bump_peak(next); return true; }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Try to reserve `bytes` against the full budget. Used by the buffer pool.
    pub fn try_alloc(&self, bytes: usize) -> bool {
        self.try_alloc_below(bytes, self.budget())
    }

    /// Try to reserve `bytes` for a native (non-pool) structure. Leaves the pool reserve
    /// free; if there is no room, asks the reclaimer (buffer pool) to evict cached pages.
    pub fn try_alloc_native(&self, bytes: usize) -> bool {
        let limit = self.native_limit();
        if self.try_alloc_below(bytes, limit) { return true; }
        if let Some(reclaim) = self.reclaimer.get() {
            let need = (self.used() + bytes).saturating_sub(limit);
            if need > 0 && reclaim(need) > 0 {
                return self.try_alloc_below(bytes, limit);
            }
        }
        false
    }

    /// Register a function that frees up to `n` bytes of cached memory and returns how
    /// many it freed. Only the first registration takes effect.
    pub fn set_reclaimer(&self, f: Box<dyn Fn(usize) -> usize + Send + Sync>) {
        let _ = self.reclaimer.set(f);
    }

    /// Force-allocate bytes (for tracking data that must exist regardless, e.g. a
    /// transient copy of a page being read). Can push `used` past the budget.
    pub fn force_alloc(&self, bytes: usize) {
        let now = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.bump_peak(now);
    }

    /// Release `bytes` back to the budget.
    pub fn free(&self, bytes: usize) {
        let prev = self.used.fetch_sub(bytes, Ordering::Relaxed);
        debug_assert!(prev >= bytes, "MemoryGuard::free underflow ({} < {})", prev, bytes);
    }

    /// RAII reservation for a native structure, or None if it would exceed the native limit.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        if self.try_alloc_native(bytes) {
            Some(Reservation { guard: Arc::clone(self), bytes })
        } else {
            None
        }
    }

    /// RAII reservation that always succeeds (accounted even if over budget).
    /// Use only for small, bounded, unavoidable allocations.
    pub fn reserve_force(self: &Arc<Self>, bytes: usize) -> Reservation {
        self.force_alloc(bytes);
        Reservation { guard: Arc::clone(self), bytes }
    }

    /// How many bytes are currently allocated.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Highest `used` value observed.
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// How many bytes remain before hitting the ceiling.
    pub fn remaining(&self) -> usize {
        self.budget().saturating_sub(self.used())
    }

    /// How many bytes a native structure could still reserve.
    pub fn native_remaining(&self) -> usize {
        self.native_limit().saturating_sub(self.used())
    }

    /// The total budget.
    pub fn budget(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
    }

    pub fn native_limit(&self) -> usize {
        self.budget().saturating_sub(self.pool_reserve.load(Ordering::Relaxed))
    }

    /// Change the budget (e.g. OS safety net). The pool reserve is rescaled.
    pub fn set_budget(&self, bytes: usize) {
        self.budget.store(bytes, Ordering::Relaxed);
        self.pool_reserve.store(default_pool_reserve(bytes), Ordering::Relaxed);
    }

    /// Serialize `data` to ChunkStore and return a PageId handle.
    pub fn spill(&self, data: &[u8]) -> io::Result<PageId> {
        let page_id = self.store.next_page_id();
        self.store.write_page(page_id, data, PageFlags::empty())?;
        Ok(page_id)
    }

    /// Load spilled data back from ChunkStore.
    pub fn load(&self, page_id: PageId) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.store.read_page(page_id, &mut buf)?;
        Ok(buf)
    }

    /// Load and delete — one-shot retrieval.
    pub fn load_and_delete(&self, page_id: PageId) -> io::Result<Vec<u8>> {
        let buf = self.load(page_id)?;
        let _ = self.store.delete_page(page_id);
        Ok(buf)
    }
}

/// Bytes reserved in a `MemoryGuard`; released when dropped.
/// Using this instead of paired `try_alloc`/`free` calls makes it impossible to
/// free bytes that were never reserved (or to forget to free them).
pub struct Reservation {
    guard: Arc<MemoryGuard>,
    bytes: usize,
}

impl Reservation {
    pub fn bytes(&self) -> usize { self.bytes }

    /// Try to grow this reservation by `extra` bytes (native limit applies).
    pub fn try_grow(&mut self, extra: usize) -> bool {
        if self.guard.try_alloc_native(extra) {
            self.bytes += extra;
            true
        } else {
            false
        }
    }

    /// Grow unconditionally (accounted even if over budget).
    pub fn grow_force(&mut self, extra: usize) {
        self.guard.force_alloc(extra);
        self.bytes += extra;
    }

    /// Release part of the reservation.
    pub fn shrink(&mut self, less: usize) {
        let less = less.min(self.bytes);
        self.guard.free(less);
        self.bytes -= less;
    }

    /// Adjust the reservation to exactly `bytes`, growing unconditionally if needed.
    pub fn resize_force(&mut self, bytes: usize) {
        if bytes > self.bytes { self.grow_force(bytes - self.bytes) } else { self.shrink(self.bytes - bytes) }
    }

    pub fn guard(&self) -> &Arc<MemoryGuard> { &self.guard }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.guard.free(self.bytes);
        }
    }
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Reservation({} B)", self.bytes)
    }
}

/// Approximate heap bytes of a `Vec<T>` with the given capacity (incl. allocator overhead).
#[inline]
pub fn vec_bytes<T>(cap: usize) -> usize {
    if cap == 0 { 0 } else { cap * std::mem::size_of::<T>() + 16 }
}

/// Approximate heap bytes of a hashbrown `HashMap<K, V>` with the given capacity.
#[inline]
pub fn map_bytes<K, V>(cap: usize) -> usize {
    if cap == 0 { return 0; }
    let buckets = (cap * 8 / 7).next_power_of_two().max(4);
    buckets * (std::mem::size_of::<(K, V)>() + 1) + 16
}

/// Make the allocator hand freed memory back to the OS so that RSS tracks the ledger.
///
/// glibc keeps freed memory in per-thread arenas (up to 8 x cores of them) and only
/// trims above 128 KB of free top-of-heap by default; with a churning DFS that leaves
/// RSS far above what is actually live. Call once at startup, before spawning threads.
pub fn tune_allocator_for_budget() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, std::env::var("AIR_HUIM_MALLOC_ARENAS").ok().and_then(|v| v.parse().ok()).unwrap_or(1));
        libc::mallopt(libc::M_TRIM_THRESHOLD, 128 * 1024);
        libc::mallopt(libc::M_MMAP_THRESHOLD, 256 * 1024);
    }
}

/// Return free heap memory to the OS (after a phase that freed a lot).
pub fn release_free_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Resident set size of this process in bytes (Linux: /proc/self/status VmRSS).
pub fn current_rss_bytes() -> usize {
    proc_status_kb("VmRSS:").map(|kb| kb * 1024).unwrap_or(0)
}

/// Peak resident set size of this process in bytes (Linux: VmHWM).
pub fn peak_rss_bytes() -> usize {
    proc_status_kb("VmHWM:").map(|kb| kb * 1024).unwrap_or(0)
}

fn proc_status_kb(key: &str) -> Option<usize> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    s.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard(budget: usize) -> Arc<MemoryGuard> {
        let store = Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().keep(), false,
        ).unwrap());
        Arc::new(MemoryGuard::new(budget, store))
    }

    #[test]
    fn reservation_frees_on_drop() {
        let g = guard(1000);
        {
            let _r = g.reserve(100).unwrap();
            assert_eq!(g.used(), 100);
        }
        assert_eq!(g.used(), 0);
    }

    #[test]
    fn native_limit_leaves_pool_reserve() {
        let g = guard(1000); // reserve = 250
        assert!(g.reserve(800).is_none());
        let _r = g.reserve(750).unwrap();
        assert!(g.try_alloc(250)); // pool may still use the reserve
        assert!(!g.try_alloc(1));
    }

    #[test]
    fn failed_reservation_does_not_change_usage() {
        let g = guard(1000);
        let _a = g.reserve(700).unwrap();
        assert!(g.reserve(100).is_none());
        assert_eq!(g.used(), 700);
    }
}
