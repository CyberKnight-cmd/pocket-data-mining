use std::io;
use crate::types::{ItemId, Utility, PageId, ULEntry, UtilityList, RecomputeFlag};
use smallvec::SmallVec;

use std::sync::Arc;
use crate::buffer_pool::pool::{BufferPool, OwnedPage};
use crate::mining::core::memory_guard::{MemoryGuard, Reservation, vec_bytes};

/// Serialize ULEntry slice to bytes.
pub fn serialize_ul_body(entries: &[ULEntry]) -> Vec<u8> {
    // ULEntry is #[repr(C, packed)] with size 20 bytes
    let mut buf = Vec::with_capacity(entries.len() * 20);
    for e in entries {
        buf.extend_from_slice(&e.tid.to_le_bytes());
        buf.extend_from_slice(&e.iutils.to_le_bytes());
        buf.extend_from_slice(&e.rutils.to_le_bytes());
    }
    buf
}

/// Deserialize ULEntry slice from bytes.
pub fn deserialize_ul_body(bytes: &[u8]) -> Vec<ULEntry> {
    assert_eq!(bytes.len() % 20, 0, "ULEntry bytes must be multiple of 20");
    bytes.chunks_exact(20).map(|chunk| {
        let tid = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
        let iutils = i64::from_le_bytes(chunk[4..12].try_into().unwrap());
        let rutils = i64::from_le_bytes(chunk[12..20].try_into().unwrap());
        ULEntry { tid, iutils, rutils }
    }).collect()
}

/// Lists at least this large get a page of their own; smaller ones are packed.
const OWN_PAGE_BYTES: usize = 64 * 1024;
/// Size of one packed spill page.
const ARENA_PAGE_BYTES: usize = 256 * 1024;

/// Packs small spilled lists into shared pages so a memory-starved search does not
/// create one file (and one pool frame) per tiny list. Single-threaded: create one per
/// task / thread. A packed page is discarded once every list stored in it is dropped.
pub struct SpillArena {
    pool: Arc<BufferPool>,
    inner: std::cell::RefCell<ArenaInner>,
}

struct ArenaInner {
    slot: Option<Arc<OwnedPage>>,
    buf: Vec<u8>,
    _res: Reservation,
}

impl SpillArena {
    pub fn new(pool: &Arc<BufferPool>, guard: &Arc<MemoryGuard>) -> Self {
        Self {
            pool: Arc::clone(pool),
            inner: std::cell::RefCell::new(ArenaInner {
                slot: None,
                buf: Vec::new(),
                _res: guard.reserve_force(ARENA_PAGE_BYTES),
            }),
        }
    }

    fn append(&self, entries: &[ULEntry]) -> io::Result<UlBody> {
        let bytes = entries.len() * 20;
        let mut inner = self.inner.borrow_mut();
        if inner.slot.is_none() || inner.buf.len() + bytes > ARENA_PAGE_BYTES {
            Self::flush_inner(&self.pool, &mut inner)?;
            inner.slot = Some(Arc::new(OwnedPage::reserve_id(&self.pool)));
            inner.buf = Vec::with_capacity(ARENA_PAGE_BYTES);
        }
        let offset = inner.buf.len() as u32;
        inner.buf.extend_from_slice(&serialize_ul_body(entries));
        let slot = Arc::clone(inner.slot.as_ref().unwrap());
        Ok(UlBody::Packed { slot, offset, count: entries.len() as u32 })
    }

    fn flush_inner(pool: &Arc<BufferPool>, inner: &mut ArenaInner) -> io::Result<()> {
        if let Some(slot) = inner.slot.take() {
            // If every list in this page already died, there is nothing to write.
            if Arc::strong_count(&slot) > 1 && !inner.buf.is_empty() {
                pool.insert_page(slot.id, std::mem::take(&mut inner.buf))?;
            }
        }
        inner.buf.clear();
        Ok(())
    }

    /// Write the open page to the pool (needed before other threads read packed lists).
    pub fn flush(&self) -> io::Result<()> {
        Self::flush_inner(&self.pool, &mut self.inner.borrow_mut())
    }

    /// Read a packed list that is still in the open (unwritten) page.
    fn read_open(&self, slot: &Arc<OwnedPage>, offset: u32, count: u32) -> Option<Vec<ULEntry>> {
        let inner = self.inner.borrow();
        let cur = inner.slot.as_ref()?;
        if !Arc::ptr_eq(cur, slot) { return None; }
        let start = offset as usize;
        Some(deserialize_ul_body(&inner.buf[start..start + count as usize * 20]))
    }
}

/// Where utility-list bodies live. Bundles the buffer pool and the memory ledger so a
/// body is kept in RAM only when the budget has room for it.
#[derive(Clone, Copy)]
pub struct BodyAlloc<'a> {
    pub pool: &'a Arc<BufferPool>,
    pub guard: &'a Arc<MemoryGuard>,
    pub arena: Option<&'a SpillArena>,
}

impl<'a> BodyAlloc<'a> {
    pub fn new(pool: &'a Arc<BufferPool>, guard: &'a Arc<MemoryGuard>) -> Self {
        Self { pool, guard, arena: None }
    }

    pub fn with_arena(self, arena: &'a SpillArena) -> Self {
        Self { arena: Some(arena), ..self }
    }

    /// Store entries that must go to the pool (no RAM reservation).
    pub fn spill_body(&self, entries: &[ULEntry]) -> io::Result<UlBody> {
        let bytes = entries.len() * 20;
        match self.arena {
            Some(arena) if bytes < OWN_PAGE_BYTES => arena.append(entries),
            _ => Ok(UlBody::OnDisk(OwnedPage::create(self.pool, serialize_ul_body(entries))?)),
        }
    }

    /// Turn finished entries into a body: in RAM if the budget allows, else spilled.
    pub fn make_body(&self, mut entries: Vec<ULEntry>) -> io::Result<UlBody> {
        entries.shrink_to_fit();
        if entries.is_empty() {
            return Ok(UlBody::InMemory(entries, None));
        }
        match self.guard.reserve(vec_bytes::<ULEntry>(entries.capacity())) {
            Some(r) => Ok(UlBody::InMemory(entries, Some(r))),
            None => self.spill_body(&entries),
        }
    }

    /// Read access to a body. Spilled bodies are read in place from their pinned pool
    /// page (zero-copy: the page is already charged to the ledger by the pool).
    pub fn view<'b>(&self, body: &'b UlBody) -> io::Result<BodyView<'b>> {
        match body {
            UlBody::InMemory(entries, _) => Ok(BodyView::Borrowed(entries)),
            UlBody::OnDisk(page) => {
                let pin = page.pin()?;
                let n = pin.len() / 20;
                Ok(BodyView::pinned(pin, 0, n, self.guard))
            }
            UlBody::Packed { slot, offset, count } => {
                if let Some(v) = self.arena.and_then(|a| a.read_open(slot, *offset, *count)) {
                    let r = self.guard.reserve_force(vec_bytes::<ULEntry>(v.len()));
                    return Ok(BodyView::Loaded(v, r));
                }
                let pin = slot.pin()?;
                Ok(BodyView::pinned(pin, *offset as usize, *count as usize, self.guard))
            }
        }
    }
}

/// Container for utility-list body data that may be in RAM or on disk.
/// Dropping a body releases its budget (in RAM) or its page / page share (spilled).
#[derive(Debug)]
pub enum UlBody {
    /// Entries kept in RAM, with the budget reservation that pays for them.
    InMemory(Vec<ULEntry>, Option<Reservation>),
    /// Stored as its own buffer-pool page.
    OnDisk(OwnedPage),
    /// Stored inside a shared packed page (see `SpillArena`).
    Packed { slot: Arc<OwnedPage>, offset: u32, count: u32 },
}

impl UlBody {
    pub fn page_id(&self) -> PageId {
        match self {
            UlBody::InMemory(..) => 0,
            UlBody::OnDisk(p) => p.id,
            UlBody::Packed { slot, .. } => slot.id,
        }
    }
}

/// Borrowed, pinned-in-place, or temporarily loaded body entries.
pub enum BodyView<'a> {
    Borrowed(&'a [ULEntry]),
    /// Entries read directly from a pinned pool page: (pin, byte offset, entry count).
    Pinned(crate::buffer_pool::frame::PinGuard, usize, usize),
    Loaded(Vec<ULEntry>, Reservation),
}

impl BodyView<'_> {
    fn pinned(pin: crate::buffer_pool::frame::PinGuard, offset: usize, count: usize, guard: &Arc<MemoryGuard>) -> Self {
        // The on-page format is the packed little-endian ULEntry layout, so on little-endian
        // targets (x86_64, aarch64) the bytes can be used in place.
        if cfg!(target_endian = "little") {
            debug_assert!(offset + count * 20 <= pin.len());
            BodyView::Pinned(pin, offset, count)
        } else {
            let v = deserialize_ul_body(&pin[offset..offset + count * 20]);
            BodyView::Loaded(v, guard.reserve_force(vec_bytes::<ULEntry>(count)))
        }
    }
}

impl std::ops::Deref for BodyView<'_> {
    type Target = [ULEntry];
    fn deref(&self) -> &[ULEntry] {
        match self {
            BodyView::Borrowed(s) => s,
            // SAFETY: ULEntry is repr(C, packed) (align 1, 20 bytes) and the page holds
            // `count` entries serialized little-endian at `offset`; only constructed on
            // little-endian targets. The pin keeps the frame resident and unmoved.
            BodyView::Pinned(pin, off, n) => unsafe {
                std::slice::from_raw_parts(pin.as_ptr().add(*off) as *const ULEntry, *n)
            },
            BodyView::Loaded(v, _) => v,
        }
    }
}

/// Join utility lists to produce a new extended utility list.
///
/// FHM 3-pointer merge:
/// - prefix_ul: UL of the prefix itemset (for 1-itemsets, this is UL({}) = all tids)
/// - px_ul: UL of prefix + X
/// - py_ul: UL of prefix + Y
///
/// For each tid in px_ul ∩ py_ul:
///   new_entry.tid = tid
///   new_entry.iutils = px_entry.iutils + py_entry.iutils - prefix_entry.iutils
///   new_entry.rutils = py_entry.rutils
///
/// For 1-itemset joins (prefix is empty), prefix_entry.iutils = 0.
///
/// Returns the new UtilityList header and body (UlBody).
pub fn join_utility_lists(
    itemset: SmallVec<[ItemId; 8]>,
    prefix_body: &[ULEntry],  // empty slice if prefix is the empty set
    px_body: &[ULEntry],
    py_body: &[ULEntry],
    alloc: BodyAlloc,
) -> io::Result<(UtilityList, UlBody)> {
    Ok(join_utility_lists_la(itemset, prefix_body, px_body, py_body, alloc, None)?
        .expect("join without LA-prune always completes"))
}

/// LA-prune (HUP-Miner / HUI-Miner*): the upper bound of P·x·y starts at the bound of
/// P·x and loses each P·x transaction that has no P·y entry. Once it drops below the
/// threshold the join is abandoned (returns `None`).
#[derive(Clone, Copy)]
pub struct LaPrune {
    /// Upper bound of P·x (sum over its entries of `bound_of(entry)`).
    pub start: Utility,
    pub threshold: Utility,
    /// Average-utility mode: an entry's bound is its `rutils` (the transaction's max item
    /// utility); otherwise it is `iutils + rutils`.
    pub average: bool,
}

pub fn join_utility_lists_la(
    itemset: SmallVec<[ItemId; 8]>,
    prefix_body: &[ULEntry],
    px_body: &[ULEntry],
    py_body: &[ULEntry],
    alloc: BodyAlloc,
    la: Option<LaPrune>,
) -> io::Result<Option<(UtilityList, UlBody)>> {
    let mut bound = la.map_or(0, |l| l.start);
    let lose = |e: &ULEntry| -> Utility {
        match la { Some(l) if l.average => e.rutils, Some(_) => e.iutils + e.rutils, None => 0 }
    };
    // The merge buffer is transient but can be large; account for it while it exists.
    let cap = px_body.len().min(py_body.len());
    let scratch = alloc.guard.reserve_force(vec_bytes::<ULEntry>(cap));
    let mut result: Vec<ULEntry> = Vec::with_capacity(cap);
    let mut sum_iutils: Utility = 0;
    let mut sum_rutils: Utility = 0;

    if prefix_body.is_empty() {
        // 1-itemset join: no prefix entries, just find common tids
        let mut i = 0usize;
        let mut j = 0usize;
        while i < px_body.len() && j < py_body.len() {
            let px_tid = px_body[i].tid;
            let py_tid = py_body[j].tid;
            match px_tid.cmp(&py_tid) {
                std::cmp::Ordering::Equal => {
                    let tid = px_body[i].tid;
                    let iutils = px_body[i].iutils + py_body[j].iutils;
                    let rutils = py_body[j].rutils;
                    sum_iutils += iutils;
                    sum_rutils += rutils;
                    result.push(ULEntry { tid, iutils, rutils });
                    i += 1; j += 1;
                }
                std::cmp::Ordering::Less => {
                    if let Some(l) = la {
                        bound -= lose(&px_body[i]);
                        if bound < l.threshold { return Ok(None); }
                    }
                    i += 1;
                }
                std::cmp::Ordering::Greater => { j += 1; }
            }
        }
    } else {
        // k-itemset join: 3-pointer merge
        let mut p = 0usize;
        let mut i = 0usize;
        let mut j = 0usize;
        while i < px_body.len() && j < py_body.len() {
            let px_tid = px_body[i].tid;
            let py_tid = py_body[j].tid;
            if px_tid != py_tid {
                if px_tid < py_tid {
                    if let Some(l) = la {
                        bound -= lose(&px_body[i]);
                        if bound < l.threshold { return Ok(None); }
                    }
                    i += 1;
                } else {
                    j += 1;
                }
                continue;
            }
            let tid = px_body[i].tid;
            // Advance prefix pointer to this tid
            while p < prefix_body.len() && { let ptid = prefix_body[p].tid; ptid < tid } { p += 1; }
            let prefix_iutils = if p < prefix_body.len() && { let ptid = prefix_body[p].tid; ptid == tid } {
                prefix_body[p].iutils
            } else {
                0
            };
            let iutils = px_body[i].iutils + py_body[j].iutils - prefix_iutils;
            let rutils = py_body[j].rutils;
            sum_iutils += iutils;
            sum_rutils += rutils;
            result.push(ULEntry { tid, iutils, rutils });
            i += 1; j += 1;
        }
    }

    let len = result.len() as u32;
    drop(scratch); // make_body re-accounts the (shrunk) buffer itself
    let body = alloc.make_body(result)?;
    let page_id = body.page_id();

    let ul = UtilityList {
        itemset,
        sum_iutils,
        sum_rutils,
        len,
        page_id,
        resident: true,
        recompute: if page_id == 0 { RecomputeFlag::Recomputable } else { RecomputeFlag::Materialized },
    };

    Ok(Some((ul, body)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn entry(tid: u32, i: i64, r: i64) -> ULEntry { ULEntry { tid, iutils: i, rutils: r } }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let entries = vec![
            entry(1, 30, 70),
            entry(3, 50, 20),
            entry(5, 40, 0),
        ];
        let bytes = serialize_ul_body(&entries);
        let decoded = deserialize_ul_body(&bytes);
        assert_eq!(entries.len(), decoded.len());
        for (a, b) in entries.iter().zip(decoded.iter()) {
            let at = a.tid; let bt = b.tid;
            let ai = a.iutils; let bi = b.iutils;
            let ar = a.rutils; let br = b.rutils;
            assert_eq!(at, bt);
            assert_eq!(ai, bi);
            assert_eq!(ar, br);
        }
    }

    #[test]
    fn join_1itemset_basic() {
        // UL({A}): tids 1,2,3; UL({B}): tids 1,3,4
        // Join gives tids 1,3
        let px = vec![entry(1, 30, 70), entry(2, 20, 50), entry(3, 40, 10)];
        let py = vec![entry(1, 10, 0),  entry(3, 50, 0),  entry(4, 60, 0)];
        let store = std::sync::Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().into_path(), false
        ).unwrap());
        let pool = crate::buffer_pool::pool::BufferPool::new_arc(1024 * 1024, store.clone(), Box::new(crate::buffer_pool::eviction::LruPolicy::new()));
        let guard = Arc::new(MemoryGuard::new(1024 * 1024, store.clone()));
        let (ul, body) = join_utility_lists(
            smallvec![1u32, 2u32], &[], &px, &py, BodyAlloc::new(&pool, &guard)
        ).unwrap();
        assert_eq!(ul.len, 2); // tids 1 and 3
        // tid1: iutils = 30+10=40, rutils=0
        // tid3: iutils = 40+50=90, rutils=0
        assert_eq!(ul.sum_iutils, 130);
        assert_eq!(ul.sum_rutils, 0);
        match body { UlBody::InMemory(entries, _) => { assert_eq!(entries.len(), 2); } _ => {} }
    }

    #[test]
    fn join_empty_intersection() {
        let px = vec![entry(1, 10, 5)];
        let py = vec![entry(2, 20, 5)];
        let store = std::sync::Arc::new(crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().into_path(), false
        ).unwrap());
        let pool = crate::buffer_pool::pool::BufferPool::new_arc(1024 * 1024, store.clone(), Box::new(crate::buffer_pool::eviction::LruPolicy::new()));
        let guard = Arc::new(MemoryGuard::new(1024 * 1024, store.clone()));
        let (ul, _) = join_utility_lists(smallvec![1u32, 2u32], &[], &px, &py, BodyAlloc::new(&pool, &guard)).unwrap();
        assert_eq!(ul.len, 0);
        assert_eq!(ul.sum_iutils, 0);
    }
}
