use std::io;
use crate::types::{ItemId, Utility, PageId, ULEntry, UtilityList, RecomputeFlag};
use smallvec::SmallVec;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
/// Size of one packed spill page (at most; smaller under tiny budgets).
const ARENA_PAGE_BYTES: usize = 256 * 1024;

/// Entries per chunk of a chunked body (80 KB), scaled down under tiny budgets so the
/// per-join working set (a few chunks) stays a small fraction of the budget.
pub fn chunk_entries(guard: &MemoryGuard) -> usize {
    (guard.budget() / 256 / 20).clamp(64, 4096)
}

fn arena_page_bytes(guard: &MemoryGuard) -> usize {
    (guard.budget() / 64).clamp(4 * 1024, ARENA_PAGE_BYTES)
}

/// Packs small spilled lists into shared pages so a memory-starved search does not
/// create one file (and one pool frame) per tiny list. Single-threaded: create one per
/// task / thread. A packed page is discarded once every list stored in it is dropped.
pub struct SpillArena {
    pool: Arc<BufferPool>,
    page_bytes: usize,
    inner: std::cell::RefCell<ArenaInner>,
}

struct ArenaInner {
    slot: Option<Arc<OwnedPage>>,
    buf: Vec<u8>,
    _res: Reservation,
}

impl SpillArena {
    pub fn new(pool: &Arc<BufferPool>, guard: &Arc<MemoryGuard>) -> Self {
        let page_bytes = arena_page_bytes(guard);
        Self {
            pool: Arc::clone(pool),
            page_bytes,
            inner: std::cell::RefCell::new(ArenaInner {
                slot: None,
                buf: Vec::new(),
                _res: guard.reserve_force(page_bytes),
            }),
        }
    }

    fn append(&self, entries: &[ULEntry]) -> io::Result<UlBody> {
        let bytes = entries.len() * 20;
        let mut inner = self.inner.borrow_mut();
        if inner.slot.is_none() || inner.buf.len() + bytes > self.page_bytes {
            Self::flush_inner(&self.pool, &mut inner)?;
            inner.slot = Some(Arc::new(OwnedPage::reserve_id(&self.pool)));
            inner.buf = Vec::with_capacity(self.page_bytes.max(bytes));
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
    /// Where spill/read timings are recorded (cost model), if any.
    pub stats: Option<&'a CostStats>,
}

impl<'a> BodyAlloc<'a> {
    pub fn new(pool: &'a Arc<BufferPool>, guard: &'a Arc<MemoryGuard>) -> Self {
        Self { pool, guard, arena: None, stats: None }
    }

    pub fn with_stats(self, stats: &'a CostStats) -> Self {
        Self { stats: Some(stats), ..self }
    }

    pub fn with_arena(self, arena: &'a SpillArena) -> Self {
        Self { arena: Some(arena), ..self }
    }

    /// Store entries that must go to the pool (no RAM reservation). Lists longer than one
    /// chunk become a chunked body, so no single page (and no single pin) grows with the data.
    pub fn spill_body(&self, entries: &[ULEntry]) -> io::Result<UlBody> {
        let t0 = std::time::Instant::now();
        let body = self.spill_body_untimed(entries)?;
        if let Some(st) = self.stats {
            st.write_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            st.write_bytes.fetch_add((entries.len() * 20) as u64, Ordering::Relaxed);
        }
        Ok(body)
    }

    fn spill_body_untimed(&self, entries: &[ULEntry]) -> io::Result<UlBody> {
        let chunk = chunk_entries(self.guard);
        if entries.len() > chunk {
            let parts = entries.chunks(chunk).map(|c| self.spill_one(c)).collect::<io::Result<Vec<_>>>()?;
            return Ok(UlBody::Chunked(parts));
        }
        self.spill_one(entries)
    }

    fn spill_one(&self, entries: &[ULEntry]) -> io::Result<UlBody> {
        let bytes = entries.len() * 20;
        match self.arena {
            Some(arena) if bytes < OWN_PAGE_BYTES.min(arena.page_bytes / 2) => arena.append(entries),
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
            UlBody::Dropped => Err(dropped_err()),
            UlBody::Chunked(_) => {
                // Contiguous access to a chunked body needs a copy; the utility-list engine
                // streams chunks instead (see `BodyCursor`), this is for other callers.
                let n = body.len(*self)?;
                let r = self.guard.reserve_force(vec_bytes::<ULEntry>(n));
                let mut v = Vec::with_capacity(n);
                let mut c = BodyCursor::new(*self, body);
                while let Some(e) = c.head()? {
                    v.push(e);
                    c.advance();
                }
                Ok(BodyView::Loaded(v, r))
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
    /// A long list stored as consecutive chunks (each one of the variants above).
    Chunked(Vec<UlBody>),
    /// Not stored: rematerialised on demand from its parents (see `ListSrc::Join`). Only the
    /// header (length and sums) is kept.
    Dropped,
}

impl UlBody {
    pub fn page_id(&self) -> PageId {
        match self {
            UlBody::InMemory(..) => 0,
            UlBody::OnDisk(p) => p.id,
            UlBody::Packed { slot, .. } => slot.id,
            UlBody::Chunked(parts) => parts.first().map_or(0, |p| p.page_id()),
            UlBody::Dropped => 0,
        }
    }

    pub fn is_dropped(&self) -> bool { matches!(self, UlBody::Dropped) }

    /// Number of entries.
    pub fn len(&self, alloc: BodyAlloc) -> io::Result<usize> {
        Ok(match self {
            UlBody::InMemory(v, _) => v.len(),
            UlBody::Packed { count, .. } => *count as usize,
            UlBody::OnDisk(_) => alloc.view(self)?.len(),
            UlBody::Chunked(parts) => {
                let mut n = 0;
                for p in parts { n += p.len(alloc)?; }
                n
            }
            UlBody::Dropped => return Err(dropped_err()),
        })
    }

    /// TID of the last entry (entries are in ascending TID order).
    pub fn last_tid(&self, alloc: BodyAlloc) -> io::Result<Option<u32>> {
        match self {
            UlBody::Chunked(parts) => match parts.last() { Some(p) => p.last_tid(alloc), None => Ok(None) },
            UlBody::Dropped => Err(dropped_err()),
            _ => Ok(alloc.view(self)?.last().map(|e| { let t = e.tid; t })),
        }
    }
}

fn dropped_err() -> io::Error {
    io::Error::new(io::ErrorKind::Other, "internal: a dropped utility list must be read through its recipe")
}

/// Measured costs, accumulated over a run, that drive the recompute-vs-spill decision.
#[derive(Default)]
pub struct CostStats {
    pub join_ns: AtomicU64,
    pub join_entries: AtomicU64,
    pub write_ns: AtomicU64,
    pub write_bytes: AtomicU64,
    pub read_ns: AtomicU64,
    pub read_bytes: AtomicU64,
    /// Lists dropped for rematerialisation, and an estimate of the bytes not written.
    pub dropped: AtomicU64,
    pub dropped_bytes: AtomicU64,
    /// Entries produced by recomputation (lazy streams and materialisations).
    pub recomputed_entries: AtomicU64,
}

impl CostStats {
    fn rate(ns: &AtomicU64, units: &AtomicU64, default: f64) -> f64 {
        let (n, u) = (ns.load(Ordering::Relaxed), units.load(Ordering::Relaxed));
        if u < 4096 { default } else { n as f64 / u as f64 }
    }
    /// ns per entry merged by a join (CPU cost of recomputation).
    pub fn cpu_ns_per_entry(&self) -> f64 { Self::rate(&self.join_ns, &self.join_entries, 10.0) }
    /// ns per byte written to / read from spilled pages (measured on this device).
    pub fn write_ns_per_byte(&self) -> f64 { Self::rate(&self.write_ns, &self.write_bytes, 10.0) }
    pub fn read_ns_per_byte(&self) -> f64 { Self::rate(&self.read_ns, &self.read_bytes, 5.0) }
}

/// What to do with a new list once the budget has no room for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RematMode {
    /// Always spill (previous behaviour).
    Off,
    /// Cost model: drop when recomputing is estimated cheaper than spilling.
    Auto,
    /// Always drop (for experiments).
    Always,
}

impl RematMode {
    pub fn from_env() -> Self {
        match std::env::var("AIR_HUIM_REMAT").ok().as_deref() {
            Some("off") => RematMode::Off,
            Some("always") => RematMode::Always,
            _ => RematMode::Auto,
        }
    }
}

/// Inputs to the recompute-vs-spill decision for one new list.
#[derive(Clone, Copy)]
pub struct DropPolicy<'a> {
    pub mode: RematMode,
    /// How many later joins will read this list as P·y (each one re-runs the recomputation).
    pub uses: u32,
    /// Entries the recomputation has to merge (sizes of its parents).
    pub recompute_entries: u64,
    /// Upper bound on the new list's length.
    pub max_len: u64,
    /// Relative weight of flash writes (wear); AIR_HUIM_WRITE_WEIGHT, default 1.
    pub write_weight: f64,
    pub stats: &'a CostStats,
}

impl DropPolicy<'_> {
    /// Recompute (drop) rather than spill?
    fn prefer_drop(&self) -> bool {
        match self.mode {
            RematMode::Off => false,
            RematMode::Always => true,
            RematMode::Auto => {
                let bytes = self.max_len as f64 * 20.0;
                let reads = self.uses as f64 + 1.0; // each P·y use + one materialisation as P·x
                let spill = bytes * (self.write_weight * self.stats.write_ns_per_byte() + reads * self.stats.read_ns_per_byte());
                let recompute = reads * self.recompute_entries as f64 * self.stats.cpu_ns_per_entry();
                recompute < spill
            }
        }
    }
}

/// Source of a utility list for reading: a stored body, or a recipe (lazy join of parents).
pub enum ListSrc<'a> {
    Body(&'a UlBody),
    Join(Box<JoinSrc<'a>>),
}

pub struct JoinSrc<'a> {
    pub prefix: Option<ListSrc<'a>>,
    pub px: ListSrc<'a>,
    pub py: ListSrc<'a>,
}

/// Streaming reader over a `ListSrc`: a body cursor, or a lazy join that produces entries
/// on the fly from its inputs' streams (no buffer for the recomputed list).
pub enum ListStream<'a> {
    Body(BodyCursor<'a>),
    Join(Box<JoinStream<'a>>),
}

pub struct JoinStream<'a> {
    p: Option<ListStream<'a>>,
    x: ListStream<'a>,
    y: ListStream<'a>,
    cur: Option<ULEntry>,
    done: bool,
    stats: Option<&'a CostStats>,
}

impl<'a> ListStream<'a> {
    pub fn new(alloc: BodyAlloc<'a>, src: &ListSrc<'a>, stats: Option<&'a CostStats>) -> Self {
        match src {
            ListSrc::Body(b) => ListStream::Body(BodyCursor::new(alloc, b)),
            ListSrc::Join(j) => ListStream::Join(Box::new(JoinStream {
                p: j.prefix.as_ref().map(|p| ListStream::new(alloc, p, stats)),
                x: ListStream::new(alloc, &j.px, stats),
                y: ListStream::new(alloc, &j.py, stats),
                cur: None,
                done: false,
                stats,
            })),
        }
    }

    #[inline]
    pub fn head(&mut self) -> io::Result<Option<ULEntry>> {
        match self {
            ListStream::Body(c) => c.head(),
            ListStream::Join(j) => j.head(),
        }
    }

    #[inline]
    pub fn advance(&mut self) {
        match self {
            ListStream::Body(c) => c.advance(),
            ListStream::Join(j) => j.cur = None,
        }
    }
}

impl JoinStream<'_> {
    fn head(&mut self) -> io::Result<Option<ULEntry>> {
        if let Some(e) = self.cur { return Ok(Some(e)); }
        if self.done { return Ok(None); }
        loop {
            let (Some(ex), Some(ey)) = (self.x.head()?, self.y.head()?) else { self.done = true; return Ok(None) };
            let (tx, ty) = (ex.tid, ey.tid);
            if tx < ty { self.x.advance(); continue; }
            if tx > ty { self.y.advance(); continue; }
            let mut prefix_iutils = 0;
            if let Some(c) = self.p.as_mut() {
                while let Some(e) = c.head()? {
                    let t = e.tid;
                    if t < tx { c.advance(); continue; }
                    if t == tx { prefix_iutils = e.iutils; }
                    break;
                }
            }
            self.x.advance();
            self.y.advance();
            let e = ULEntry { tid: tx, iutils: ex.iutils + ey.iutils - prefix_iutils, rutils: ey.rutils };
            if let Some(s) = self.stats { s.recomputed_entries.fetch_add(1, Ordering::Relaxed); }
            self.cur = Some(e);
            return Ok(Some(e));
        }
    }
}

/// Sequential reader over a body, one chunk in memory (or pinned) at a time.
pub struct BodyCursor<'a> {
    alloc: BodyAlloc<'a>,
    parts: SmallVec<[&'a UlBody; 1]>,
    next_part: usize,
    cur: Option<BodyView<'a>>,
    pos: usize,
}

impl<'a> BodyCursor<'a> {
    pub fn new(alloc: BodyAlloc<'a>, body: &'a UlBody) -> Self {
        let parts: SmallVec<[&'a UlBody; 1]> = match body {
            UlBody::Chunked(ps) => ps.iter().collect(),
            other => smallvec::smallvec![other],
        };
        Self { alloc, parts, next_part: 0, cur: None, pos: 0 }
    }

    /// Current entry (None at the end).
    #[inline]
    pub fn head(&mut self) -> io::Result<Option<ULEntry>> {
        loop {
            if let Some(v) = &self.cur {
                if self.pos < v.len() { return Ok(Some(v[self.pos])); }
            }
            if self.next_part >= self.parts.len() { return Ok(None); }
            self.cur = None; // unpin the previous chunk before pinning the next one
            let part = self.parts[self.next_part];
            let spilled = matches!(part, UlBody::OnDisk(_) | UlBody::Packed { .. });
            let t0 = spilled.then(std::time::Instant::now);
            self.cur = Some(self.alloc.view(part)?);
            if let (Some(t0), Some(st)) = (t0, self.alloc.stats) {
                st.read_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                st.read_bytes.fetch_add((self.cur.as_ref().unwrap().len() * 20) as u64, Ordering::Relaxed);
            }
            self.next_part += 1;
            self.pos = 0;
        }
    }

    #[inline]
    pub fn advance(&mut self) { self.pos += 1; }
}

/// Builds a body chunk by chunk: each full chunk is kept in RAM if the budget allows,
/// otherwise spilled. Memory held by the writer itself is one chunk.
pub struct BodyWriter<'a> {
    alloc: BodyAlloc<'a>,
    chunk: usize,
    buf: Vec<ULEntry>,
    parts: Vec<UlBody>,
    /// Write chunks straight to the pool (used for lists we already know will not fit).
    prefer_disk: bool,
    /// When the budget has no room: drop (rematerialise later) instead of spilling, if the
    /// policy says recomputing is cheaper.
    drop_policy: Option<DropPolicy<'a>>,
    dropping: bool,
    pub len: u32,
    pub sum_iutils: Utility,
    pub sum_rutils: Utility,
    _res: Reservation,
}

impl<'a> BodyWriter<'a> {
    pub fn new(alloc: BodyAlloc<'a>, prefer_disk: bool) -> Self {
        let chunk = chunk_entries(alloc.guard);
        Self { alloc, chunk, buf: Vec::new(), parts: Vec::new(), prefer_disk, drop_policy: None, dropping: false,
               len: 0, sum_iutils: 0, sum_rutils: 0,
               _res: alloc.guard.reserve_force(vec_bytes::<ULEntry>(chunk)) }
    }

    pub fn with_drop_policy(mut self, p: DropPolicy<'a>) -> Self {
        self.drop_policy = Some(p);
        self
    }

    #[inline]
    pub fn push(&mut self, e: ULEntry) -> io::Result<()> {
        self.sum_iutils += e.iutils;
        self.sum_rutils += e.rutils;
        self.len += 1;
        if self.dropping { return Ok(()); } // only the header is kept
        if self.buf.capacity() == 0 { self.buf.reserve_exact(self.chunk); }
        self.buf.push(e);
        if self.buf.len() >= self.chunk { self.flush_part()?; }
        Ok(())
    }

    fn flush_part(&mut self) -> io::Result<()> {
        if self.buf.is_empty() || self.dropping { return Ok(()); }
        let mut entries = std::mem::take(&mut self.buf);
        let part = if self.prefer_disk {
            self.alloc.spill_body(&entries)?
        } else {
            entries.shrink_to_fit();
            match self.alloc.guard.reserve(vec_bytes::<ULEntry>(entries.capacity())) {
                Some(r) => UlBody::InMemory(entries, Some(r)),
                None => {
                    if self.drop_policy.is_some_and(|p| p.prefer_drop()) {
                        // Budget full and recomputing is cheaper: keep only the header.
                        self.dropping = true;
                        self.parts.clear();
                        return Ok(());
                    }
                    self.alloc.spill_body(&entries)?
                }
            }
        };
        self.parts.push(part);
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<UlBody> {
        self.flush_part()?;
        if self.dropping {
            if let Some(p) = self.drop_policy {
                p.stats.dropped.fetch_add(1, Ordering::Relaxed);
                p.stats.dropped_bytes.fetch_add(self.len as u64 * 20, Ordering::Relaxed);
            }
            return Ok(UlBody::Dropped);
        }
        Ok(match self.parts.len() {
            0 => UlBody::InMemory(Vec::new(), None),
            1 => self.parts.pop().unwrap(),
            _ => UlBody::Chunked(std::mem::take(&mut self.parts)),
        })
    }
}

/// Streaming join over list sources (stored bodies or recipes). Same semantics as
/// `join_bodies`; inputs that were dropped are recomputed on the fly by lazy join streams, and
/// the result may itself be dropped under `policy`.
#[allow(clippy::too_many_arguments)]
pub fn join_srcs<'a>(
    itemset: SmallVec<[ItemId; 8]>,
    prefix: Option<&ListSrc<'a>>,
    px: &ListSrc<'a>,
    py: &ListSrc<'a>,
    alloc: BodyAlloc<'a>,
    la: Option<LaPrune>,
    policy: Option<DropPolicy<'a>>,
    mut on_entry: Option<&mut dyn FnMut(&ULEntry)>,
) -> io::Result<Option<(UtilityList, UlBody)>> {
    let t0 = std::time::Instant::now();
    let mut scanned = 0u64;
    let mut bound = la.map_or(0, |l| l.start);
    let lose = |e: &ULEntry| -> Utility {
        match la { Some(l) if l.average => e.rutils, Some(_) => e.iutils + e.rutils, None => 0 }
    };
    let stats = alloc.stats;
    let mut cx = ListStream::new(alloc, px, stats);
    let mut cy = ListStream::new(alloc, py, stats);
    let mut cp = prefix.map(|p| ListStream::new(alloc, p, stats));
    let mut w = BodyWriter::new(alloc, false);
    if let Some(p) = policy { w = w.with_drop_policy(p); }
    loop {
        let (Some(ex), Some(ey)) = (cx.head()?, cy.head()?) else { break };
        scanned += 1;
        let (tx, ty) = (ex.tid, ey.tid);
        if tx < ty {
            if let Some(l) = la {
                bound -= lose(&ex);
                if bound < l.threshold { return Ok(None); }
            }
            cx.advance();
            continue;
        }
        if tx > ty {
            cy.advance();
            continue;
        }
        let mut prefix_iutils = 0;
        if let Some(c) = cp.as_mut() {
            while let Some(e) = c.head()? {
                let t = e.tid;
                if t < tx { c.advance(); continue; }
                if t == tx { prefix_iutils = e.iutils; }
                break;
            }
        }
        let e = ULEntry { tid: tx, iutils: ex.iutils + ey.iutils - prefix_iutils, rutils: ey.rutils };
        if let Some(f) = on_entry.as_mut() { f(&e); }
        w.push(e)?;
        cx.advance();
        cy.advance();
    }
    drop((cx, cy, cp));
    if let Some(st) = stats {
        st.join_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        st.join_entries.fetch_add(scanned.max(1), Ordering::Relaxed);
    }
    let (len, sum_iutils, sum_rutils) = (w.len, w.sum_iutils, w.sum_rutils);
    let body = w.finish()?;
    let page_id = body.page_id();
    let ul = UtilityList {
        itemset,
        sum_iutils,
        sum_rutils,
        len,
        page_id,
        resident: true,
        recompute: if body.is_dropped() { RecomputeFlag::Recomputable } else { RecomputeFlag::Materialized },
    };
    Ok(Some((ul, body)))
}

/// Materialise a list source into a stored body (RAM if it fits, else spilled).
pub fn materialize<'a>(src: &ListSrc<'a>, alloc: BodyAlloc<'a>) -> io::Result<UlBody> {
    let mut s = ListStream::new(alloc, src, alloc.stats);
    let mut w = BodyWriter::new(alloc, false);
    while let Some(e) = s.head()? {
        w.push(e)?;
        s.advance();
    }
    drop(s);
    w.finish()
}

/// Streaming utility-list join: P·x·y from the bodies of P (None for the empty prefix), P·x
/// and P·y. Reads one chunk of each input at a time and writes the result chunk by chunk,
/// so the working set is a constant few chunks regardless of list lengths.
/// Same semantics as `join_utility_lists_la` (including LA-prune).
pub fn join_bodies(
    itemset: SmallVec<[ItemId; 8]>,
    prefix: Option<&UlBody>,
    px: &UlBody,
    py: &UlBody,
    alloc: BodyAlloc,
    la: Option<LaPrune>,
) -> io::Result<Option<(UtilityList, UlBody)>> {
    let mut bound = la.map_or(0, |l| l.start);
    let lose = |e: &ULEntry| -> Utility {
        match la { Some(l) if l.average => e.rutils, Some(_) => e.iutils + e.rutils, None => 0 }
    };
    let mut cx = BodyCursor::new(alloc, px);
    let mut cy = BodyCursor::new(alloc, py);
    let mut cp = prefix.map(|p| BodyCursor::new(alloc, p));
    let mut w = BodyWriter::new(alloc, false);
    loop {
        let (Some(ex), Some(ey)) = (cx.head()?, cy.head()?) else { break };
        let (tx, ty) = (ex.tid, ey.tid);
        if tx < ty {
            if let Some(l) = la {
                bound -= lose(&ex);
                if bound < l.threshold { return Ok(None); }
            }
            cx.advance();
            continue;
        }
        if tx > ty {
            cy.advance();
            continue;
        }
        let mut prefix_iutils = 0;
        if let Some(c) = cp.as_mut() {
            while let Some(e) = c.head()? {
                let t = e.tid;
                if t < tx { c.advance(); continue; }
                if t == tx { prefix_iutils = e.iutils; }
                break;
            }
        }
        w.push(ULEntry { tid: tx, iutils: ex.iutils + ey.iutils - prefix_iutils, rutils: ey.rutils })?;
        cx.advance();
        cy.advance();
    }
    drop((cx, cy, cp));
    let (len, sum_iutils, sum_rutils) = (w.len, w.sum_iutils, w.sum_rutils);
    let body = w.finish()?;
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
