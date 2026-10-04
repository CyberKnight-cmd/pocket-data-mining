//! Horizontal (transaction-major) database that lives inside the memory budget.
//!
//! Projection-based miners (EFIM family) need random access to transactions by index.
//! The database is stored as flat ~256 KB segments; each segment is kept in RAM while
//! the budget allows, otherwise it becomes a buffer-pool page (cached when there is
//! room, evicted to disk when native structures need the memory).
//!
//! Segment layout (little endian):
//!   ntx: u32 | entry_offsets: u32 x (ntx + 1) | items: u32 x E | utils: i64 x E | rem: i64 x E
//! where `rem[k]` is the utility of the items after position k in the transaction.

use std::io;
use std::sync::Arc;
use crate::buffer_pool::{frame::PinGuard, pool::{BufferPool, OwnedPage}};
use crate::mining::core::memory_guard::{MemoryGuard, Reservation};
use crate::types::{ItemId, Utility};

/// Segment size: 1/64 of the budget, 4 KB to 256 KB. A reader pins one segment at a time;
/// the builder holds one open segment (twice, while copying it out).
pub fn segment_bytes_for(budget: usize) -> usize {
    (budget / 64).clamp(4 * 1024, 256 * 1024)
}

enum SegData {
    Mem(Vec<u8>, Reservation),
    Disk(OwnedPage),
}

pub struct PagedDb {
    segs: Vec<SegData>,
    /// First transaction index of each segment.
    seg_first: Vec<u32>,
    n_tx: u32,
    pub resident_bytes: usize,
    pub paged_bytes: usize,
}

pub struct PagedDbBuilder<'a> {
    pool: &'a Arc<BufferPool>,
    guard: &'a Arc<MemoryGuard>,
    db: PagedDb,
    // current segment being filled
    offsets: Vec<u32>,
    items: Vec<ItemId>,
    utils: Vec<Utility>,
    rem: Vec<Utility>,
    segment: usize,
    _scratch: Reservation,
}

impl<'a> PagedDbBuilder<'a> {
    pub fn new(pool: &'a Arc<BufferPool>, guard: &'a Arc<MemoryGuard>) -> Self {
        Self {
            pool,
            guard,
            db: PagedDb { segs: Vec::new(), seg_first: Vec::new(), n_tx: 0, resident_bytes: 0, paged_bytes: 0 },
            offsets: vec![0],
            items: Vec::new(),
            utils: Vec::new(),
            rem: Vec::new(),
            segment: segment_bytes_for(guard.budget()),
            _scratch: guard.reserve_force(2 * segment_bytes_for(guard.budget())),
        }
    }

    /// Append a transaction (items already in processing order). Returns its index.
    pub fn push(&mut self, items: &[ItemId], utils: &[Utility]) -> io::Result<u32> {
        let idx = self.db.n_tx;
        let mut ru: Utility = utils.iter().sum();
        for (k, &it) in items.iter().enumerate() {
            ru -= utils[k];
            self.items.push(it);
            self.utils.push(utils[k]);
            self.rem.push(ru);
        }
        self.offsets.push(self.items.len() as u32);
        self.db.n_tx += 1;
        if self.cur_bytes() >= self.segment {
            self.seal_segment()?;
        }
        Ok(idx)
    }

    fn cur_bytes(&self) -> usize {
        4 + self.offsets.len() * 4 + self.items.len() * 20
    }

    fn seal_segment(&mut self) -> io::Result<()> {
        let ntx = self.offsets.len() - 1;
        if ntx == 0 { return Ok(()); }
        let mut buf = Vec::with_capacity(self.cur_bytes());
        buf.extend_from_slice(&(ntx as u32).to_le_bytes());
        for o in &self.offsets { buf.extend_from_slice(&o.to_le_bytes()); }
        for i in &self.items { buf.extend_from_slice(&i.to_le_bytes()); }
        for u in &self.utils { buf.extend_from_slice(&u.to_le_bytes()); }
        for r in &self.rem { buf.extend_from_slice(&r.to_le_bytes()); }
        let first = self.db.n_tx - ntx as u32;
        let len = buf.len();
        let seg = match self.guard.reserve(len + 16) {
            Some(r) => { self.db.resident_bytes += len; SegData::Mem(buf, r) }
            None => { self.db.paged_bytes += len; SegData::Disk(OwnedPage::create(self.pool, buf)?) }
        };
        self.db.segs.push(seg);
        self.db.seg_first.push(first);
        self.offsets.clear();
        self.offsets.push(0);
        self.items.clear();
        self.utils.clear();
        self.rem.clear();
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<PagedDb> {
        self.seal_segment()?;
        Ok(std::mem::replace(&mut self.db, PagedDb {
            segs: Vec::new(), seg_first: Vec::new(), n_tx: 0, resident_bytes: 0, paged_bytes: 0,
        }))
    }
}

impl PagedDb {
    pub fn len(&self) -> u32 { self.n_tx }

    pub fn cursor(&self) -> TxCursor<'_> {
        TxCursor { db: self, seg: usize::MAX, pin: None }
    }

    fn seg_of(&self, tx: u32) -> usize {
        self.seg_first.partition_point(|&f| f <= tx) - 1
    }
}

/// Sequential-friendly reader: keeps the current segment open (pinned if on disk).
/// Create short-lived cursors; do not hold one across recursion.
pub struct TxCursor<'a> {
    db: &'a PagedDb,
    seg: usize,
    pin: Option<PinGuard>,
}

/// One transaction's items, utilities and remaining utilities.
#[derive(Clone, Copy)]
pub struct TxView<'b> {
    bytes: &'b [u8],
    items_at: usize,
    utils_at: usize,
    rem_at: usize,
    start: usize,
    pub len: usize,
}

impl TxView<'_> {
    #[inline]
    pub fn item(&self, k: usize) -> ItemId {
        let p = self.items_at + (self.start + k) * 4;
        u32::from_le_bytes(self.bytes[p..p + 4].try_into().unwrap())
    }
    #[inline]
    pub fn util(&self, k: usize) -> Utility {
        let p = self.utils_at + (self.start + k) * 8;
        i64::from_le_bytes(self.bytes[p..p + 8].try_into().unwrap())
    }
    #[inline]
    pub fn rem(&self, k: usize) -> Utility {
        let p = self.rem_at + (self.start + k) * 8;
        i64::from_le_bytes(self.bytes[p..p + 8].try_into().unwrap())
    }
    /// Position of `item` at or after `from`, if present.
    pub fn position_from(&self, from: usize, item: ItemId) -> Option<usize> {
        (from..self.len).find(|&k| self.item(k) == item)
    }
    pub fn contains(&self, item: ItemId) -> bool {
        self.position_from(0, item).is_some()
    }
}

impl<'a> TxCursor<'a> {
    pub fn tx(&mut self, idx: u32) -> io::Result<TxView<'_>> {
        let s = self.db.seg_of(idx);
        if s != self.seg {
            self.pin = None;
            if let SegData::Disk(page) = &self.db.segs[s] {
                self.pin = Some(page.pin()?);
            }
            self.seg = s;
        }
        let bytes: &[u8] = match &self.db.segs[s] {
            SegData::Mem(v, _) => v,
            SegData::Disk(_) => &self.pin.as_ref().unwrap()[..],
        };
        let ntx = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let local = (idx - self.db.seg_first[s]) as usize;
        let off = |i: usize| u32::from_le_bytes(bytes[4 + i * 4..8 + i * 4].try_into().unwrap()) as usize;
        let total = off(ntx);
        let items_at = 4 + (ntx + 1) * 4;
        let utils_at = items_at + total * 4;
        let rem_at = utils_at + total * 8;
        let start = off(local);
        Ok(TxView { bytes, items_at, utils_at, rem_at, start, len: off(local + 1) - start })
    }
}
