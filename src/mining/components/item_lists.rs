//! Budget-aware construction of 1-itemset utility lists.
//!
//! The 1-itemset lists are the whole database in vertical form, so they are usually the
//! largest structure a utility-list miner builds. `ItemListBuilder` accumulates entries
//! in RAM while its share of the budget allows, and spills sorted "runs" to the buffer
//! pool / disk when it does not. `finish()` merges runs back item by item and keeps the
//! smallest lists in RAM (cheap, many of them) while paging the large ones.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use crate::buffer_pool::pool::OwnedPage;
use crate::mining::core::memory_guard::{Reservation, vec_bytes, map_bytes};
use crate::types::{ItemId, Utility, ULEntry, UtilityList, RecomputeFlag};
use super::ul_join::{BodyAlloc, BodyWriter, SpillArena, UlBody, serialize_ul_body};

/// Target size of one spill segment.
const SEGMENT_BYTES: usize = 1 << 20;
/// Never spill less than this (unless the whole limit is smaller): tiny runs cost more
/// in metadata and merge cursors than they free.
const MIN_SPILL_BYTES: usize = 4 << 20;

/// Spill segment size: 1 MB, smaller under tiny budgets.
fn segment_bytes(alloc: BodyAlloc) -> usize {
    (alloc.guard.budget() / 32).clamp(4 * 1024, SEGMENT_BYTES)
}
const RECORD_HEADER: usize = 8; // item u32 + count u32

/// One spilled run: segments of `[item][count][entries..]` records, items ascending.
struct Run {
    segments: Vec<OwnedPage>,
}

pub struct ItemListBuilder<'a> {
    alloc: BodyAlloc<'a>,
    bufs: HashMap<ItemId, Vec<ULEntry>>,
    counts: HashMap<ItemId, u64>,
    /// Pays for `bufs` (vector capacities + table).
    res: Reservation,
    buffered: usize,
    /// Max bytes the in-RAM buffers may use before a spill.
    limit: usize,
    runs: Vec<Run>,
}

impl<'a> ItemListBuilder<'a> {
    /// `share` = fraction of the currently free native budget the buffers may use.
    pub fn new(alloc: BodyAlloc<'a>, share: f64) -> Self {
        let limit = ((alloc.guard.native_remaining() as f64) * share) as usize;
        Self {
            res: alloc.guard.reserve_force(0),
            alloc,
            bufs: HashMap::new(),
            counts: HashMap::new(),
            buffered: 0,
            limit: limit.max(64 * 1024),
            runs: Vec::new(),
        }
    }

    /// Append one entry for `item`. Entries for an item must arrive in ascending tid order.
    pub fn push(&mut self, item: ItemId, entry: ULEntry) -> io::Result<()> {
        let map_cap_before = self.bufs.capacity();
        let v = self.bufs.entry(item).or_default();
        let cap_before = v.capacity();
        v.push(entry);
        let mut delta = vec_bytes::<ULEntry>(v.capacity()).saturating_sub(vec_bytes::<ULEntry>(cap_before));
        if self.bufs.capacity() != map_cap_before {
            delta += map_bytes::<ItemId, Vec<ULEntry>>(self.bufs.capacity())
                .saturating_sub(map_bytes::<ItemId, Vec<ULEntry>>(map_cap_before));
        }
        *self.counts.entry(item).or_insert(0) += 1;
        if delta > 0 {
            // Already allocated: account for it, then spill if we are over our share.
            self.res.grow_force(delta);
            self.buffered += delta;
            let starved = self.alloc.guard.native_remaining() == 0
                && self.buffered >= MIN_SPILL_BYTES.min(self.limit);
            if self.buffered > self.limit || starved {
                self.spill()?;
            }
        }
        Ok(())
    }

    /// Write all buffered entries to a new run and release their memory.
    fn spill(&mut self) -> io::Result<()> {
        let bufs = std::mem::take(&mut self.bufs);
        let mut items: Vec<(ItemId, Vec<ULEntry>)> = bufs.into_iter().filter(|(_, v)| !v.is_empty()).collect();
        items.sort_unstable_by_key(|(i, _)| *i);

        let mut run = Run { segments: Vec::new() };
        // Segments (and the record pieces inside them) are bounded, so reading a run back
        // later pins at most one bounded segment at a time — whatever the item sizes.
        let seg_bytes = segment_bytes(self.alloc);
        let piece = (seg_bytes / 20).max(1);
        let _seg_res = self.alloc.guard.reserve_force(seg_bytes * 2);
        let mut seg: Vec<u8> = Vec::with_capacity(seg_bytes);
        for (item, entries) in items {
            for part in entries.chunks(piece) {
                seg.extend_from_slice(&item.to_le_bytes());
                seg.extend_from_slice(&(part.len() as u32).to_le_bytes());
                seg.extend_from_slice(&serialize_ul_body(part));
                if seg.len() >= seg_bytes {
                    run.segments.push(OwnedPage::create_on_disk(self.alloc.pool, std::mem::take(&mut seg))?);
                    seg = Vec::with_capacity(seg_bytes);
                }
            }
            drop(entries);
        }
        if !seg.is_empty() {
            run.segments.push(OwnedPage::create_on_disk(self.alloc.pool, seg)?);
        }
        self.runs.push(run);
        let b = self.res.bytes();
        self.res.shrink(b);
        self.buffered = 0;
        Ok(())
    }

    /// Merge several runs into one (same record format, items ascending), on disk.
    fn merge_runs(&self, group: Vec<Run>, items: &[ItemId]) -> io::Result<Run> {
        let seg_bytes = segment_bytes(self.alloc);
        let piece = (seg_bytes / 20).max(1);
        let _seg_res = self.alloc.guard.reserve_force(seg_bytes * 2);
        let mut cursors: Vec<RunCursor> = group.into_iter().map(RunCursor::new).collect::<io::Result<_>>()?;
        let mut run = Run { segments: Vec::new() };
        let mut seg: Vec<u8> = Vec::with_capacity(seg_bytes);
        let mut part: Vec<ULEntry> = Vec::with_capacity(piece);
        let pool = self.alloc.pool;
        let flush_part = |item: ItemId, part: &mut Vec<ULEntry>, seg: &mut Vec<u8>, run: &mut Run| -> io::Result<()> {
            if part.is_empty() { return Ok(()); }
            seg.extend_from_slice(&item.to_le_bytes());
            seg.extend_from_slice(&(part.len() as u32).to_le_bytes());
            seg.extend_from_slice(&serialize_ul_body(part));
            part.clear();
            if seg.len() >= seg_bytes {
                run.segments.push(OwnedPage::create_on_disk(pool, std::mem::replace(seg, Vec::with_capacity(seg_bytes)))?);
            }
            Ok(())
        };
        for &item in items {
            for c in cursors.iter_mut() {
                c.take_item(item, &mut |e| {
                    part.push(e);
                    if part.len() >= piece { flush_part(item, &mut part, &mut seg, &mut run)?; }
                    Ok(())
                })?;
            }
            flush_part(item, &mut part, &mut seg, &mut run)?;
        }
        if !seg.is_empty() {
            run.segments.push(OwnedPage::create_on_disk(pool, seg)?);
        }
        Ok(run)
    }

    /// Merge everything into final per-item lists, sorted by item id.
    pub fn finish(mut self) -> io::Result<Vec<(ItemId, UtilityList, UlBody)>> {
        let mut items: Vec<ItemId> = self.counts.keys().copied().collect();
        items.sort_unstable();

        // Keep the smallest lists in RAM up to half of what is free; page the rest.
        let free = self.alloc.guard.native_remaining() + self.buffered;
        let mut by_size: Vec<u64> = self.counts.values().copied().collect();
        by_size.sort_unstable();
        let mut acc = 0usize;
        let mut ram_max_len = 0u64;
        for c in by_size {
            acc += vec_bytes::<ULEntry>(c as usize);
            if acc > free / 2 { break; }
            ram_max_len = c;
        }

        // The final merge pins one segment per run. Merge runs in groups first if that would
        // take more than a quarter of the free budget (external-sort fan-in).
        let seg_bytes = segment_bytes(self.alloc);
        let fan_in = (self.alloc.guard.native_remaining() / 4 / seg_bytes).max(2);
        while self.runs.len() > fan_in {
            let runs = std::mem::take(&mut self.runs);
            let mut it = runs.into_iter();
            loop {
                let group: Vec<Run> = it.by_ref().take(fan_in).collect();
                if group.is_empty() { break; }
                self.runs.push(if group.len() == 1 { group.into_iter().next().unwrap() } else { self.merge_runs(group, &items)? });
            }
        }
        let mut cursors: Vec<RunCursor> = std::mem::take(&mut self.runs)
            .into_iter().map(RunCursor::new).collect::<io::Result<_>>()?;

        // Pack the paged lists into shared pages instead of one file per item.
        let arena = SpillArena::new(self.alloc.pool, self.alloc.guard);
        let packer = self.alloc.with_arena(&arena);

        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let count = self.counts[&item] as usize;
            let tail = self.bufs.remove(&item).unwrap_or_default();
            let tail_bytes = vec_bytes::<ULEntry>(tail.capacity());

            // Stream the item's entries (spilled runs in order, then the in-RAM tail) into a
            // chunk writer: the whole list is never held in one buffer.
            let mut w = BodyWriter::new(packer, count as u64 > ram_max_len);
            for c in cursors.iter_mut() {
                c.take_item(item, &mut |e| w.push(e))?;
            }
            for e in &tail { w.push(*e)?; }
            drop(tail);
            // The tail's bytes are now owned by the body (re-reserved per chunk).
            self.res.shrink(tail_bytes);
            let (len, sum_iutils, sum_rutils) = (w.len, w.sum_iutils, w.sum_rutils);
            let body = w.finish()?;
            let ul = UtilityList {
                itemset: smallvec::smallvec![item],
                sum_iutils,
                sum_rutils,
                len,
                page_id: body.page_id(),
                resident: true,
                recompute: RecomputeFlag::Recomputable,
            };
            out.push((item, ul, body));
        }
        // Other threads read these lists: the open packed page must be in the pool.
        arena.flush()?;
        Ok(out)
    }
}

/// Sequential reader over one run's segments. Reads go through the buffer pool. The current
/// segment stays pinned until the cursor moves past it: re-pinning it per item let the pool
/// evict it between items, and a merge then re-read each segment once per item (hundreds of
/// GB read for a 3 GB merge). Pinned memory is one segment per run.
struct RunCursor {
    segments: std::vec::IntoIter<OwnedPage>,
    /// Declared before `cur`: the pin must be released before its page is discarded.
    pin: Option<crate::buffer_pool::frame::PinGuard>,
    cur: Option<OwnedPage>,
    pos: usize,
    len: usize,
}

impl RunCursor {
    fn new(run: Run) -> io::Result<Self> {
        let mut c = Self { segments: run.segments.into_iter(), pin: None, cur: None, pos: 0, len: 0 };
        c.advance_segment()?;
        Ok(c)
    }

    fn advance_segment(&mut self) -> io::Result<()> {
        // Unpin, then drop the consumed segment (discarding it from the pool and disk).
        self.pin = None;
        self.cur = self.segments.next();
        self.pos = 0;
        self.pin = match &self.cur { Some(p) => Some(p.pin()?), None => None };
        self.len = self.pin.as_ref().map_or(0, |p| p.len());
        Ok(())
    }

    /// Feed every record piece of `item` at the cursor position (possibly spanning several
    /// segments) to `f`, one entry at a time.
    fn take_item(&mut self, item: ItemId, f: &mut dyn FnMut(ULEntry) -> io::Result<()>) -> io::Result<()> {
        loop {
            let Some(pin) = &self.pin else { return Ok(()) };
            if self.pos + RECORD_HEADER > pin.len() { return Ok(()); }
            let rec_item = u32::from_le_bytes(pin[self.pos..self.pos + 4].try_into().unwrap());
            if rec_item != item { return Ok(()); }
            let count = u32::from_le_bytes(pin[self.pos + 4..self.pos + 8].try_into().unwrap()) as usize;
            let start = self.pos + RECORD_HEADER;
            for k in 0..count {
                let b = &pin[start + 20 * k..start + 20 * k + 20];
                f(ULEntry {
                    tid: u32::from_le_bytes(b[0..4].try_into().unwrap()),
                    iutils: i64::from_le_bytes(b[4..12].try_into().unwrap()),
                    rutils: i64::from_le_bytes(b[12..20].try_into().unwrap()),
                })?;
            }
            self.pos = start + count * 20;
            if self.pos >= self.len {
                self.advance_segment()?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mining::core::memory_guard::MemoryGuard;
    use crate::buffer_pool::{pool::BufferPool, eviction::LruPolicy};

    #[test]
    fn spilled_lists_match_in_memory_lists() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::storage::FileChunkStore::new(dir.path(), false).unwrap());
        let build = |budget: usize| {
            let pool = BufferPool::new_arc(budget, store.clone(), Box::new(LruPolicy::new()));
            let guard = Arc::new(MemoryGuard::new(budget, store.clone()));
            pool.attach_guard(guard.clone());
            let mut b = ItemListBuilder::new(BodyAlloc::new(&pool, &guard), 0.5);
            for tid in 0..20_000u32 {
                for item in [tid % 7, 100 + tid % 13, 1000 + tid % 3] {
                    b.push(item, ULEntry { tid, iutils: item as i64, rutils: tid as i64 }).unwrap();
                }
            }
            let lists = b.finish().unwrap();
            let alloc = BodyAlloc::new(&pool, &guard);
            let flat: Vec<(ItemId, Vec<(u32, i64, i64)>)> = lists.iter().map(|(i, _, body)| {
                let v = alloc.view(body).unwrap();
                (*i, v.iter().map(|e| (e.tid, e.iutils, e.rutils)).collect())
            }).collect();
            drop(lists);
            (flat, guard.used())
        };
        let (big, _) = build(1 << 30);
        let (small, used_after) = build(256 * 1024);
        assert_eq!(big, small);
        assert_eq!(used_after, 0);
    }
}
