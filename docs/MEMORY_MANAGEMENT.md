# Air-HUIM Memory Management Subsystem

## 1. Goal
The user gives one number — the memory budget (e.g. `-b 64`). Every algorithm must keep the
**process RSS** within it, and should *use* it: keep data in RAM while it fits, and spill to
disk only when it does not. Results must be identical at every budget.

## 2. One ledger (`src/mining/core/memory_guard.rs`)
`MemoryGuard` is the single account of everything that scales with the data:

| Charged to the ledger | How |
|---|---|
| Native structures (utility lists, projections, EUCS, tree pages, headers, Top-K heap) | RAII `Reservation`s |
| Buffer-pool frames (data + `FRAME_OVERHEAD` per frame) | the pool charges the ledger when attached (`BufferPool::attach_guard`) |
| Process baseline (binary, libraries) | charged once at startup from `VmRSS` |

`main.rs` gives the ledger `ledger_for_budget(budget)` = budget minus a margin of 7 MB + 3%
(capped at 128 MB and at half the budget) for what the ledger cannot see: allocator metadata and
fragmentation, I/O buffers, thread stacks. Calibrated on chainstore/FHM with streaming joins: peak
RSS exceeds the peak ledger by ~6.5 MB at 24-64 MB budgets and ~30 MB at 1 GB.

### 2.1 Reservations instead of `try_alloc`/`free` pairs
`guard.reserve(n)` returns `Option<Reservation>`; the bytes are released when the reservation is
dropped. This removed a class of bugs where code freed bytes it never reserved (EFIM did this on
every spill, which drifted the ledger downward until it no longer limited anything).

* `reserve(n)` — for structures that have a fallback (spill). Fails above the *native limit*
  (`budget - pool_reserve`, where the pool reserve is 25% of the budget, max 256 MB).
* `reserve_force(n)` — for small, bounded, unavoidable allocations (merge buffers, page reads).
  Accounted, never refused.

### 2.2 The pool yields to native data
Cached pool pages are copies of spilled data and can be reloaded; active native structures
cannot. When a native reservation fails, the ledger asks the pool (its *reclaimer*) to evict
cached pages first. Without this, a pool that cached early spills starved the native side and
forced pathological spilling (one run per pushed entry).

## 2.3 Constant-memory joins
Utility-list bodies longer than one chunk (4,096 entries = 80 KB, smaller under tiny budgets) are
stored as a sequence of chunks (`UlBody::Chunked`). Joins stream: `BodyCursor`s read the prefix,
P·x and P·y lists one chunk at a time and a `BodyWriter` emits the result chunk by chunk, so a
join's working set is about four chunks whatever the list lengths. The 1-itemset list builder
streams spilled runs (written in bounded record pieces) straight into chunk writers.

## 2.4 Admission control (`MiningContext::admit`)
Before mining, the utility-list and EFIM engines estimate what cannot be spilled: a fixed part
(per-item headers and maps, builder buffers, output queue) and a per-worker part (join chunks,
spill page, extension headers / utility bins). Admission then
* refuses the run up front, with the smallest workable budget, if even one worker does not fit
  (e.g. chainstore/FHM: "Use at least -b 19"; 19 MB then runs exactly with a 15.4 MB peak);
* lowers the thread count until the per-worker sets use at most half of what is free.
Below that floor the process would otherwise exceed its budget (or be killed by a kernel limit).

The practical floor is dominated by the process itself (~5 MB resident before any work) and the
margin; on chainstore the unspillable mining state is under 3 MB.

## 3. Buffer pool (`src/buffer_pool/pool.rs`)
* Charges the shared ledger; if it cannot make room (everything pinned or the budget held by
  native structures), `insert_page` **writes through** to disk instead of caching.
* Eviction samples 16 of the oldest frames and lets the policy choose among them — O(1) per
  eviction instead of copying the metadata of every frame.
* `discard(id)` / `OwnedPage`: pages of dead DFS branches are dropped without being flushed and
  deleted from disk. Previously they stayed in the pool and were written out on eviction.
* Race fixes: a page is removed only if still unpinned and clean (`remove_if`), and two threads
  loading the same page share one frame instead of one overwriting the other.

## 4. Spill policy
Data is kept in RAM while the budget allows and spilled when it does not — never by a fixed size
threshold. Spilled small lists are packed into shared 256 KB pages (`SpillArena`) so a starved
search does not create one file and one frame per tiny list.

| Structure | In RAM while it fits | Otherwise |
|---|---|---|
| 1-itemset utility lists / TID lists (`ItemListBuilder`) | buffered | sorted runs spilled, merged item-by-item |
| Derived utility lists (`BodyAlloc::make_body`) | `InMemory` + reservation | packed / own pool page, read zero-copy |
| EUCS | built in as many partitions as needed, compacted per partition | disabled (never truncated — a partial EUCS prunes wrongly) |
| EFIM database (`PagedDb`) | RAM segments | pool pages |
| EFIM projections | reservation | pool page |
| Tree nodes (`NodeArena`) | RAM pages | pool pages |
| Two-phase candidates | — | always on disk (`ItemsetSpool`), verified in budget-sized batches |
| Filtered DB for re-scans (`TxSpool`) | — | binary pages in the chunk store |

## 5. Allocator
glibc keeps freed memory in per-thread arenas; with a churning DFS that left RSS ~100 MB above
live data. `tune_allocator_for_budget()` (called first thing in `main`) sets one malloc arena and
low trim/mmap thresholds; `release_free_memory()` trims after phases that free a lot. Set
`AIR_HUIM_MALLOC_ARENAS=N` to trade memory for ~10% speed with more threads.

## 6. Observability
* The TUI shows process RSS and ledger use against the budget, plus the pool's share.
* `AIR_HUIM_MEMLOG=1` prints `rss / ledger (peak) / pool / stage` once a second to stderr.
* Every run ends with `Memory: budget … | ledger peak … | process peak RSS …`.

## 7. Verification
`tests/budget_integration.rs` runs every algorithm on seeded random databases (including a
retail-like "cheap but frequent item" case) at a generous and a 96 KB budget, 1 and 4 threads,
and checks: exact equality with a brute-force miner, ledger peak within budget, and zero
reservations left after the run. `bench/pi_bench.py` measures real RSS against SPMF.

## 8. Output queue
Parallel runs send HUIs to one writer thread over a bounded channel. Its capacity scales with the
budget (~1/64 of it) and is reserved up front, so a slow disk applies backpressure instead of
growing memory.
