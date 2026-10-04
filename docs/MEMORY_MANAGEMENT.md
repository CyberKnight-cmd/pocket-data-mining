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
Every engine, right after its first pass (before anything that could spill is loaded), estimates
what it cannot spill: a fixed part and a per-worker part, both as functions of the budget.
Admission then
* refuses the run up front, with the smallest workable budget, if even one worker does not fit
  (e.g. chainstore/FHM: "Use at least -b 19"; 19 MB then runs exactly with a 15.4 MB peak);
* lowers the thread count until the per-worker sets use at most half of what is free.
Below that floor the process would otherwise exceed its budget (or be killed by a kernel limit).

| Engine (algorithms) | Fixed | Per worker |
|---|---|---|
| Utility lists (FHM, FHM+, HUI-Miner, HUP-Miner, mHUIMiner, TKO, REPT, HAUI-Miner, HUIM-MMU, IncFHM; SHUIM per window) | per-item list headers, builder segments, DB spool (EUCS / pairs), output queue | one streaming join (4 chunks), arena page, extension headers |
| Two-Phase | per-item list headers, builder segments, output queue | one streaming join, arena page, list headers on the DFS path |
| EFIM | renaming maps, su / projection-size arrays | utility bins, pinned and open segments, smallest first-level batch |
| EFIM-Closed | per-item maps, DB builder segment, output queue | 2 x the largest projection (a spilled projection is loaded while its child is built), lu/su maps, one DB segment, smallest batch |
| Trees (IHUP, HUI-Trie, HUP-Tree, UP-Growth, UP-Growth+, TKU) | TWU map of all items, per-promising-item maps, DB and candidate spool buffers, two node pages, output queue | smallest phase-2 candidate batch, spool read buffers |
| Heuristics (HUIM-GA, HUIM-BPSO, MHUI-ACO) | per-item index and arrays, smallest cache / found-set, pass-2 buffers | one chunk per selected item (streaming intersection), one DB segment |

Buffers that used to be fixed now scale with the budget, so the floor scales too: spool segments
(1/32 of the budget, 1 KB-1 MB), node pages (1/64, 4-64 KB), DB segments (1/64, 4-256 KB),
minimum phase-2 batch (1/16, 8-128 KB), EFIM's minimum first-level batch (2 segments).
`tests/budget_integration.rs` checks that all 23 algorithms refuse cleanly at 8 KB, and run
exactly at 96 KB with the ledger peak at most 64 KB per thread above the budget.

The practical floor is dominated by the process itself (~5 MB resident before any work) and the
margin; on chainstore the unspillable mining state is under 3 MB.

Heuristics keep the set of HUIs already written (so each is output once) exactly while the budget
lets it grow; then it becomes a Bloom filter of the same size. A false positive can only skip a
genuinely new HUI (recall), never output a wrong or duplicate one (precision).

## 2.5 Recompute instead of spill (cost-based rematerialisation)
A derived utility list can always be rebuilt from its parents: list(P·x·y) =
join(list(P), list(P·x), list(P·y)), and those parents are alive higher up the search. When a new
list does not fit the budget, the engine may *drop* it (keeping only its header and a recipe)
instead of writing it to disk. A dropped list read as P·y is produced on the fly by a lazy join
stream over its parents (no buffer); when its own turn comes as P·x it is materialised once.
A cost model decides per list, using costs measured during the run (ns per joined entry, ns per
byte written / read): spill ≈ size × (write × AIR_HUIM_WRITE_WEIGHT + reads × read), recompute ≈
uses × parent sizes × CPU cost. `AIR_HUIM_REMAT=auto|off|always`; each run reports lists dropped,
bytes not written and entries recomputed. On flash storage (SD cards) this trades CPU for writes.

## 2.6 Tree miners: partition projection instead of spilled trees
A prefix tree that does not fit cannot be mined from spilled node pages: every parent or
node-link step can be a page load. UP-Growth+ on retail at 32 MB made 3.3M pool misses and read
207 GB of pages for a 26 MB tree (140 s, against 2.8 s at 64 MB). When the tree's size bound
(one node per item occurrence) exceeds the free budget, the tree miners (IHUP, HUI-Trie,
HUP-Tree, UP-Growth, UP-Growth+, TKU) switch to partition projection
(`tree_partition.rs`, after Han, Pei & Yin 2000):

* each filtered transaction, in tree order, is written to the partition of its last item;
* items are processed from the bottom of the tree order up: partition *i* holds exactly the
  transactions containing *i*, cut after *i*; a small tree of them is built in RAM and *i* is
  mined on it with the unchanged per-item mining code; then each row loses *i* and moves to the
  partition of its new last item;
* partitions are grouped into about sqrt(n) rank ranges so only about 2 sqrt(n) write buffers
  are open, sized from a quarter of the free budget.

All I/O is sequential. A partition tree has the same node TWUs, node utilities, counts and
utility vectors for the mined item as the global tree; for UP-Growth+ / TKU an ancestor's
minimal node utility is taken over the partition's transactions only, which is at least as large
(a tighter, still valid bound). Phase 2 verification makes the output exact either way.
`AIR_HUIM_TREE_PARTITION=auto|always|off`.

Measured (laptop, 4 threads requested, 300 s cap; identical output at every budget and mode):

| dataset / algorithm | off, 32 MB | auto, 24 MB | auto, 32 MB | auto, 64 MB | auto, 256 MB | off, 256 MB |
|---|---|---|---|---|---|---|
| chainstore / UP-Growth+ | > 300 s | 17.8 s | 15.5 s | 13.5 s | 14.4 s | 32.1 s |
| chainstore / UP-Growth | > 300 s | 18.2 s | 15.9 s | 14.0 s | 15.2 s | 29.9 s |
| chainstore / IHUP | > 300 s | 18.2 s | 15.9 s | 14.8 s | 36.2 s | 42.2 s |
| chainstore / HUI-Trie | > 300 s | 25.6 s | 23.2 s | 17.8 s | 48.2 s | 45.1 s |
| chainstore / HUP-Tree | > 300 s | 20.7 s | 17.8 s | 15.2 s | 17.2 s | > 300 s |
| chainstore / TKU | > 300 s | 272.9 s | 280.1 s | 280.6 s | 60.8 s | 78.1 s |
| kosarak / UP-Growth+ | > 300 s | 72.9 s | 59.2 s | 53.5 s | 48.6 s | 72.9 s |
| retail / UP-Growth+ | 225.2 s | 6.0 s | 5.3 s | 3.7 s | 3.6 s | 3.7 s |

Partitions are often faster than the global tree even when it fits (small trees are
cache-friendly). Still slow and unrelated to memory (they also time out at 256 MB with
partitions off): IHUP, HUI-Trie and HUP-Tree on retail and kosarak, UP-Growth on retail, TKU on
kosarak (TWU-only bounds give too many candidates at these thresholds); TKU on chainstore below
128 MB (its phase-2 verification runs in many small batches).

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
| Tree nodes (`NodeArena`) | RAM pages | pool pages; a global tree that does not fit is not built (partition projection, 2.6) |
| Two-phase candidates | — | always on disk (`ItemsetSpool`), verified in budget-sized batches |
| Filtered DB for re-scans (`TxSpool`) | — | binary pages in the chunk store |

## 5. Allocator
glibc keeps freed memory in per-thread arenas; with a churning DFS that left RSS ~100 MB above
live data. `tune_allocator_for_budget()` (called first thing in `main`) sets one malloc arena and
low trim/mmap thresholds; `release_free_memory()` trims after phases that free a lot. Set
`AIR_HUIM_MALLOC_ARENAS=N` to trade memory for ~10% speed with more threads.

With several threads in one arena, long-lived blocks (cached pages, in-RAM lists) interleave with
short-lived join chunks, and the freed holes stay resident (glibc returns only the top of the
heap on `free`): Two-Phase on chainstore at 64 MB with 4 threads reached 80 MB RSS with a 41 MB
ledger. A background trimmer (`spawn_heap_trimmer`) calls `malloc_trim(0)`, which releases free
pages anywhere in the heap, whenever RSS exceeds the ledger by more than max(8 MB, budget / 10);
checked every 250 ms. (Lowering the mmap threshold to 64 KB also fixed it, but cost ~80% run
time.) `AIR_HUIM_MMAP_THRESHOLD` overrides the threshold for experiments.

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
