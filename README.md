# Air-HUIM (Pocket Data Mining)

**High-utility itemset mining (HUIM) under a user-set memory budget, aimed at small devices
such as a Raspberry Pi.**

Most HUIM implementations assume the working set fits in RAM and report peak memory after the
fact. Air-HUIM takes the budget as an input (`-b 64` = 64 MB) and keeps the whole process
within it: data structures stay in RAM while they fit and are spilled to disk when they do not,
and results are the same at every budget. It is written in Rust.

## What it provides
- **One memory ledger** (`MemoryGuard`) for everything that grows with the data: algorithm
  structures and buffer-pool pages draw from the same budget, with RAII reservations.
  A margin is held back for memory the ledger cannot see (allocator, I/O buffers, stacks).
- **Budget-driven spilling**: utility lists, projections, tree nodes, candidate sets and the
  EUCS pruning table move to a disk-backed buffer pool only when the budget requires it.
  EUCS is built in as many partitions as needed instead of being truncated, which would make
  pruning wrong.
- **23 algorithms**: Two-Phase, IHUP, HUP-Tree, HUI-Trie, UP-Growth, UP-Growth+, HUI-Miner, FHM,
  FHM+, HUP-Miner, mHUIMiner, EFIM, EFIM-Closed, HAUI-Miner, TKO, TKU, REPT, HUIM-GA,
  HUIM-BPSO, MHUI-ACO, HUIM-MMU, SHUIM, IncFHM — each a distinct implementation (two are partial
  or our own design; see [Implemented.md](Implemented.md)).
- **Observability**: a terminal dashboard (process RSS and ledger vs. budget), a memory log
  (`AIR_HUIM_MEMLOG=1`), and a peak-memory summary at the end of every run.

## Verification
- `tests/budget_integration.rs` compares every algorithm with a brute-force miner on seeded
  random databases, at a generous and a 96 KB budget, with 1 and 4 threads, and checks that
  the ledger stays within budget and that no reservation is left behind.
- Outputs of FHM, EFIM-Closed and HAUI-Miner were checked against SPMF on real datasets.
- `bench/` contains the Raspberry Pi benchmark harness (Air-HUIM vs SPMF, budget sweeps,
  ablation against the earlier implementation); see `bench/collect.py` and `bench/plan.py`.

## Usage
```bash
cargo run --release                       # interactive wizard
cargo run --release -- mine -d foodmart.txt -a fhm -m 3000 -b 64 --threads 4
```
Input is the SPMF utility format (`items:transaction_utility:item_utilities`).

## Known limitations
- The memory bound is verified empirically (tests and measured RSS), not proven: small,
  bounded transient allocations are accounted but can briefly exceed the ledger.
- Every run has a floor (process ~5 MB + margin + a few MB of unspillable state). Utility-list
  and EFIM runs below it are refused up front with the minimum workable budget; admission
  control is not yet applied to the tree, Two-Phase, EFIM-Closed and heuristic engines.
- mHUIMiner uses a pair-level existence filter rather than the paper's IHUP-tree, and HUI-Trie
  is our own design (no published description found) — see [Implemented.md](Implemented.md).
