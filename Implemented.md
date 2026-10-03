# Implemented Algorithms

> **Status (2026-10-03):** all 23 algorithms are distinct implementations, each verified against a
> brute-force miner in `tests/budget_integration.rs` (exact results, or — for the three heuristics —
> only true HUIs with exact utilities). Two are partial or our own design and are labelled below:
> **mHUIMiner** (pair-level existence filter instead of the IHUP-tree) and **HUI-Trie** (no published
> description found; our design).
>
> **Memory:** every algorithm below runs within the user's memory budget (see
> `docs/MEMORY_MANAGEMENT.md`) and is checked for exact results at a generous and a tiny budget
> by `tests/budget_integration.rs`. Several algorithms share an engine; where an algorithm is
> currently another algorithm's search under its own name, this is stated explicitly below.

This document details the implementation status of various High Utility Itemset Mining (HUIM) algorithms in the repository.

## 🏛️ Family 1: Level-Wise (Apriori-Style)

*   **Two-Phase**: **Implemented**
    *   **Details**: Reads the dataset to build 1-itemset TWUs. Uses a vertical index (Inverted Database) for phase 2. It tracks the vertical DB RAM footprint and maps transactions to itemsets in memory using a custom `DfsNode`. Incorporates memory budget handling and DFS execution to control memory pressure.
*   **IHUP**: **Implemented** — two-phase: TWU prefix tree (phase 1), candidates verified with a rarest-item index (phase 2).
    *   **Details**: Implements an `IhupTree` with a 2-phase approach. The `IhupTree` builds tree nodes keeping track of `item`, `twu`, `parent`, `children`, and a `next` node link for traversal. It performs `mine_tree` recursively on the `IhupTree` using a Conditional Pattern Base (CPB) to extract patterns. Utilizes a Header Table mapped to item IDs.
*   **HUP-Tree**: **Implemented (distinct, one-phase)** — HUP-Growth (Lin, Hong & Lu, 2011): every node stores a utility vector for itself and its ancestors (kept in a budget-aware arena), so exact utilities come from the tree; no candidate phase.
    *   **Details**: Implements the `HupTreeStruct`. It features a very similar structure to `IHUP` using tree nodes with child and sibling links, but specifically optimized for improved pruning strategies. Uses `HupNode` and includes OS safety net memory bounds to prevent crashes during large tree builds.

## 🌲 Family 2: Tree-Based (FP-Growth Style)

*   **UP-Growth**: **Implemented** — DGU/DGN in the global tree; DLU/DLN in local trees using global minimum item utilities.
    *   **Details**: Uses a buffer pool-based Node Arena (`NodeArena`) for scalable tree building. Pages in and out `UpNode` entries which are packed compactly (28 bytes). It maintains pruning strategies over a UP-Tree. The struct `UpNode` uses offset integer links (`parent`, `first_child`, `next_sibling`, `node_link`) rather than standard pointers to compress memory and allow disk-spilling. Implemented with `BufferPool` and OS-level memory bounding (`MemoryGuard`).
*   **UP-Growth+**: **Implemented (distinct)**
    *   **Details**: UP-Tree nodes now store a count and a minimal node utility (mnu); DLU/DLN subtract node-level minimums instead of global ones, giving fewer candidates than UP-Growth.
*   **HUI-Trie**: **Implemented (our design)** — no published description found; IHUP's phase 1, with phase 2 counting candidates through a prefix trie walked once per transaction.
    *   **Details**: A Trie-based algorithm for exact High Utility Itemset Mining. It leverages a global trie structure using `TrieNode` to maintain itemsets and their TWUs without needing multiple database scans or utility-list intersections. Nodes represent a prefix, and the tree is mined directly via CPB (Conditional Pattern Base) projection.

## 📋 Family 3: Utility-List Based (Where FHM lives)

*   **HUI-Miner**: **Implemented**
    *   **Details**: Core utility-list based algorithm. Implements exact HUIM without EUCS pruning. Performs a two-pass dataset stream to build 1-itemset utility lists. Uses `join_utility_lists` to intersect transaction IDs. Memory paths route through `BufferPool` to allow scaling out to disk if RAM is limited.
*   **FHM**: **Implemented**
    *   **Details**: Mainstream utility-list approach. Uses Estimated Utility Co-occurrence Structure (EUCS) to prune join operations heavily. Multi-level UL joins occur with asynchronous prefetching support. Integrates the buffer-pool cache and dynamic memory auto-tuning.
*   **FHM+**: **Implemented**
    *   **Details**: Extends FHM. Applies length constraints in addition to EUCS pruning for tighter bounds.
*   **HUP-Miner**: **Implemented (distinct)**
    *   **Details**: HUI-Miner search plus HUP-Miner's two strategies: PU-prune (per-partition utility sums of each list; a join is skipped when the partition-wise bound of P·x·y is below the threshold) and LA-prune (a join is abandoned once its running bound drops below it). Partitions: `AIR_HUIM_PARTITIONS` (default 8).
*   **mHUIMiner**: **Implemented (partially faithful)**
    *   **Details**: HUI-Miner that does not build utility lists for itemsets absent from the database. The paper uses an IHUP-tree for this at the level of whole prefixes; here a budget-aware pair-existence structure does it at the pair level. Same output as HUI-Miner, fewer joins on sparse data.

## ⚡ Family 4: Projection-Based (The Speed Kings)

*   **EFIM**: **Implemented (full)** — item renaming, high-utility database projection with transaction merging, utility-bin arrays, su/lu pruning (`components/efim_engine.rs`). 6–145x faster than the previous offset-based version on dense data, identical output.
    *   **Details**: Uses database projection + transaction merging. Uses a compact 28-byte `ProjTx` entry for keeping track of projected databases. Allows for in-memory or on-disk spilling of projections (`EfimProj::InMemory` vs `EfimProj::OnDisk`) using a `MemoryGuard`. Tracks `prefix_utility` and `path_utility` to effectively prune the search space.
*   **EFIM-Closed**: **Implemented**
    *   **Details**: Implements Closed High Utility Itemset Mining using projection-based methods. It shares the base architectural projection techniques from `EFIM` (including `EfimProj` memory handling) while checking closures to avoid generating redundant non-closed itemsets.
*   **HAUI-Miner**: **Implemented (distinct)**
    *   **Details**: High *average*-utility itemset mining: X is output when u(X)/|X| >= min_utility. Average-utility lists carry each transaction's maximum item utility; pruning uses the anti-monotone auub bound (plus LA-prune). Output lines carry the total utility (`#UTIL`); the average is u/|X|. Matches SPMF's HAUI-Miner on foodmart (1,369 itemsets).

## 🏆 Family 5: Top-K (No Threshold Needed)

*   **TKO**: **Implemented**
    *   **Details**: Top-K mining using Utility Lists. Maintains a `TkoState` structure with a thread-safe min-heap (`BinaryHeap` locked by a `Mutex`) and an `AtomicI64` threshold. Dynamically raises the minimum threshold globally as it searches without needing an initial minimum utility to be supplied by the user.
*   **TKU**: **Implemented (distinct)** — Top-K on the UP-Tree: pre-evaluation (PE) border from exact 1-/2-itemset utilities, UP-Growth+ phase 1 at the border, phase 2 with SE skipping as the border rises.
    *   **Details**: Implements the base Top-K utility mining structure. Iteratively raises minimum utility threshold while scanning combinations. Shares state dynamics with TKO but focuses heavily on base-level initial threshold generation to cap memory bounds early.
*   **REPT**: **Implemented (distinct)** — TKO's utility-list Top-K search started at a pre-evaluated threshold (K-th best exact 1-/2-itemset utility) instead of 0.
    *   **Details**: Top-K mining with early threshold pruning. Extends the `TkoState` structures and dynamically tightens pruning thresholds faster than standard TKO by predicting lower-bound utility structures earlier in the depth-first search branch, maintaining similar structure mechanics.

## 🌊 Family 6: Streaming / Incremental

*   **HUIM-MMU**: **Implemented (distinct)** — multiple minimum utilities (Lin et al., KBS 2016): mu(i) = max(min_utility, β·u(i)) with β = `AIR_HUIM_MMU_BETA` (default 0.1); X qualifies when u(X) ≥ min mu over X; items processed in ascending mu (sorted downward closure).
    *   **Details**: Mines each 1000-transaction window (sliding by 500) with FHM and appends each window's HUIs. Low memory use is inherent to the window size. Temporary window files are written next to the output (not `/tmp`, which is RAM-backed on Raspberry Pi OS).
*   **SHUIM**: **Implemented** — sliding-window stream mining: windows of `AIR_HUIM_WINDOW` transactions (default 1000) sliding by half a window, each mined exactly; one block of HUIs per window.
*   **IncFHM**: **Implemented (EIHI-style incremental)** — the database arrives in `AIR_HUIM_BATCHES` batches (default 10); after each, only itemsets occurring in the new transactions are mined; final output = HUIs of the whole database.

## 🤖 Family 7: Heuristic / AI-Based

*   **HUIM-GA / HUIM-BPSO / MHUI-ACO**: **Implemented (distinct, approximate)**
    *   **Details**: Genetic algorithm, binary particle swarm and ant colony searches over the promising items, sharing one evaluation engine (`components/heuristic_engine.rs`). Fitness is the exact utility from TID lists, so every reported itemset is a true HUI with its exact utility; recall is below 100%.
    *   **Transaction-guided search**: itemsets are built from transactions (random subsets of a random transaction; extensions taken from a random transaction containing the itemset; GA/BPSO candidates repaired until they occur), so no evaluation is wasted on itemsets absent from the data. GA/BPSO keep their populations diverse by mutating already-evaluated itemsets; ACO's pheromone has a floor and rewards newly found HUIs.
    *   **Recall on foodmart (85,034 HUIs)**: 2,000 iterations: GA 8.7%, BPSO 8.3%, ACO 13.0% (previously 1.6%, 1.4%, 0.08%); 20,000: 50% / 35% / 43%; 100,000: 65% / 60% / 63%. Seeded and reproducible (`AIR_HUIM_SEED`, `AIR_HUIM_ITERS`, `AIR_HUIM_POP`, `AIR_HUIM_MAXLEN`).
