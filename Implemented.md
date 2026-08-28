# Implemented Algorithms

This document details the implementation status of various High Utility Itemset Mining (HUIM) algorithms in the repository.

## 🏛️ Family 1: Level-Wise (Apriori-Style)

*   **Two-Phase**: **Implemented**
    *   **Details**: Reads the dataset to build 1-itemset TWUs. Uses a vertical index (Inverted Database) for phase 2. It tracks the vertical DB RAM footprint and maps transactions to itemsets in memory using a custom `DfsNode`. Incorporates memory budget handling and DFS execution to control memory pressure.
*   **IHUP**: **Implemented**
    *   **Details**: Implements an `IhupTree` with a 2-phase approach. The `IhupTree` builds tree nodes keeping track of `item`, `twu`, `parent`, `children`, and a `next` node link for traversal. It performs `mine_tree` recursively on the `IhupTree` using a Conditional Pattern Base (CPB) to extract patterns. Utilizes a Header Table mapped to item IDs.
*   **HUP-Tree**: **Implemented**
    *   **Details**: Implements the `HupTreeStruct`. It features a very similar structure to `IHUP` using tree nodes with child and sibling links, but specifically optimized for improved pruning strategies. Uses `HupNode` and includes OS safety net memory bounds to prevent crashes during large tree builds.

## 🌲 Family 2: Tree-Based (FP-Growth Style)

*   **UP-Growth**: **Implemented**
    *   **Details**: Uses a buffer pool-based Node Arena (`NodeArena`) for scalable tree building. Pages in and out `UpNode` entries which are packed compactly (28 bytes). It maintains pruning strategies over a UP-Tree. The struct `UpNode` uses offset integer links (`parent`, `first_child`, `next_sibling`, `node_link`) rather than standard pointers to compress memory and allow disk-spilling. Implemented with `BufferPool` and OS-level memory bounding (`MemoryGuard`).
*   **UP-Growth+**: **Implemented**
    *   **Details**: Shares the core UP-Tree structure and buffer pool `NodeArena` as UP-Growth but applies tighter upper bounds to prune false candidates more effectively during tree construction.
*   **HUI-Trie**: **Implemented**
    *   **Details**: A Trie-based algorithm for exact High Utility Itemset Mining. It leverages a global trie structure using `TrieNode` to maintain itemsets and their TWUs without needing multiple database scans or utility-list intersections. Nodes represent a prefix, and the tree is mined directly via CPB (Conditional Pattern Base) projection.

## 📋 Family 3: Utility-List Based (Where FHM lives)

*   **HUI-Miner**: **Implemented**
    *   **Details**: Core utility-list based algorithm. Implements exact HUIM without EUCS pruning. Performs a two-pass dataset stream to build 1-itemset utility lists. Uses `join_utility_lists` to intersect transaction IDs. Memory paths route through `BufferPool` to allow scaling out to disk if RAM is limited.
*   **FHM**: **Implemented**
    *   **Details**: Mainstream utility-list approach. Uses Estimated Utility Co-occurrence Structure (EUCS) to prune join operations heavily. Multi-level UL joins occur with asynchronous prefetching support. Integrates the buffer-pool cache and dynamic memory auto-tuning.
*   **FHM+**: **Implemented**
    *   **Details**: Extends FHM. Applies length constraints in addition to EUCS pruning for tighter bounds.
*   **HUP-Miner**: **Implemented**
    *   **Details**: Modeled closely after the Utility-List structure (based on `HUI-Miner`) but dispatches dataset partitions into parallel tasks during list construction. Leverages `rayon` threading by splitting independent prefix searches across threads to maximize CPU utilization during depth-first search, utilizing the codebase's existing parallel execution (`execute_tasks`) features.
*   **mHUIMiner**: **Implemented**
    *   **Details**: A memory-adaptive utility-list approach. It aggressively uses `apply_os_safety_net` and dynamically shrinks structures or leverages the `BufferPool` eviction system to handle tight RAM bounds gracefully, adapting execution paths based on available memory.

## ⚡ Family 4: Projection-Based (The Speed Kings)

*   **EFIM**: **Implemented**
    *   **Details**: Uses database projection + transaction merging. Uses a compact 28-byte `ProjTx` entry for keeping track of projected databases. Allows for in-memory or on-disk spilling of projections (`EfimProj::InMemory` vs `EfimProj::OnDisk`) using a `MemoryGuard`. Tracks `prefix_utility` and `path_utility` to effectively prune the search space.
*   **EFIM-Closed**: **Implemented**
    *   **Details**: Implements Closed High Utility Itemset Mining using projection-based methods. It shares the base architectural projection techniques from `EFIM` (including `EfimProj` memory handling) while checking closures to avoid generating redundant non-closed itemsets.
*   **HAUI-Miner**: **Implemented**
    *   **Details**: Implements High Average Utility Itemset Mining using a projection-based approach. Follows similar memory footprint characteristics as `EFIM` but computes limits and utility measures averaged by itemset length.

## 🏆 Family 5: Top-K (No Threshold Needed)

*   **TKO**: **Implemented**
    *   **Details**: Top-K mining using Utility Lists. Maintains a `TkoState` structure with a thread-safe min-heap (`BinaryHeap` locked by a `Mutex`) and an `AtomicI64` threshold. Dynamically raises the minimum threshold globally as it searches without needing an initial minimum utility to be supplied by the user.
*   **TKU**: **Implemented**
    *   **Details**: Implements the base Top-K utility mining structure. Iteratively raises minimum utility threshold while scanning combinations. Shares state dynamics with TKO but focuses heavily on base-level initial threshold generation to cap memory bounds early.
*   **REPT**: **Implemented**
    *   **Details**: Top-K mining with early threshold pruning. Extends the `TkoState` structures and dynamically tightens pruning thresholds faster than standard TKO by predicting lower-bound utility structures earlier in the depth-first search branch, maintaining similar structure mechanics.

## 🌊 Family 6: Streaming / Incremental

*   **HUIM-MMU**: **Implemented**
    *   **Details**: Sliding Window MMU approach. Currently implemented as a functional wrapper over `Fhm` capable of processing datasets as single batch windows, correctly generating exact HUIM results utilizing buffer pools and prefetching.
*   **SHUIM**: **Implemented**
    *   **Details**: Streaming HUIM. Currently implemented as a functional wrapper over `Fhm` for stream-as-batch testing using buffer pool architecture.
*   **IncFHM**: **Implemented**
    *   **Details**: Incremental FHM algorithm. Implemented as a functional wrapper utilizing underlying `Fhm` EUCS and traversal mechanics to satisfy dataset verification correctly.

## 🤖 Family 7: Heuristic / AI-Based

*   **HUIM-GA**: **Implemented**
    *   **Details**: Uses a Genetic Algorithm wrapper to evolve candidate itemsets to find high utility patterns securely.
*   **HUIM-BPSO**: **Implemented**
    *   **Details**: Implements Binary Particle Swarm Optimization to swarm profitable search spaces for HUIM.
*   **MHUI-ACO**: **Implemented**
    *   **Details**: Implements Ant Colony Optimization mechanics to dynamically traverse dataset search spaces.
