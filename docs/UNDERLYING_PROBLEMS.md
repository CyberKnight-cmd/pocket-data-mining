# Known Issues, Tech Debt, and The Top-K Crisis

This document outlines the deepest underlying flaws in the Air-HUIM framework that developers and agents must address. 

## 1. The Top-K Threshold Crisis (CRITICAL)

### 1.1 The Symptom
If you run `Two-Phase` (Vertical DFS) on `chainstore.txt` and select `Top-K Mining` with $K = 1000$, the engine will discover over **65 Million High Utility Itemsets**. It will run for minutes, flooding the disk with gigabytes of output text.
The framework's primary flaw is that it generates tens of millions of results when the user explicitly only asked for 1,000.

### 1.2 The Root Cause
The algorithms categorized under Family 5 (e.g., `TKO`, `TKU`) are the only mathematical engines capable of true Top-K mining. Because they are not yet written, the CLI Wizard uses a "fallback" mechanic for traditional algorithms (Two-Phase, EFIM, FHM). 
When Top-K is selected on a non-Top-K algorithm, the engine defaults to a hardcoded baseline: `min_utility = 1000`.

In retail datasets like `chainstore.txt`, the Total Database Utility is 2.6 Billion. A utility threshold of `1000` is effectively zero. It asks the DFS Engine to validate almost the entire combinatorial power set of the database. The DFS engine is so incredibly optimized that it actually survives this computational nightmare (evaluating 150+ Billion nodes without crashing), but it results in a flooded disk and a meaningless output file.

### 1.3 The Required Implementation (`TKO`)
To solve this, we must build `src/mining/algorithms/tko.rs` (Top-K in One phase). 
*   **The Priority Queue**: `TKO` must maintain a `BinaryHeap` (Min-Heap) of size $K$ in memory.
*   **Dynamic Pruning**: The baseline `min_utility` starts at 0. As the engine discovers HUIs, they are pushed to the Min-Heap. The moment the Min-Heap reaches size $K$ (e.g., 1000), the global `min_utility` is permanently raised to the value of the *lowest* item in the heap (the root of the Min-Heap).
*   **The Effect**: As mining progresses, the threshold aggressively rockets upward (e.g., from 0 to 500,000 to 1,200,000). This dynamically prunes billions of sub-trees in the DFS traversal, allowing the algorithm to finish in seconds and output exactly 1,000 items.
*   **Parallel Synchronization**: In a 16-thread Rayon environment, this `min_utility` threshold must be wrapped in an `Arc<AtomicI64>`. When Thread A finds a massive HUI, it atomic-maxes the global threshold, instantly allowing Thread B to prune its current branch.

## 2. Tree Memory Tracking Bypass (HIGH)

### 2.1 The Flaw in FHM / UP-Growth
While Two-Phase and EFIM are rigorously integrated into `MemoryGuard`, the Tree algorithms (Family 2 & 3) rely on massive node-based structures (e.g., HUP-Tree, Header Tables, EUCS maps). 
Rust's `Box<Node>` and `HashMap` allocations are currently bypassing the `MemoryGuard`. If run with 16 threads, the recursive allocation of Conditional UP-Trees will silently leak beyond the OS Budget and trigger a hard kernel kill.

### 2.2 The Solution
Tree structures must be rewritten to implement explicit byte-tracking on `insert()`. Alternatively, the nodes must be serialized to the `ChunkStore` via the `BufferPool` when RAM budgets are tight.

## 3. The AI Concurrency Predictor Stub (MEDIUM)
The framework currently uses static math in `main.rs` to warn users about thread scaling:
```rust
// Static heuristic formula
let est_ram_per_thread = (stats.total_utility / stats.density) ... 
```
This fails to account for combinatorial density explosions. Air-HUIM is slated to implement an embedded Machine Learning model (Meta-Learning Predictor) in `src/mining/core/predictor.rs` that trains on dataset characteristics to output safe Rayon thread counts.
