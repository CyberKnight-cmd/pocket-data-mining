# Air-HUIM Algorithms & Computational Transformations

## 1. The Breadth-First vs. Depth-First Dilemma

The most significant architectural shift in the Air-HUIM codebase is the transition from Breadth-First Search (BFS) to Depth-First Search (DFS) for combinatorial generation.

### 1.1 The Apriori / Two-Phase Failure
The standard "Two-Phase" algorithm operates on an Apriori foundation. It evaluates the database level-by-level:
1. Find all 1-itemsets ($L_1$)
2. Generate all 2-itemsets ($C_2$) by crossing $L_1 \times L_1$
3. Test $C_2$ against the database to find $L_2$
4. Generate $C_3$ from $L_2$

**The Mathematical Trap**: On dense datasets, or when the `min_utility` threshold is low, $L_2$ generation triggers an $O(N^2)$ combinatorial explosion. For $N=46,000$ items, $C_2$ generates over 1 Billion candidates. Storing 1 Billion vectors in RAM requires roughly 40 Gigabytes. If an engine attempts to buffer these combinations (BFS), it will either cause an OS-level Out-Of-Memory crash, or it will require days of paging to disk to evaluate them.

## 2. The Vertical-DFS Engine (Eclat-Hybrid)
To solve the Two-Phase explosion, Air-HUIM implemented a complete engine rewrite, translating the database into a Vertical Inverted Index and traversing it via DFS.

### 2.1 The Inverted Index (Phase 1 & 2)
Instead of scanning horizontal rows `(Transaction -> Items)`, the engine pre-computes a vertical slice: `(Item -> [Tx1, Tx5, Tx99])`.
To optimize memory, we define a compact struct:
```rust
struct TxEntry {
    tx_idx: u32,
    util: i64,
}
```
At 12 bytes per entry, a 1.1 million transaction database maps cleanly into ~150 MB of contiguous RAM, rigorously tracked by the `MemoryGuard`.

### 2.2 The Fast TID-List Intersection (Phase 3)
The engine generates candidates purely dynamically down a single path (DFS). 
When evaluating the extension of prefix $\{A, B\}$ with item $C$, it does **not** scan the database. It performs a 2-pointer linear intersection between the TID-list of $\{A, B\}$ and the TID-list of $\{C\}$.

Because the TID-lists are strictly sorted by `tx_idx`, the intersection is $O(M+N)$ time complexity and takes microseconds.
```rust
while i < tids.len() && j < ext_list.len() {
    if p.tx_idx == x.tx_idx {
        // Match found! Sum exact utility.
        next_tids.push(...);
        i += 1; j += 1;
    } ...
}
```

### 2.3 Memory Guarantees of Vertical DFS
Because DFS explores one single path at a time (e.g., $A \rightarrow AB \rightarrow ABC$), it only needs to allocate TID-list intersections for the current depth. 
As depth increases, the TID-list intersection strictly shrinks.
The maximum memory footprint per thread is mathematically bounded by:
$$ \sum_{d=1}^{D} |TID_d| \times 12 \text{ bytes} $$
Where $D$ is the maximum transaction length. In practice, this peaks at around **3 to 5 Megabytes per thread**. By operating 16 Rayon threads, the engine evaluates over 1 Billion combinations per second while maintaining a flat, predictable ~300 MB global footprint.

## 3. Algorithm: EFIM (Family 4)
EFIM uses a horizontal database projection method. Unlike the Vertical DFS, EFIM creates dense arrays (Local Utility Arrays) for every recursive call.
Because these arrays must scale with the database size at *every* node in the recursion tree, executing EFIM in parallel across 16 threads causes linear RAM multiplication, immediately breaking the budget.
Therefore, the Concurrency Predictor strictly restricts EFIM to **Single-Threaded** execution, utilizing `ChunkStore` disk-paging to dump projection arrays when the tree gets too deep.

## 4. Algorithm: FHM and TKO (Family 5)
FHM (Fast High-Utility Miner) builds upon the Vertical DFS architecture by utilizing **Utility-Lists**. A Utility-List tracks not just the exact utility of an itemset in each transaction, but also its "remaining utility", allowing for incredibly tight upper-bound pruning without needing full database scans.

**TKO (Top-K in One phase)** is the ultimate evolution of FHM for Top-K mining. It maintains a global, thread-safe Min-Heap of the top $K$ itemsets discovered so far. 
- The minimum utility threshold starts at 0.
- When the Min-Heap reaches size $K$, the global min_utility is dynamically raised to the value of the root of the heap.
- As mining progresses in parallel, the threshold aggressively rockets upward, instantly pruning billions of combinatorial sub-trees across all Rayon threads.
