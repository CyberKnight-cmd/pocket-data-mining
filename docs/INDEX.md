# Air-HUIM Documentation Index

Welcome to the Air-HUIM (Pocket Data Mining) Developer & Engine manual. 
This repository contains the architecture, constraints, and implementations for the world's most aggressive strict-memory High Utility Itemset Mining framework.

### Subsystem Documentation
Please consult the following manuals to understand the core engine decisions:

1. **[Memory Management Subsystem](./MEMORY_MANAGEMENT.md)**
   *The philosophy of zero-trust OS RAM bounding. Details the `MemoryGuard`, atomic multi-threaded short-circuiting, and bounded backpressure output channels.*

2. **[Algorithms & Engine State](./ALGORITHMS.md)**
   *Mathematical breakdowns of the engines. Why traditional Breadth-First Apriori fails, how the Vertical DFS hybrid intersection works in $O(K)$ memory, and the status of EFIM/FHM.*

3. **[Data Layer Subsystem](./DATA_LAYER.md)**
   *The architecture of disk-paging. Details the `ChunkStore` block allocations, binary serialization, and `BufferPool` LRU caching and pinning.*

4. **[Known Issues, Tech Debt, & Top-K Crisis](./UNDERLYING_PROBLEMS.md)**
   *The critical flaws remaining in the framework. A deep dive into the broken fallback threshold generating 65M+ results, the need for a Min-Heap `TKO` implementation, and missing Tree memory trackers.*
