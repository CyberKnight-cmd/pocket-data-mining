# 🚀 Air-HUIM (Pocket Data Mining)

**The world's first strict-memory, embedded-ready High Utility Itemset Mining (HUIM) framework.**

Air-HUIM was built to solve a singular, massive problem in the data mining industry: **Combinatorial RAM Explosions**. Traditional mining algorithms (like Apriori, EFIM, and FHM) are notoriously memory-hungry. When run on massive retail datasets, they routinely consume 40+ GB of RAM, crashing standard developer machines and rendering them entirely useless for embedded devices, edge computing, or budget hardware.

Air-HUIM flips this paradigm upside down. Written in Rust, it enforces a strict, user-defined memory ceiling (e.g., 200 MB, 2 GB) at the OS level. It uses lightning-fast Disk Paging (BufferPool) and Vertical Depth-First Search (DFS) transformations to guarantee that the algorithms never OOM, gracefully pacing 16-core parallel throughput to exactly match the hardware's limits.

### ✨ Key Features
- **🛡️ MemoryGuard**: A global, atomic RAM enforcer. Algorithms mathematically cannot allocate memory beyond the user's CLI-specified budget.
- **⚡ Vertical DFS Hybrids**: Traditional Breadth-First algorithms (like Apriori/Two-Phase) have been fundamentally rewritten into Vertical Inverted Index DFS engines, dropping RAM footprints from Gigabytes to single-digit Megabytes.
- **💾 Embedded BufferPool & ChunkStore**: Algorithms that require massive projection arrays (like EFIM) dynamically spill out-of-core to a custom disk-backed chunk store when RAM gets tight.
- **📊 Real-Time TUI Dashboard**: A stunning terminal UI providing live diagnostics of DFS Depth, RAM Fast-Path Reads/Writes, BufferPool hits/misses, and precise Memory Footprints.
- **🧵 Bounded Parallelism**: Fully multi-threaded with Rayon, safely capped by bounded crossbeam channels to apply backpressure to disk writers, ensuring output queues never bloat RAM.

### 🛠️ Usage
```bash
cargo run --release
```
Follow the interactive wizard to select your Mining Algorithm, Dataset, Hardware Threading limits, and strict RAM Budget. Watch the dashboard melt the dataset!

---

*Built with strict memory awareness by Antigravity and the systems engineering team.*
