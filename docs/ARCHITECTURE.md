# Air-HUIM Architecture

This document provides a deep-dive architectural view of the core engine components driving the Air-HUIM framework.

## 1. MemoryGuard (`src/mining/core/memory_guard.rs`)
The `MemoryGuard` is the absolute heart of the framework. Because traditional HUIM algorithms are mathematically prone to combinatorial explosions, `MemoryGuard` exists to act as an OS-level RAM kill-switch and tracker.

*   **Mechanism**: Uses a global `Arc<AtomicUsize>` to track every byte of dynamic allocation.
*   **Usage**: Algorithms must call `try_alloc(bytes)`. If this returns `true`, the algorithm is permitted to push to its vectors. If `false`, the algorithm must either gracefully halt, block, or trigger the `BufferPool` to page memory to disk.
*   **Rayon Thread Safety**: The atomic nature allows thousands of Rayon threads to concurrently request memory. If the global pool exceeds the user's budget (e.g., 2048 MB), all threads are instantly blocked via atomic short-circuits.

## 2. The BufferPool & ChunkStore (`src/storage/chunk_store.rs`, `src/buffer_pool/`)
To support running on 100MB embedded devices, algorithms that traditionally build massive in-memory projection trees (like FHM and EFIM) use the Data Layer.

*   **ChunkStore**: A disk-backed paging file. When `MemoryGuard` denies an allocation, algorithms serialize their arrays (Chunks) to a `PageId` and write it to disk.
*   **BufferPool**: An LRU-cached page manager. If an algorithm requests a `PageId` that isn't in RAM, it evicts the least recently used page, writes it to the ChunkStore, and loads the requested page.

## 3. MiningContext & Execution Pipeline (`src/mining/core/context.rs`)
The pipeline connects the UI to the underlying mathematical engines.

*   **`ctx.execute_tasks`**: A highly optimized execution function. It takes a list of tasks, spans a bounded `Rayon` thread pool perfectly matching `ctx.threads`, and provides each thread with a `WriterProxy`.
*   **Bounded Backpressure**: The `WriterProxy` sends discovered High Utility Itemsets (HUIs) through a `crossbeam_channel::bounded(100_000)` queue. This is a critical architectural decision: if the 16 fast RAM-bound CPU threads find HUIs faster than the single Disk Writer thread can write them, the bounded queue fills up, forcing the CPU threads to pause. This prevents the output queue from leaking gigabytes of RAM.

## 4. Precomputation & Predictor (`src/mining/core/dataset_stats.rs`)
Before any algorithm runs, the dataset is streamed once sequentially to compute:
*   Total Transactions ($|D|$)
*   Unique Items ($|I|$)
*   Average / Max Transaction Length
*   Total Database Utility

These stats feed the **Concurrency Predictor**, a mathematical model that attempts to warn the user if their selected thread count and algorithm choice will violate their Memory Budget.
