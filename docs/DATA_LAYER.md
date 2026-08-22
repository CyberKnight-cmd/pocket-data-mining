# Air-HUIM Data Layer Subsystem

## 1. Overview
The Data Layer is responsible for abstracting physical hardware limitations (specifically volatile RAM limits) away from the mathematical mining engines. It consists of the `BufferPool` and the `ChunkStore`.

## 2. ChunkStore (`src/storage/chunk_store.rs`)
The `ChunkStore` is the interface to the non-volatile storage (disk). It treats the hard drive as an infinite, slow-access RAM extension.

### 2.1 Paged Serialization
When an algorithm (like EFIM) generates a massive database projection array that exceeds the `MemoryGuard` budget, it serializes the array into binary format and passes it to the `ChunkStore`.
The `ChunkStore` allocates a sequential `PageId` and flushes the binary blob to a temporary physical file (`.tmp_chunks/`). 
This guarantees that algorithms can recursively explore trees infinitely deep without ever triggering an OOM crash.

## 3. BufferPool (`src/buffer_pool/`)
Disk I/O is the enemy of computational speed. To mitigate the latency of `ChunkStore` reads, the framework utilizes an LRU (Least Recently Used) `BufferPool`.

### 3.1 LRU Eviction & Pinning
The `BufferPool` sits between the Algorithms and the `ChunkStore`. It is granted its own strict byte budget (e.g., 500 MB out of the 2048 MB global limit).
*   **Hits**: When an algorithm requests `PageId(42)`, the BufferPool checks its in-memory Hash Map. If present, it returns a fast reference.
*   **Misses & Eviction**: If `PageId(42)` is on disk, the BufferPool must load it. If the pool is at its 500 MB capacity, it identifies the Least Recently Used unpinned page, evicts it from RAM, and loads the new page.
*   **Pinning**: Algorithms can "pin" pages (e.g., the root Header Table of an FP-Tree) to guarantee they are never evicted by the LRU algorithm, ensuring $O(1)$ access for critical path structures.
