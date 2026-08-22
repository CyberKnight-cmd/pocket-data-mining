# Air-HUIM Memory Management Subsystem

## 1. Rationale and Philosophy
The most critical bottleneck in High Utility Itemset Mining (HUIM) is memory consumption. Datasets in the retail space (e.g., `chainstore.txt`) are incredibly sparse, but the combinatorial permutations of frequent items generate search spaces bordering on the trillions of nodes. 

Historically, algorithms like Apriori, EFIM, and FHM were developed in academic vacuums where researchers simply provisioned 64GB-128GB RAM servers to hold intermediate projection arrays, EUCS (Estimated Utility Co-occurrence Structures), and utility-lists. Air-HUIM (Pocket Data Mining) was designed for embedded systems, edge devices, and budget hardware. Therefore, the memory management subsystem was built as a hostile, zero-trust enforcement layer.

## 2. The MemoryGuard Architecture (`src/mining/core/memory_guard.rs`)
At the core of the subsystem is the `MemoryGuard`. It is not a garbage collector; it is a strict, atomic accounting ledger.

### 2.1 The Atomic Ledger
`MemoryGuard` wraps an `AtomicUsize` representing the current bytes allocated. When the user sets a budget (e.g., 2048 MB) via the CLI, the Guard treats this as a hard physical limit.
Algorithms cannot natively allocate large `Vec`s or `HashMap`s without explicitly requesting permission:
```rust
if guard.try_alloc(bytes) {
    // proceed with allocation
} else {
    // OOM sequence triggered
}
```

### 2.2 Parallel Thread Safety and Short-Circuiting
Because Air-HUIM is heavily multi-threaded (using `Rayon`), memory bursts can happen concurrently across 16 cores. 
During the initial implementation of the Vertical DFS engine, we encountered a 2.7 GB RAM spike. The root cause was Rayon's parallel `.unzip()` mapping 46,000 iterators into memory simultaneously before the `MemoryGuard` could evaluate the flattened array.

**The Fix (The Atomic OOM Flag):**
To combat parallel memory bloat, `MemoryGuard` logic is injected directly into the lowest levels of the Rayon closures, paired with a global `AtomicBool` flag. The exact microsecond any of the 16 threads is denied an allocation, it flips the `oom_flag` to `true` (via `Ordering::Relaxed`). Every other thread checks this flag at the start of their loop and instantly aborts, collapsing the execution tree and perfectly capping RAM.

## 3. The Bounded Backpressure Output Queue
In iteration v0.2, the Engine successfully evaluated 121 Billion fast-path RAM reads in 12 seconds, discovering 65 Million High Utility Itemsets. 
This triggered a massive 9 GB RAM leak. The algorithms were perfectly memory-stable, but the **Output Pipeline** failed.

### 3.1 The Unbounded Channel Trap
The engine uses an MPSC (Multi-Producer, Single-Consumer) queue to funnel HUIs from 16 fast CPU threads to 1 slow Disk I/O thread. Originally, this was an `mpsc::unbounded()` crossbeam channel. Because the CPU threads discovered 5 million HUIs per second, but the disk could only write 100,000 per second, 64 million vectors piled up in the channel's memory buffer.

### 3.2 The Bounded Enforcement
We rewrote the pipeline to use `mpsc::bounded(100_000)`. 
This creates **Disk Backpressure**. When the queue reaches 100,000 items (taking exactly ~4 MB of RAM), the channel blocks. The 16 Rayon threads are forced to sleep, stalling their DFS traversal until the disk writer flushes the buffer. This guarantees that the framework paces itself precisely to the hardware limitations of the user's disk drive, keeping OS RAM flat and completely eliminating the memory leak.
