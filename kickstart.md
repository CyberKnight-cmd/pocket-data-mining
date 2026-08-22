# Air-HUIM: Exact High-Utility Itemset Mining Under Severe Memory Constraints

## Project Overview

Design and implement a research-grade system called **Air-HUIM** — *Adaptive/Out-of-core Resource-aware High-Utility Itemset Mining* — that investigates whether **exact High-Utility Itemset Mining (HUIM)** can be performed on datasets substantially larger than the available RAM without sacrificing correctness.

The central idea is to **decouple the size of the mining dataset from the amount of RAM required to execute the mining algorithm**.

Traditional HUIM implementations commonly assume that large portions of the mining data structures can remain resident in memory. As transaction databases grow, utility lists, projected databases, search structures, metadata, and intermediate structures can consume large amounts of RAM. Once memory becomes insufficient, conventional implementations may fail, require extremely large-memory machines, or rely on approximation/pruning strategies that can potentially sacrifice completeness.

Air-HUIM should instead treat RAM as a **strictly bounded working-set cache** backed by persistent storage, preferably a fast NVMe SSD.

The complete dataset and materialized mining structures should be allowed to exist outside RAM. Only the portions required by the current mining traversal should be brought into memory. When those structures are no longer useful, they should be evicted from RAM while remaining recoverable from storage.

The ultimate objective is not to make constrained hardware magically as fast as a high-memory server. The objective is to demonstrate that **exact mining remains feasible under severe memory constraints**, with predictable performance degradation as the memory budget decreases.

---

# 1. Core Research Problem

Investigate the following research question:

> **Can exact High-Utility Itemset Mining be decoupled from large-memory requirements through a traversal-aware, disk-backed buffer-pool architecture that dynamically manages the movement of mining structures between persistent storage and bounded RAM?**

The system should specifically investigate memory budgets such as:

* 8 GB
* 4 GB
* 2 GB
* 1 GB

while executing the same mining workload.

The critical property is that reducing the memory budget must **not change the final mining result**.

The system should therefore prioritize:

**Exactness > memory efficiency > I/O efficiency > execution time.**

Performance remains important, but the primary research claim is that the system can preserve exact HUIM semantics even when available RAM is dramatically smaller than the dataset and mining working set.

---

# 2. Important Conceptual Distinction

Do not frame the project simply as:

> "Running HUIM on a Raspberry Pi."

That is too narrow and does not constitute the central research contribution.

Likewise, do not claim:

> "Nobody has performed memory-constrained high-utility mining."

Existing literature already demonstrates memory-adaptive HUIM/HUSP techniques.

Instead, position Air-HUIM around the following distinction:

### Existing memory-adaptive approaches

When memory becomes constrained, an algorithm may:

* prune candidate structures;
* discard low-promising patterns;
* approximate the search;
* reduce the search space;
* sample the data;
* distribute computation across machines.

### Air-HUIM

Air-HUIM should instead:

* preserve the complete logical mining state;
* move inactive structures to secondary storage;
* keep only an actively useful working set in RAM;
* predict or infer future structure requirements from the mining traversal;
* prefetch useful structures before they are needed;
* evict structures according to mining-aware policies;
* reload structures when the search reaches them;
* preserve exact output.

The fundamental idea is therefore:

> **Do not discard information merely because RAM is insufficient. Move it through the storage hierarchy.**

---

# 3. Research Gap

The literature review must explicitly acknowledge and differentiate Air-HUIM from prior work.

## 3.1 MAHUSP

The most important prior work is:

**Zihayat et al., "Memory-adaptive high utility sequential pattern mining over data streams" (2017).**

MAHUSP introduces a memory-adaptive mechanism for high-utility sequential pattern mining over streaming data.

Its memory management strategy includes maintaining a bounded structure and removing less-promising patterns when memory becomes constrained.

Air-HUIM must explicitly acknowledge this work rather than claiming that memory-adaptive utility mining is unexplored.

However, Air-HUIM investigates a different problem:

* MAHUSP: high-utility **sequential pattern mining**
* Air-HUIM: high-utility **itemset mining**
* MAHUSP: **streaming/unbounded data**
* Air-HUIM: **static transactional databases**
* MAHUSP: memory pressure causes **logical pruning/discarding**
* Air-HUIM: memory pressure causes **physical eviction to persistent storage**
* MAHUSP: approximation/memory-adaptive search
* Air-HUIM: **exact output**
* MAHUSP: custom memory-adaptive mining structure
* Air-HUIM: **disk-backed buffer pool and storage hierarchy**

Do not overstate this distinction. Investigate whether later literature has blurred these boundaries.

---

# 4. Existing Secondary-Storage HUIM Work

Investigate previous approaches that move HUIM structures to secondary storage, including work involving memory-adaptive UP-Growth and secondary-memory/page-blocking mechanisms.

These works are particularly important because they establish that:

> Secondary storage has previously been considered as a solution to HUIM memory pressure.

However, carefully investigate whether those approaches provide:

* a general buffer-pool abstraction;
* explicit page/frame management;
* traversal-aware eviction;
* predictive prefetching;
* asynchronous I/O;
* cost-aware materialization;
* recomputation-versus-storage decisions;
* exactness guarantees;
* experimentally controlled memory budgets.

Some relevant work may be retracted or have methodological limitations. Do not rely on retracted work as the scientific foundation, but still acknowledge it as prior art where appropriate.

---

# 5. Parallel and Distributed HUIM

Investigate approaches such as approximate parallel HUIM.

These approaches attack scalability through:

* parallel computation;
* distributed processing;
* sampling;
* workload partitioning;
* additional compute resources.

Air-HUIM should explicitly position itself as a complementary **scale-up / single-machine approach**.

The question is not:

> "Can we use more machines?"

It is:

> "Can we make one constrained machine execute the exact workload by intelligently managing its memory hierarchy?"

---

# 6. Out-of-Core Computing as Architectural Inspiration

Investigate modern out-of-core systems, including work such as **ACGraph** and related systems research.

ACGraph and similar systems demonstrate architectural ideas such as:

* block-centric processing;
* active/inactive blocks;
* dynamic scheduling;
* asynchronous I/O;
* memory-aware execution;
* minimizing unnecessary storage accesses;
* prioritizing useful blocks.

Air-HUIM should learn from this systems architecture but not simply reproduce it.

The crucial difference is that Air-HUIM's scheduling and storage decisions should be **specific to the traversal characteristics of HUIM**.

HUIM is not graph processing.

The system must understand:

* utility lists;
* candidate extensions;
* prefix-based search;
* DFS traversal;
* utility upper bounds;
* subtree relationships;
* candidate dependencies;
* future utility-list requirements.

This should lead to a **mining-aware storage hierarchy**, rather than a generic cache.

---

# 7. Air-HUIM Architecture

Design the system around a hierarchy similar to:

```text
                 Static Transaction Database
                           │
                           ▼
                  Preprocessing Layer
                           │
                           ▼
                 Persistent ChunkStore
                           │
                  ┌────────┴────────┐
                  │                 │
             Prefetcher          Eviction
                  │                 │
                  ▼                 ▼
              Buffer Pool / RAM Cache
                           │
                           ▼
                  HUIM Search Engine
                           │
                           ▼
                   Exact HUIs
```

The persistent storage layer should hold the complete recoverable mining state.

RAM should act as a bounded **buffer pool**, not as the permanent home of the entire dataset.

---

# 8. Buffer Pool Manager

The buffer pool is the central systems component.

It should provide abstractions conceptually similar to:

```text
PIN(page_id)
UNPIN(page_id)
PREFETCH(page_id)
EVICT(page_id)
FLUSH(page_id)
LOAD(page_id)
```

The exact API may differ.

A page/chunk should have metadata such as:

* page ID;
* storage location;
* size;
* dirty state;
* pin count;
* last access time;
* access frequency;
* predicted future access probability;
* utility-related information;
* parent/child relationship;
* traversal depth;
* current mining state;
* loading cost;
* recomputation cost.

The buffer pool must enforce a strict configurable memory budget.

For example:

```text
8 GB mode → maximum 8 GB application buffer
4 GB mode → maximum 4 GB application buffer
2 GB mode → maximum 2 GB application buffer
1 GB mode → maximum 1 GB application buffer
```

The memory budget must be measurable and reproducible.

Do not simply allocate a huge amount of RAM and claim that the algorithm is using less.

---

# 9. Mining-Aware Eviction

Do not rely solely on generic policies such as:

* LRU;
* FIFO;
* random eviction.

Implement and investigate policies that understand the mining workload.

For example, a page's eviction priority could consider:

```text
EvictionScore =
    recency
  + frequency
  + predicted future access
  + traversal position
  + utility relevance
  + reload cost
  + recomputation cost
  + dependency state
```

A page that is unlikely to be accessed again should be an eviction candidate.

A page that is highly likely to be needed in the next DFS branch should remain resident.

A page that is expensive to reload or reconstruct should receive different treatment from a cheap page.

The exact mathematical formulation should be investigated experimentally rather than assumed in advance.

---

# 10. Predictive Prefetching

This is one of the most important potential contributions.

Instead of waiting for the mining algorithm to request a structure:

```text
REQUEST
   ↓
DISK READ
   ↓
WAIT
   ↓
COMPUTE
```

Air-HUIM should attempt:

```text
CURRENT MINING STATE
        ↓
Predict next required structures
        ↓
Asynchronous prefetch
        ↓
Data enters RAM before demand
        ↓
Mining continues
```

The predictor can initially use deterministic information from the HUIM traversal rather than immediately requiring machine learning.

Potential signals include:

* current prefix;
* DFS depth;
* candidate ordering;
* utility-list relationships;
* parent/child dependencies;
* previously observed access sequence;
* access frequency;
* traversal locality.

Later versions can investigate learned prediction models if they provide measurable benefits.

Do not introduce machine learning merely for novelty.

The predictor should first be **simple, interpretable, and measurable**.

---

# 11. Utility-Aware Prefetching

A particularly interesting research direction is to move beyond:

> "Which page will probably be accessed next?"

and instead ask:

> **"Which page is most valuable to have in RAM next?"**

For example, define a priority based on some combination of:

```text
ExpectedBenefit =
    P(access)
    × ExpectedMiningValue
    ÷ StorageLoadCost
```

This allows Air-HUIM to prioritize structures that are both:

* likely to be required;
* valuable to the mining process.

Investigate whether utility-aware prediction produces better cache efficiency than conventional access prediction.

This could become one of the central algorithmic contributions of the project.

---

# 12. Exactness Requirement

Air-HUIM must be an **exact mining system**.

The output should contain exactly the same high-utility itemsets as an appropriate trusted reference implementation operating without the Air-HUIM memory constraint.

For every experiment:

```text
Reference HUIM output
        ==
Air-HUIM output
```

Validate:

* zero false negatives;
* zero false positives;
* identical utility values;
* identical itemsets.

Do not rely only on output counts.

Where feasible, perform deterministic set comparison and utility-value comparison.

This exactness requirement is one of the central differentiators from approximation-oriented memory-adaptive approaches.

---

# 13. Memory-Budget Experiments

The same workload should be executed under progressively constrained memory budgets:

```text
8 GB
4 GB
2 GB
1 GB
```

The purpose is to demonstrate that the architecture gracefully degrades rather than simply failing when memory becomes insufficient.

The expected result is not necessarily constant performance.

Instead:

```text
Less RAM
   ↓
More storage I/O
   ↓
Lower cache hit rate
   ↓
Higher execution time
   ↓
BUT
   ↓
Exact output preserved
```

This is an important research result.

The system is not expected to eliminate the fundamental cost of limited memory.

It is expected to **trade memory capacity for storage traffic while preserving correctness**.

---

# 14. Dataset-Scaling Experiments

Do not only vary RAM.

Also vary dataset size while keeping the RAM budget fixed.

For example:

```text
RAM = 4 GB

Dataset:
2 GB
5 GB
10 GB
20 GB
50 GB
100 GB
```

The exact dataset sizes should depend on available hardware and meaningful benchmark datasets.

The important question is:

> **Can dataset size grow substantially beyond the available RAM while the system continues to produce exact results?**

This experiment is arguably more important than the Raspberry Pi experiment.

---

# 15. Memory-Performance Frontier

Measure how execution changes as the memory budget decreases.

At minimum record:

* execution time;
* peak application memory;
* buffer-pool size;
* cache hit rate;
* cache miss rate;
* number of page loads;
* number of evictions;
* number of prefetches;
* useful prefetch rate;
* unnecessary prefetch rate;
* disk read volume;
* disk write volume;
* I/O latency;
* CPU utilization;
* mining throughput;
* exactness.

Construct a memory-performance curve such as:

```text
Execution Time
      │
      │             *
      │          *
      │       *
      │    *
      │ *
      └──────────────────
        1  2  4  8 GB
           RAM Budget
```

The goal is to characterize the cost of reducing memory rather than simply presenting a single benchmark.

---

# 16. Baselines

Air-HUIM must be compared against meaningful baselines.

At minimum investigate:

### Baseline 1 — Conventional in-memory HUIM

Run a standard exact HUIM implementation with sufficient memory.

This establishes the performance reference.

### Baseline 2 — Generic LRU buffer pool

Use the same out-of-core architecture but replace mining-aware eviction with ordinary LRU.

This isolates the contribution of mining-aware cache management.

### Baseline 3 — Non-predictive out-of-core HUIM

Use the same storage hierarchy but only load data on demand.

This isolates the contribution of predictive prefetching.

### Baseline 4 — Predictive but utility-agnostic caching

Predict future accesses but ignore utility information.

This isolates whether utility-aware prediction actually matters.

### Baseline 5 — Existing memory-adaptive HUIM/HUSP techniques

Compare conceptually and experimentally where the problem definitions permit fair comparison.

Do not make invalid apples-to-oranges comparisons between sequential-pattern and itemset-mining algorithms.

---

# 17. Raspberry Pi Validation

Once the architecture is validated on a conventional development machine, deploy it on constrained edge hardware.

Potential targets include:

* Raspberry Pi 4;
* Raspberry Pi 5;
* other ARM SBCs where appropriate.

The Raspberry Pi should be treated as a **real-world constrained deployment platform**, not as the sole source of novelty.

Measure:

* RAM consumption;
* CPU utilization;
* storage I/O;
* execution time;
* temperature;
* thermal throttling;
* power consumption if measurement hardware is available.

The strongest claim is not:

> "Raspberry Pi can mine data."

It is:

> **"The same storage-backed exact mining architecture can operate under constrained memory on inexpensive edge hardware."**

---

# 18. Energy and I/O Efficiency

Investigate whether bounded-memory execution introduces a meaningful storage and energy cost.

Measure, where practical:

* total energy;
* average power;
* energy per transaction processed;
* energy per GB mined;
* total bytes read;
* total bytes written;
* I/O amplification.

The goal is not necessarily to prove that Air-HUIM is always more energy-efficient.

Instead, characterize the tradeoff:

```text
More RAM
    ↓
Less I/O
    ↓
Lower execution time

Less RAM
    ↓
More I/O
    ↓
Higher execution time
```

This makes the system's behavior scientifically measurable.

---

# 19. Important Theoretical Question

Investigate whether there is a practical lower bound on memory for exact HUIM under the proposed architecture.

For example:

> What happens when the buffer pool becomes too small to retain the minimum useful working set?

At some point, the system may enter pathological behavior:

```text
Load page A
Compute
Evict A

Load page B
Compute
Evict B

Need A again
Reload A
...
```

This can create severe thrashing.

Air-HUIM should therefore identify:

* minimum viable memory;
* thrashing threshold;
* working-set size;
* I/O saturation point;
* cache locality characteristics.

This can become an important part of the analysis.

---

# 20. Recompute vs. Materialize

Do not assume that every mining structure should always be stored on disk.

Some structures may be:

* cheap to recompute;
* expensive to store;
* rarely accessed.

Others may be:

* expensive to recompute;
* frequently accessed;
* large but valuable to preserve.

Therefore investigate a decision mechanism:

```text
Should this structure be:

A) kept in RAM?
B) written to disk?
C) evicted and reconstructed later?
D) prefetched?
E) discarded because it is provably unnecessary?
```

The distinction between **logical pruning** and **physical eviction** must remain clear.

A structure should only be discarded permanently when the HUIM algorithm itself proves that it cannot contribute to the exact result.

Memory management must not accidentally become approximation.

---

# 21. Storage Representation

Design an efficient persistent representation for mining structures.

Investigate:

* chunking;
* page sizing;
* compression;
* serialization overhead;
* metadata locality;
* alignment;
* sequential versus random access;
* storage amplification.

The storage format should be designed around the actual access pattern of utility-list-based HUIM.

Do not blindly use a general database engine if a purpose-built chunk store provides better experimental control.

However, compare against reasonable storage mechanisms where appropriate.

---

# 22. Concurrency and Asynchronous I/O

Where hardware permits, investigate asynchronous I/O.

The objective is to overlap:

```text
CPU computation
+
future page loading
```

instead of forcing:

```text
COMPUTE
WAIT FOR DISK
COMPUTE
WAIT FOR DISK
```

Potential architecture:

```text
                 ┌──────────────┐
                 │ Mining Engine│
                 └──────┬───────┘
                        │
                 Future Access
                        │
                        ▼
                ┌──────────────┐
                │  Prefetcher  │
                └──────┬───────┘
                        │
                 Async I/O Queue
                        │
                        ▼
                     NVMe
```

Measure whether asynchronous prefetching actually reduces wall-clock execution time.

---

# 23. Hardware and Memory Budget Reproducibility

Every experiment must clearly distinguish:

### Physical system RAM

The total RAM physically installed.

### Air-HUIM memory budget

The maximum memory that Air-HUIM is permitted to use.

For example:

```text
Physical RAM = 32 GB
Air-HUIM budget = 1 GB
```

is a valid experiment.

Do not simply claim that the machine is a 1-GB system if the operating system has access to 32 GB.

Report:

* physical RAM;
* configured Air-HUIM budget;
* actual peak RSS;
* buffer pool allocation;
* metadata allocation;
* OS overhead;
* storage capacity.

This is essential for scientific credibility.

---

# 24. Implementation Language

The system may be implemented in **Rust or C/C++**, with the final choice driven by:

* predictable memory usage;
* storage control;
* asynchronous I/O;
* performance;
* instrumentation;
* correctness;
* reproducibility.

Rust should be seriously considered because buffer-pool systems involve:

* ownership of pages;
* pinned frames;
* eviction;
* asynchronous loading;
* concurrent references;
* dirty-page management.

Memory safety is particularly valuable in this class of systems.

The programming language itself is not the research contribution.

---

# 25. Core Research Hypothesis

The project should ultimately test the following hypothesis:

> **A traversal-aware, storage-backed buffer pool with predictive and utility-aware data placement can substantially reduce the RAM requirement of exact high-utility itemset mining, allowing datasets significantly larger than available memory to be mined without false negatives or false positives, at the cost of additional storage I/O and execution time.**

A secondary hypothesis is:

> **Mining-aware predictive caching can outperform generic caching and demand-only out-of-core execution by reducing unnecessary storage accesses and improving effective cache utilization.**

A third hypothesis is:

> **The architecture remains viable on inexpensive resource-constrained edge hardware such as Raspberry Pi systems.**

---

# 26. What Must NOT Be Claimed

Avoid claims such as:

> "Nobody has ever done memory-adaptive HUIM."

Incorrect.

Avoid:

> "No one has ever used disk storage for HUIM."

Incorrect.

Avoid:

> "Raspberry Pi makes HUIM possible."

Too broad.

Avoid:

> "Air-HUIM makes huge datasets require almost no memory."

Potentially misleading.

Instead claim precisely:

> **Air-HUIM investigates whether exact HUIM can be executed with a strictly bounded RAM working set by using persistent storage as an extension of the mining state.**

And demonstrate the limits experimentally.

---

# 27. The Main Novelty to Investigate

The central novelty should be the **intersection** of:

```text
Exact HUIM
     +
Static transactional databases
     +
Strict memory budgets
     +
Disk-backed utility-list structures
     +
Traversal-aware buffer management
     +
Predictive prefetching
     +
Utility-aware eviction
     +
Single-machine execution
```

The novelty must be validated through an exhaustive literature search before making publication claims.

If existing literature already contains one of these mechanisms, do not hide it.

Instead, identify exactly what Air-HUIM contributes beyond it.

---

# 28. Expected Scientific Contribution

If experiments validate the architecture, the project could contribute:

1. A disk-backed architecture for exact HUIM under strict memory constraints.
2. A bounded-memory buffer pool specifically adapted to HUIM traversal.
3. A predictive prefetching mechanism based on mining traversal.
4. A utility-aware cache/eviction policy.
5. An experimental characterization of the memory-versus-I/O tradeoff.
6. An evaluation of exact HUIM at 8 GB, 4 GB, 2 GB and 1 GB memory budgets.
7. Evaluation on datasets substantially larger than available RAM.
8. Comparison against conventional in-memory, LRU, demand-paging and existing memory-adaptive approaches.
9. Demonstration on constrained edge hardware such as Raspberry Pi.
10. A reproducible framework for studying out-of-core HUIM.

---

# 29. The Most Important Experimental Result

The strongest possible result is not:

> "Air-HUIM is faster."

It may not be.

The strongest result would look more like:

> A conventional exact HUIM implementation requires a large memory footprint for a given workload, whereas Air-HUIM executes the same workload with a 1–8 GB explicitly bounded memory budget by maintaining the mining state in a disk-backed storage hierarchy. Across all completed configurations, Air-HUIM produces exactly the same itemsets and utility values as the reference implementation, while exhibiting a measurable and explainable increase in storage I/O and execution time as the memory budget decreases.

That is a scientifically meaningful result.

---

# 30. Long-Term Direction

If the core architecture succeeds, investigate whether the same storage-management abstraction can support:

* high-utility sequential pattern mining;
* frequent-itemset mining;
* graph mining;
* stream mining;
* other memory-intensive data-mining algorithms.

However, these should remain extensions.

The initial system should stay focused on establishing whether **exact HUIM can be effectively decoupled from large RAM requirements through intelligent storage hierarchy management**.

The ultimate vision is:

> **Air-HUIM turns RAM from a hard dataset-size requirement into a configurable performance parameter.**

Instead of:

```text
Dataset too large for RAM
        ↓
Cannot execute
```

the system should provide:

```text
Dataset
   ↓
Persistent Mining State
   ↓
Bounded RAM Working Set
   ↓
Predict → Prefetch → Compute → Evict → Recover
   ↓
Exact Result
```

The amount of RAM determines **how efficiently** the workload runs, rather than whether the workload can theoretically be executed at all.
