# Literature notes (first pass, 2026-10-03)

Scope of this pass: is "exact HUIM under a hard, user-set memory budget on edge hardware"
already covered? Sources were found by web search; papers were not read in full — treat the
claims below as leads to verify before citing.

## 1. Closest prior work: HUIM when data does not fit in memory
- **Disk-based projection for HUIM** — "Efficient Mining of High Utility Itemsets from Large
  Datasets" (PAKDD 2008) uses a *parallel projection scheme that falls back to disk when main
  memory is inadequate*. This is the most direct precedent: memory-aware HUIM is not new.
  It is a two-phase-era algorithm; it does not take a user budget or cover a family of
  algorithms. [Springer](https://link.springer.com/chapter/10.1007/978-3-540-68125-0_50) ·
  [ResearchGate](https://www.researchgate.net/publication/220894929_Efficient_Mining_of_High_Utility_Itemsets_from_Large_Datasets)
- **Projection-based indexing for HUIM** to reduce memory (KAIS 2012).
  [Springer](https://link.springer.com/article/10.1007/s10115-012-0492-y)
- **Distributed HUIM** (Spark/Hadoop: DMOUM, PHUIM, Hadoop-based parallel HUIM) scales out
  instead of bounding memory on one device.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S1319157821001038) ·
  [SciOpen](https://www.sciopen.com/article/10.23919/CSMS.2022.0023)

## 2. Memory of standard HUIM algorithms (what the field measures)
- Empirical evaluation of HUIM algorithms (Expert Systems with Applications 2018): EFIM uses
  the least memory in most cases (e.g. 1.3–27x less than d2HUP), under 100 MB on four of six
  datasets, never above 1 GB; others often exceed 1 GB. Memory is *reported*, not *bounded*.
  [PDF](https://www.philippe-fournier-viger.com/spmf/EvaluationHUIM.pdf) ·
  [ACM](https://dl.acm.org/doi/10.1016/j.eswa.2018.02.008)
- Survey of utility-oriented pattern mining (Gan, Lin, Fournier-Viger et al.).
  [arXiv](https://arxiv.org/pdf/1805.10511)
- EFIM paper (memory-efficient projection + transaction merging).
  [PDF](https://www.philippe-fournier-viger.com/spmf/MICAI2015_EFIM_High_Utility_Itemset_Mining.pdf)

## 3. Out-of-core / memory-bounded frequent itemset mining (adjacent field)
- **DRFP-tree** (disk-resident FP-tree; mines in memory and moves to disk only when it runs
  out of memory). Very close in spirit to our spill policy, for *frequent* itemsets.
  [Springer](https://link.springer.com/article/10.1007/s10489-007-0099-2)
- **Memory-efficient frequent itemset mining** (CFP-tree/array, EDBT 2011), ~10x smaller
  in-memory structures. [PDF](https://openproceedings.org/2011/conf/edbt/SchlegelGL11.pdf)
- Index support for FIM inside a DBMS (loads only needed index blocks).
  [ResearchGate](https://www.researchgate.net/publication/4133504_Index_support_for_frequent_itemset_mining_in_a_relational_DBMS)
- Wear-leveling-aware FP mining on non-volatile memory.
  [arXiv](https://arxiv.org/pdf/2001.05157)

## 4. Pattern mining on devices
- **MobileMiner** (UbiComp 2014): frequent co-occurrence patterns mined on the phone.
  Frequent (not utility) patterns; no hard budget framework.
  [PDF](https://www.winlab.rutgers.edu/~lendlice/paper/Srinivasan_MobileMiner_UbiComp14.pdf)
- A search for HUIM on Raspberry Pi / IoT edge devices returned nothing specific to HUIM.

## 5. Algorithm identities (to keep our naming honest)
- **HUIM-MMU** = HUI mining with *multiple minimum utility thresholds* (Lin, Gan,
  Fournier-Viger, Hong, Zhan; Knowledge-Based Systems 113, 2016) — not a sliding window.
  [ACM](https://dl.acm.org/doi/abs/10.1016/j.knosys.2016.09.013)
- **mHUIMiner** (Peng, Koh, Riddle; PAKDD 2017) = HUI-Miner + IHUP-tree to avoid building
  utility lists for itemsets not in the database; fastest on sparse data.
  [PDF](https://researchspace.auckland.ac.nz/bitstreams/55d6d99c-3e76-470a-ac02-c5921fb5818e/download) ·
  [SPMF](https://www.philippe-fournier-viger.com/spmf/mHUIMiner.php)
- **EIHI** = incremental HUIM in batches with FHM-style lists, results in a HUI-trie.
  [SPMF](https://www.philippe-fournier-viger.com/spmf/EIHI.php)
- Sliding-window stream HUIM: SHU-Grow, SOHUPDS, HUPMS-style work.
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0957417416300902)
- Incremental HUIM survey (Knowledge Engineering Review 2025).
  [Cambridge](https://www.cambridge.org/core/journals/knowledge-engineering-review/article/mining-of-high-utility-itemsets-from-incremental-datasets-a-survey/3ABF8A2F15846B6F7417255B9B3881A4)

## 6. Assessment for our paper
- "Memory-aware HUIM" exists (PAKDD 2008 disk projection), and out-of-core FIM is a mature
  idea (DRFP-tree). Our contribution must therefore be framed as: **a user-set hard budget
  applied uniformly across a family of HUIM algorithms, with exactness preserved and verified
  at every budget, evaluated on edge hardware** — plus specific techniques (single ledger
  shared with the buffer pool, partitioned exact EUCS, budget-sized candidate verification).
- The negative result (truncated EUCS silently drops HUIs under memory pressure) is worth a
  dedicated paragraph; check whether other memory-adaptive miners have the same issue.
- Still to do: read the PAKDD 2008 paper and DRFP-tree closely; search venues DaMoN, EDBT,
  ICDE workshops, IEEE IoT Journal for "memory budget" + "pattern mining"; check Google
  Scholar citations of the PAKDD 2008 paper for follow-ups.

## 7. Second pass (2026-11-01): Raspberry Pi, Rust, parallel and memory-bounded HUIM

**Has anyone mined high-utility itemsets on a Raspberry Pi?** Nothing found. Searched:
OpenAlex title+abstract phrase filters ("raspberry pi" with "utility itemset", "high utility",
"utility mining": 0 hits each; with "itemset": 4 hits, all Apriori/IoT weather-station work),
arXiv API (0 hits for high utility/itemset + raspberry, + memory budget, + rust), Crossref,
web search, GitHub and crates.io. dblp blocks automated access, so it was not searched.
The closest work runs *frequent* itemset mining (Apriori) on Raspberry Pi *clusters*:
- A Low-Cost Energy-Efficient Raspberry Pi Cluster for Data Mining Algorithms (Euro-Par
  workshops 2016): Apriori, up to 88.35% less power than a Xeon Phi.
  [Springer](https://link.springer.com/chapter/10.1007/978-3-319-58943-5_63)
- Improve Data Mining Techniques with a High-Performance Cluster (ICCA 2022): Apriori on a
  Pi cluster. [DOI](https://doi.org/10.1109/icca56443.2022.10039629)
- Neither bounds memory, mines utility, or runs on a single board.

**Is there an existing Rust HUIM library?** No. GitHub (language:rust) and crates.io have no HUIM
library. Only: `finalfire/huim_es` (2021, one-file genetic-algorithm toy, not exact);
`dci` (DCI-Closed, *frequent* closed itemsets); `rustyfim` (frequent itemsets for Python);
ET-Miner (frequent itemsets, Rust backend + GPU). CLOE-HOI (Zenodo 2026, C99/Rust) is
high-*occupancy* mining, a different problem. The broad HUIM libraries are SPMF (Java) and
PAMI (Python, Kiran et al., JMLR 2024). [PAMI](https://www.jmlr.org/papers/volume25/22-1026/22-1026.pdf)

**Parallel HUIM (thread-scaling comparators).**
- DPHIM (Kimura, Hayamizu, Kiran, Kitsuregawa, Goda; ICDE 2023 and IEEE TKDE 2026): dynamic,
  NUMA-aware parallel HUIM. Up to 72.7x faster than tuned serial execution, 40.4x speed-up at
  96 threads, scales past 24 threads; also runs on persistent memory (1.1-2.4x slower).
  Memory is not bounded. [TKDE](https://doi.org/10.1109/tkde.2026.3666851) ·
  [ICDE](https://doi.org/10.1109/icde55515.2023.00388)
- GA-HUIM (same group, 2026): GPU HUIM, up to 94.7x over CPU approaches.
  [Springer](https://link.springer.com/chapter/10.1007/978-981-92-0375-8_2)
- pEFIM, MCH-Miner (multi-core EFIM / iMEFIM; static load balancing).
- Air-HUIM on the Pi (4 cores): 1.1-3.7x from 1 to 4 threads (median 1.8x over 9 setups).
  On a 4-core board this is the most that can be shown; a laptop 1-16 thread run is needed
  for any comparison with DPHIM-scale results.

**Memory-aware HUIM on one machine (novelty boundary).**
- Erwin, Gopalan, Achuthan, PAKDD 2008 (CTU-PROL): parallel projection that falls back to disk.
- Vu & Nguyen, Acta Cybernetica 2011: disk-based HUIM ("Transactional Array", per-item small
  trees, non-recursive mining). [DOI](https://doi.org/10.14232/actacyb.20.2.2011.6)
- Han, Liu, Li, Gao, Information Sciences 2020 (PTM): top-k HUIM on massive data by
  prefix-based partitioning. [DOI](https://doi.org/10.1016/j.ins.2020.08.028)
- SHO (ESWA 2022): high-utility occupancy mining on massive data.
- None of these takes a user-set hard memory budget, covers a family of algorithms, or
  reports runs on edge hardware.

**Published numbers to compare against (and why most do not compare directly).**
- Zhang, Almpanidis, Wang, Liu, ESWA 2018 (the standard empirical evaluation): SPMF 2.19 on
  an i7-5960X at 4.4 GHz with 128 GB RAM and `-Xmx128g`. EFIM uses the least memory in most
  cases; d2HUP needs 8-27x more than EFIM on BMS/chess/connect/mushroom; FHM ran out of a 17 GB
  memory limit on one synthetic set. [PDF](https://www.philippe-fournier-viger.com/spmf/EvaluationHUIM.pdf)
- Published runtimes use other thresholds and machines (typically a desktop with 16-128 GB),
  so only same-machine, same-threshold comparisons are valid. Ours: Air-HUIM vs SPMF on the same
  Pi and the same files (r6 benchmark), and a 1-thread laptop table (5 repeats).
