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
