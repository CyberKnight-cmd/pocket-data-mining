# Roadmap: the complete high-utility mining library

**Goal.** Every important high-utility pattern mining algorithm, each one:
- **exact** (or labelled approximate);
- **memory-bounded**, with the ledger, admission control and spill/recompute;
- **verified against brute force** at 96 KB and 256 MB budgets, with 1 and 4 threads.

**Sources.** The SPMF 2.67a catalog (106 runnable utility algorithms, read from the jar) and a
survey of 2017–2026 work. Where SPMF's version is superseded or incomplete, we implement the
better published algorithm and keep the SPMF name only where its output is identical.

Current state: 23 algorithms (`Implemented.md`). 16 are in SPMF. 7 are not: HUP-Tree,
HUI-Trie, REPT, HUIM-MMU, SHUIM, IncFHM and MHUI-ACO. Our GA, BPSO and ACO are our own
transaction-guided variants, not the published algorithms; they are labelled as such, and the
published versions are in tier 3.

## Rules every new algorithm follows
1. **Implement from the paper.** Use SPMF only as a cross-check, never as the oracle: SPMF's
   FHM/FHM+ comparator overflows, its mHUIMiner output is corrupted, and its USpan is incomplete.
2. **Reuse an engine** (`ul_engine`, `efim_engine`, `proj_engine`, `tree_engine`,
   `heuristic_engine`, or the new sequential engine). Everything that scales with the data is
   charged to the ledger and is spillable or recomputable.
3. **Admission control:** an estimate of what cannot be spilled, a clean refusal with "Use at
   least -b N", and fewer threads when the per-worker sets don't fit.
4. **Tests:** a brute-force oracle for the variant's definition (closed, maximal, negative,
   on-shelf, average, sequential...), at both test budgets and both thread counts, with the
   ledger-peak bound and no leaked reservations.
5. **64-bit utilities and 64-bit comparisons everywhere.**

## Tier 1: state of the art and the main gaps

### Itemsets: fastest exact miners
| Algorithm | Why | Engine |
|---|---|---|
| d2HUP (Liu, Wang, Fung, ICDM 2012 / TKDE 2016) | fastest on very sparse data and long transactions (Zhang et al. ESWA 2018 benchmark) | new, on `proj_engine` |
| ULB-Miner (Duong et al., Appl. Intell. 2018) | buffered utility lists: memory reuse fits the budget model | `ul_engine` |
| HMiner (Krishnamoorthy, ESWA 2017) | compact utility lists, strong on dense data | `ul_engine` |
| UBP-Miner (Wu et al., KBS 2022) | bit-partition utility lists, not in SPMF | `ul_engine` (bitmap bodies) |
| FOTH (Yan et al., Inf. Sci. 2024) | join-free IndexSet; reports large dense-data speedups; not in SPMF | new, on `proj_engine` |

### Concise representations
| Algorithm | Engine |
|---|---|
| CHUI-Miner (closed; Wu et al. 2015) | `ul_engine` + closure check |
| CLS-Miner (closed; Dam et al. 2019; faster than CHUD and CHUI-Miner) | `ul_engine` |
| HMiner-Closed (closed; Nguyen et al., Inf. Sci. 2019) | `ul_engine` |
| CHUI-MinerMax (maximal; the only maximal miner in SPMF) | `ul_engine` |
| MinFHM (minimal; DEXA 2016) | `ul_engine` |
| HUG-Miner / GHUI-Miner (generators; ADMA 2014) | `ul_engine` |

### Variants with real-world demand
| Algorithm | Problem | Engine |
|---|---|---|
| FHN (ADMA 2014); GHUM-style bounds (KBS 2018) | negative unit profits (discounts, returns) | `ul_engine`, positive/negative split |
| FOSHU (SAC 2015) | on-shelf utility (items sold only in some periods) | `ul_engine` |
| EHAUPM (IEEE Access 2017); dHAUIM (TKDE 2019) | average utility, tighter bounds than HAUI-Miner | `ul_engine` |
| FHMFreq | frequent and high-utility | `ul_engine` |
| FCHM (bond, all-confidence); ECHUM (Kulczynski) | correlated HUIs | `ul_engine` |
| THUI (ESWA 2019) | top-k; reported up to 3 orders of magnitude faster than TKO | `efim_engine` / `ul_engine` |

### Sequences: the new engine (designed separately, see below)
| Algorithm | Why |
|---|---|
| HUSP-SP (Zhang et al., ACM TKDD 2023) | fastest and most memory-efficient exact HUSP miner; proven bounds (PEU, RSU, TRSU); not in any library |
| HUS-Span (Wang et al., KAIS 2016) | the reference PEU/RSU algorithm; second oracle for HUSP-SP |
| TKUS (Inf. Sci. 2021) | top-k HUSP |
| USpan (KDD 2012) | SPMF compatibility only. Its SPU bound is not an upper bound (Truong-Chi & Fournier-Viger survey, 2019), so it is incomplete. We document this and show the HUSPs it loses next to brute force. |

## Tier 2: completes SPMF coverage, mostly cheap on existing engines
- **Superseded but standard:**
  - HUINIV-Mine (negative);
  - TS-HOUN (on-shelf with negatives);
  - CHUD (closed);
  - HUCI-Miner (closed, generators, both);
  - HAUI-MMAU and MEMU (average utility with multiple thresholds);
  - UPHist;
  - FHIM;
  - UFH;
  - HUIM-SU;
  - PUCPMiner;
  - RMiner;
  - MEHUIM and MEHUIM-Closed;
  - IMEFIM (dynamic profits).
- **Time-aware:** LHUI-Miner and PHUI-Miner (local and peak), LTHUI-Miner (trending), PHM,
  PHMN, PHMN+ and PHM_irregular (periodic), FHMDS (top-k over a stream), EIHI (incremental;
  we have IncFHM).
- **Taxonomy and quantitative:** CLHMiner, FEACP and MLHUIMiner (cross-/multi-level, with a
  taxonomy file); VHUQI, FHUQIMiner, TKQ and CHUQI-Miner (quantitative); LCIM (low cost).
- **Frequent-utility and skyline:** FUIMTF, FUIMTWU, SkyMine, SFUPMinerUemax, SFUI-UF,
  EMSFUI_B and EMSFUI_D.
- **Rules:** HGB and HGB_All (high-utility association rules); HUSRM (high-utility sequential
  rules, on the sequential engine).
- **Episodes** (on the sequential engine): HUE-SPAN (faster than UP-SPAN), UP-SPAN, and
  TUP (top-k).
- **Sequences, other:** PHUSPM and UHUSPM (uncertain); CEPB, CorCEPB and CEPN (cost-effective).
- **Top-k average:** ETAUIM.

## Tier 3: approximate and metaheuristic (labelled approximate)
- **Published versions, for like-for-like comparison with SPMF:**
  - HUIM-GA (Kannimuthu 2014), HUIM-BPSO (Lin 2016), HUIM-ACO (Song & Nan 2020);
  - the -tree variants;
  - HUIM-ABC, HUIF-GA/PSO/BA, HUIM-SPSO, HUIM-AF, HUIM-HC, HUIM-SA;
  - TKU-CE and TKU-CE+;
  - SFU-CE.
- **Our transaction-guided variants** stay as the recommended choice.

## Research items (beyond SPMF)
- **Hamm** (Qu et al., IEEE TKDE 2023) and **REX** (KBS 2020): the newest strong claims for plain
  HUIM. Implement from the papers once we have full access, then run an independent benchmark
  against EFIM and d2HUP; the only broad independent comparison is from 2018.
- **C-HUIM and MaxC-HUIM** (KBS 2022): closed and maximal miners on MPUN lists.
- **dHAUIM** bounds also for the top-k average-utility case.
- **PTM / P2H** (Han et al., Inf. Sci. 2021): partition-by-prefix for top-k and all-HUI mining
  on massive data, close to our tree partitions.

## Order of work
1. Tier 1 itemsets, concise representations and variants: parallel agents, one per engine group.
2. The sequential engine: design first (bound discipline, oracle, memory model), then
   HUS-Span → HUSP-SP → TKUS → USpan (with its incompleteness documented).
3. Tier 2 in batches per engine.
4. Tier 3.
