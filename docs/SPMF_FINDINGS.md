# Correctness problems found in SPMF during the comparison

SPMF is the reference implementation for most HUIM algorithms. Comparing it with Air-HUIM
on the same data and thresholds turned up two cases where SPMF's output is wrong. Every
disagreement below was checked by a brute-force scan of the dataset.

## 1. FHM and FHM+: item comparator overflows on large datasets

**Symptom.** On dunnhumby LGSR (47.1M baskets, min_util = 117,945,433, i.e. 0.2% of total
utility), SPMF FHM outputs 51 HUIs and Air-HUIM FHM 52. The extra itemset {900830, 904358}
has utility 128,402,292 by a full scan, so it is a true HUI that SPMF misses. Air-HUIM
EFIM-Closed finds the same 52.

**Cause.** `AlgoFHM.compareItems` (and the same method in `AlgoFHMPlus`) orders items by TWU
with `(int)(twu(a) - twu(b))`; in the bytecode this is `lsub` followed by `l2i`. TWUs are
`long`, but the difference is truncated to 32 bits, so whenever two TWUs differ by 2^31
(about 2.1 billion) or more, the sign can come out wrong. On LGSR the largest TWU is
14,778,342,851, and 20,134 of the 1,519,896 pairs of promising items get the wrong order.
The comparator is then no longer a consistent total order. FHM relies on one order for the
items in each transaction (remaining utilities) and for the utility lists (the search), so
upper bounds are computed in inconsistent orders and a valid branch gets pruned.

**Scope.** Checked in the bytecode of the SPMF jar used in `bench/`. Only `AlgoFHM` and
`AlgoFHMPlus` contain this cast. `AlgoHUIMiner`, `AlgoHUPMiner`, `AlgoEFIM`,
`AlgoEFIMClosed` and `AlgoHAUIMiner` do not. The problem appears only once TWUs exceed
2^31, so the classic benchmark datasets don't trigger it, but any retail dataset with
billions in total utility does. Air-HUIM compares 64-bit values directly.

**Reproduce.** Convert with `bench/convert_lgsr.py`, then run `java -jar spmf.jar run FHM
lgsr.txt out.txt 117945433`. `bench/itemset_utility.py lgsr.txt "900830 904358"` gives the
exact utility (128,402,292), and `bench/spmf_comparator_check.py lgsr.txt 117945433` counts
the wrongly ordered pairs.

## 2. mHUIMiner: correct search, corrupted itemsets in the output

On retail (min_util 1491) SPMF mHUIMiner prints the right number of HUIs (22,479) with
exactly the right multiset of utility values, but 16,308 of the itemsets are wrong and 5,280
contain the same item twice. All 300 sampled disagreements were checked by brute force. The
search is right and the item labels written out are corrupted, which points to a bug in how
the output prefix is built. Its runs are compared on time and memory only. See also
`Implemented.md`.
