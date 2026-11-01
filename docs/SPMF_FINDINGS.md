# Correctness problems found in SPMF during the comparison

SPMF is the reference implementation for most HUIM algorithms. Comparing it with Air-HUIM
on the same data and thresholds turned up three cases where SPMF's output is wrong. Every
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

The Pi run (r6, 2026-10-31) confirms this on 10 of the 12 datasets where mHUIMiner finished:
the printed count always equals the true count, but the itemsets are wrong. Only bms (7 HUIs)
and chainstore (81) come out right; even kosarak's 66 include 33 wrong ones. Some corrupted
lines become identical, so the distinct count is lower:
fruithut prints 114,408 lines (correct) that collapse to 111,018 distinct, which is where the
earlier "111,018 vs 114,408" came from. A full scan checked every disputed itemset
(`bench/brute_verify.py`): none of the itemsets mHUIMiner adds is a true HUI, and Air-HUIM's
output was exact in every comparison.

## 3. TKU: returns fewer than k itemsets on chicago

With k = 100 on Chicago Crimes, SPMF TKU (v2.26) reports "Number of top-k high utility
patterns : 4" and writes 4 itemsets. Their utilities are exact, but 96 itemsets are missing:
Air-HUIM TKO and TKU, and SPMF's own TKO, all return 100 itemsets on the same file, with the
100th at utility 114,357. The result is the same with a 1 GB and a 4 GB heap and on the Pi
and the laptop, so it is not caused by memory. On the other datasets where it finished
(foodmart, fruithut, retail, mushroom), SPMF TKU returns the right 100. The cause has not
been traced yet.

**Reproduce.** `java -Xmx1024m -jar spmf.jar run TKU Chicago_Crimes_2001_to_2017_utility.txt
out.txt 100`, then the same with `TKO` for comparison.
