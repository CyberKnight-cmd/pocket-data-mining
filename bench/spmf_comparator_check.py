#!/usr/bin/env python3
"""Count item pairs that SPMF FHM/FHM+ order wrongly: sign((int32)(a-b)) != sign(a-b) on TWUs.

usage: spmf_comparator_check.py DATASET MIN_UTIL
"""
import sys, numpy as np
twu = {}
with open(sys.argv[1]) as f:
    for line in f:
        a, tu, _ = line.split(":")
        tu = int(tu)
        for i in a.split():
            twu[i] = twu.get(i, 0) + tu
min_u = int(sys.argv[2])
v = np.array(sorted(t for t in twu.values() if t >= min_u), dtype=np.int64)
d = v[:, None] - v[None, :]
wrapped = ((d + 2**31) % 2**32 - 2**31)
bad = np.sign(wrapped) != np.sign(d)
print(f"promising items {len(v)}, max TWU {v.max():,}, pairs ordered wrongly: {int(bad.sum()) // 2:,} of {len(v) * (len(v) - 1) // 2:,}")
