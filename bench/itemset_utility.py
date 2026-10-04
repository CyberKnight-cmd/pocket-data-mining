#!/usr/bin/env python3
"""Exact utility of given itemsets by a full scan of an SPMF file.

usage: itemset_utility.py DATASET "i1 i2 ..." ["j1 ..." ...]
"""
import sys
db = sys.argv[1]; sets = [frozenset(map(int, s.split())) for s in sys.argv[2:]]
u = [0] * len(sets)
with open(db) as f:
    for line in f:
        a, _, b = line.rstrip("\n").split(":")
        tx = dict(zip(map(int, a.split()), map(int, b.split())))
        for k, s in enumerate(sets):
            if s <= tx.keys():
                u[k] += sum(tx[i] for i in s)
for s, x in zip(sets, u): print(sorted(s), x)
