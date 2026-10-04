#!/usr/bin/env python3
"""Replicate an SPMF utility dataset N times (a controlled large-scale exactness check).

usage: scale_dataset.py IN.txt N OUT.txt

The output is the input repeated N times. Every itemset's utility is then exactly N times its
utility in the input, so with min_util * N the HUIs are the same itemsets with utilities * N:
the expected output is known without a reference run.
"""
import hashlib, json, os, shutil, sys


def main():
    src, n, dest = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    tmp = dest + ".tmp"
    with open(tmp, "wb") as o:
        for _ in range(n):
            with open(src, "rb") as f:
                shutil.copyfileobj(f, o, 16 << 20)
    os.replace(tmp, dest)
    h = hashlib.sha256()
    with open(dest, "rb") as f:
        for block in iter(lambda: f.read(16 << 20), b""):
            h.update(block)
    stats = {"source": os.path.basename(src), "copies": n, "bytes": os.path.getsize(dest), "sha256": h.hexdigest()}
    json.dump(stats, open(dest + ".json", "w"), indent=1)
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
