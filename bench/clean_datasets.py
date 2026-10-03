#!/usr/bin/env python3
"""Write cleaned copies of SPMF utility datasets, identical input for every tool.

* drops metadata/comment lines (@..., #..., %...)
* merges an item listed more than once in a transaction (sums its utilities), keeping
  first-occurrence order — kosarak and liquor_11 contain a few such transactions
* checks that the transaction utility equals the sum of item utilities (reports, keeps)
* datasets with decimal utilities (liquor_11: dollar amounts) are scaled x100 to integer
  cents, since the miners (ours and SPMF's standard ones) take integer utilities
Writes DST/<name> and DST/CLEANING.json with per-file statistics.

usage: clean_datasets.py SRC_DIR DST_DIR
"""
import hashlib, json, os, sys

src, dst = sys.argv[1], sys.argv[2]
os.makedirs(dst, exist_ok=True)
report = {}
for name in sorted(os.listdir(src)):
    if name in ("SHA256SUMS",) or name.startswith(".") or os.path.isdir(os.path.join(src, name)):
        continue
    stats = {"tx": 0, "dup_tx": 0, "dropped_lines": 0, "tu_mismatch": 0, "items": set(), "len_sum": 0, "max_len": 0}
    # Decimal utilities? (look only at data lines)
    scale = 1
    with open(os.path.join(src, name)) as f:
        for line in f:
            if line[:1] not in "@#%" and line.count(":") >= 2 and "." in line.split(":", 1)[1]:
                scale = 100
                break
    stats["utility_scale"] = scale
    num = (lambda x: int(x)) if scale == 1 else (lambda x: int(round(float(x) * scale)))
    h = hashlib.sha256()
    with open(os.path.join(src, name)) as fin, open(os.path.join(dst, name), "w") as fout:
        for line in fin:
            line = line.strip()
            if not line or line[0] in "@#%" or line.count(":") < 2:
                stats["dropped_lines"] += 1
                continue
            a, tu, u = line.split(":", 2)
            items, utils = a.split(), u.split()
            merged = {}
            for i, x in zip(items, utils):
                merged[i] = merged.get(i, 0) + num(x)
            if len(merged) != len(items):
                stats["dup_tx"] += 1
            if sum(merged.values()) != num(tu):
                stats["tu_mismatch"] += 1
            # After scaling/rounding, the transaction utility is defined as the item sum.
            tu = sum(merged.values()) if scale != 1 else tu
            out = f"{' '.join(merged)}:{tu}:{' '.join(str(v) for v in merged.values())}\n"
            fout.write(out)
            h.update(out.encode())
            stats["tx"] += 1
            stats["items"].update(merged)
            stats["len_sum"] += len(merged)
            stats["max_len"] = max(stats["max_len"], len(merged))
    stats["distinct_items"] = len(stats.pop("items"))
    stats["avg_len"] = round(stats.pop("len_sum") / max(stats["tx"], 1), 3)
    stats["density"] = round(stats["avg_len"] / max(stats["distinct_items"], 1), 6)
    stats["sha256"] = h.hexdigest()
    report[name] = stats
    print(name, stats, flush=True)
json.dump(report, open(os.path.join(dst, "CLEANING.json"), "w"), indent=1)
