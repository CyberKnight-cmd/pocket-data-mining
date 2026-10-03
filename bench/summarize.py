#!/usr/bin/env python3
"""Summarize pi_bench.py results into Markdown tables.

usage: summarize.py results.csv > summary.md
"""
import csv, sys
from collections import defaultdict

rows = list(csv.DictReader(open(sys.argv[1])))
by = defaultdict(dict)  # (dataset, budget) -> {(impl, algo): row}
for r in rows:
    by[(r["dataset"], int(r["budget_mb"]))][(r["impl"], r["algorithm"])] = r


def cell(r):
    if r is None:
        return "—"
    st = r["status"]
    rss = float(r["peak_rss_mb"])
    mark = "" if r["within_budget"] == "yes" else " ⚠"
    if st == "ok":
        ex = {"yes": "", "NO": " ✗wrong", "": ""}.get(r["exact"], "")
        return f"{float(r['seconds']):.1f}s / {rss:.0f}MB{mark}{ex}"
    if st == "cap":
        return f"> cap / {rss:.0f}MB{mark}"
    return f"{st} / {rss:.0f}MB{mark}"


shared = ["two-phase", "ihup", "up-growth", "up-growth-plus", "hui-miner", "fhm", "fhm-plus",
          "hup-miner", "mhuiminer", "efim", "efim-closed", "tko", "tku"]
print("# Air-HUIM vs SPMF on Raspberry Pi 4\n")
print("Cells: wall time / peak process RSS. ⚠ = RSS above the budget, ✗wrong = HUI set differs "
      "from the reference, `> cap` = did not finish within the time cap.\n")
for (ds, b) in sorted(by, key=lambda k: (k[0] != "foodmart", k[0], k[1])):
    t = by[(ds, b)]
    print(f"## {ds}, budget {b} MB\n")
    print("| algorithm | Air-HUIM (Rust) | SPMF (Java, -Xmx{0}m) |".format(b))
    print("|---|---|---|")
    for a in shared:
        print(f"| {a} | {cell(t.get(('air', a)))} | {cell(t.get(('spmf', a)))} |")
    others = sorted({a for (i, a) in t if i == "air"} - set(shared))
    if others:
        print(f"\nAir-HUIM only: " + ", ".join(f"{a} {cell(t[('air', a)])}" for a in others))
    print()

# Headline numbers
print("## Summary\n")
for impl in ("air", "spmf"):
    rs = [r for r in rows if r["impl"] == impl]
    n = len(rs)
    within = sum(r["within_budget"] == "yes" for r in rs)
    ok = sum(r["status"] == "ok" for r in rs)
    wrong = sum(r["exact"] == "NO" for r in rs)
    oom = sum(r["status"] == "OOM" for r in rs)
    print(f"- **{'Air-HUIM' if impl == 'air' else 'SPMF'}**: {n} runs, {within} within budget, "
          f"{ok} finished within the cap, {oom} out-of-memory, {wrong} wrong results")
