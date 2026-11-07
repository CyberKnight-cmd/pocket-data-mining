#!/usr/bin/env python3
"""Derive tables from raw collector data (never re-runs anything).

usage: analyze.py DATA_DIR [OUT_DIR]
Reads DATA_DIR/runs/*/result.json (+ output.norm.gz), recomputes exactness against the
reference outputs, and writes:
  OUT_DIR/runs.csv          one row per run, all scalar fields
  OUT_DIR/summary.md        headline numbers and per-experiment tables
"""
import csv, glob, gzip, hashlib, json, os, sys
from collections import defaultdict

data = sys.argv[1]
out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(data, "analysis")
os.makedirs(out, exist_ok=True)

runs = []
for f in glob.glob(os.path.join(data, "runs", "*", "result.json")):
    r = json.load(open(f))
    r["_dir"] = os.path.dirname(f)
    runs.append(r)


def kind(algo):
    return {"efim-closed": "closed", "haui-miner": "avg", "huim-mmu": "mmu", "shuim": "windows"}.get(algo, "hui")


def digest(r):
    return r.get("output_sha256")


# References: our run marked is_reference, per (dataset, min_util, kind).
refs = {}
for r in runs:
    j = r["job"]
    if j.get("is_reference") and r["status"] == "ok":
        refs[(j["dataset"], j.get("min_util"), kind(j["algo"]))] = r


def load(r):
    p = os.path.join(r["_dir"], "output.norm.gz")
    return set(l.rstrip("\n") for l in gzip.open(p, "rt") if l.strip()) if os.path.exists(p) else None


ref_sets = {}
for r in runs:
    j = r["job"]
    r["_exact"] = ""
    if r["status"] != "ok" or j["algo"] in ("tko", "tku", "rept") or j.get("k"):
        continue
    ref = refs.get((j["dataset"], j.get("min_util"), kind(j["algo"])))
    if not ref:
        continue
    if digest(r) == digest(ref):
        r["_exact"] = "yes"
        continue
    key = id(ref)
    if key not in ref_sets:
        ref_sets[key] = load(ref)
    got, exp = load(r), ref_sets[key]
    if got is None or exp is None:
        continue
    tp = len(got & exp)
    r["_precision"] = tp / len(got) if got else 1.0
    r["_recall"] = tp / len(exp) if exp else 1.0
    r["_exact"] = "subset" if got <= exp else "NO"

cols = ["experiment", "dataset", "impl", "algo", "budget_mb", "threads", "watchdog", "min_util", "k",
        "status", "wall_s", "cpu_user_s", "cpu_sys_s", "peak_rss_mb", "within_budget", "peak_spill_mb",
        "huis", "exact", "precision", "recall", "ledger_peak_mb", "id"]
with open(os.path.join(out, "runs.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for r in sorted(runs, key=lambda r: (r["job"]["experiment"], r["job"]["dataset"], r["job"]["algo"],
                                         r["job"]["impl"], r["job"].get("budget_mb") or 0)):
        j = r["job"]
        lp = ""
        m = r.get("air_memory_line", "")
        if "ledger peak" in m:
            lp = m.split("ledger peak")[1].split("MB")[0].strip()
        w.writerow([j["experiment"], j["dataset"], j["impl"], j["algo"], j.get("budget_mb"), j["threads"],
                    j.get("watchdog"), j.get("min_util"), j.get("k"), r["status"], r["wall_s"],
                    r.get("cpu_user_s"), r.get("cpu_sys_s"), r["peak_rss_mb"], r.get("within_budget"),
                    r.get("peak_spill_mb"), r.get("huis"), r["_exact"],
                    round(r["_precision"], 4) if "_precision" in r else "",
                    round(r["_recall"], 4) if "_recall" in r else "", lp, r["id"]])


def cell(r):
    if r is None:
        return "—"
    s = r["status"]
    over = "" if r.get("within_budget") in (True, None) else " ⚠"
    ex = {"yes": "", "subset": "", "NO": " ✗", "": ""}[r["_exact"]]
    if "_recall" in r and r["_exact"] == "subset":
        ex = f" (recall {r['_recall']:.0%})"
    if s == "ok":
        return f"{r['wall_s']:.1f}s / {r['peak_rss_mb']:.0f}MB{over}{ex}"
    if s == "cap":
        return f"cap / {r['peak_rss_mb']:.0f}MB{over}"
    return f"{s} / {r['peak_rss_mb']:.0f}MB{over}"


lines = ["# Air-HUIM vs SPMF — Raspberry Pi 4 (4 GB)\n",
         f"{len(runs)} runs analysed. Cells: wall time / peak RSS. ⚠ RSS above budget, ✗ wrong result, "
         "`cap` = stopped at the time cap, OOM = Java heap exhausted, budget-kill = watchdog stopped it.\n"]
# Headline
for exp in ("cmp", "hard", "ablation"):
    rs = [r for r in runs if r["job"]["experiment"] == exp]
    if not rs:
        continue
    lines.append(f"## Headline: {exp}\n")
    lines.append("| implementation | runs | within budget | finished | OOM / killed | wrong results |")
    lines.append("|---|---|---|---|---|---|")
    for impl in sorted({r["job"]["impl"] for r in rs}):
        x = [r for r in rs if r["job"]["impl"] == impl]
        lines.append(f"| {impl} | {len(x)} | {sum(r.get('within_budget') is True for r in x)} | "
                     f"{sum(r['status'] == 'ok' for r in x)} | "
                     f"{sum(r['status'] in ('OOM', 'budget-kill') for r in x)} | {sum(r['_exact'] == 'NO' for r in x)} |")
    lines.append("")

# Comparison tables per dataset/budget
cmp = defaultdict(dict)
for r in runs:
    j = r["job"]
    if j["experiment"] in ("cmp", "ref"):
        cmp[(j["dataset"], j.get("budget_mb"))][(j["impl"], j["algo"])] = r
for (ds, b) in sorted(cmp, key=lambda k: (k[0], k[1] or 0)):
    if b == 2048:
        continue
    t = cmp[(ds, b)]
    algos = sorted({a for (_, a) in t})
    lines.append(f"### {ds}, budget {b} MB\n")
    lines.append("| algorithm | Air-HUIM | SPMF |")
    lines.append("|---|---|---|")
    for a in algos:
        lines.append(f"| {a} | {cell(t.get(('air', a)))} | {cell(t.get(('spmf', a)))} |")
    lines.append("")
open(os.path.join(out, "summary.md"), "w").write("\n".join(lines))
print(f"{len(runs)} runs -> {out}/runs.csv, {out}/summary.md")
