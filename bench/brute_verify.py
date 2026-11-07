#!/usr/bin/env python3
"""Settle disagreements between implementations by brute force, independently of Air-HUIM.

For every dataset in a collect.py data directory, each SPMF output is compared with the
reference (Air-HUIM's reference run, or its own FHM run when the plan has no reference).
Every disputed itemset (reported by one side only, or with different utilities) gets its exact
utility from one plain scan of the SPMF-format file: sum of the itemset's item utilities over
the transactions that contain it. Top-K runs are compared with Air-HUIM's TKO output; all
itemsets of both lists are checked.

usage: brute_verify.py PLAN.json DATA_DIR [--out report.json]
"""
import gzip, json, os, sys
from collections import defaultdict

plan, data = sys.argv[1], sys.argv[2]
out_path = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else None
cfg = json.load(open(plan))["config"]
runs = os.path.join(data, "runs")


def load_output(run_id):
    """{frozenset(items): utility} from a run's normalized output."""
    r = {}
    p = os.path.join(runs, run_id, "output.norm.gz")
    if not os.path.exists(p):
        return None
    with gzip.open(p, "rt") as f:
        for line in f:
            items, u = line.rstrip("\n").split("\t")
            v = float(u)  # some SPMF algorithms print utilities as "3990.00"
            r[frozenset(map(int, items.split()))] = int(v) if v.is_integer() else v
    return r


def exact_utilities(path, itemsets):
    """One scan; itemsets indexed by their smallest item so each transaction checks few."""
    by_first = defaultdict(list)
    for k, s in enumerate(itemsets):
        by_first[min(s)].append(k)
    util = [0] * len(itemsets)
    with open(os.path.expanduser(path)) as f:
        for line in f:
            if not line[:1].isdigit():
                continue
            a, _, b = line.rstrip("\n").split(":")
            items = a.split()
            if not any(int(i) in by_first for i in items):
                continue
            tx = dict(zip(map(int, items), map(int, b.split())))
            for i in tx:
                for k in by_first.get(i, ()):
                    s = itemsets[k]
                    if s <= tx.keys():
                        util[k] += sum(tx[x] for x in s)
    return dict(zip(itemsets, util))


jobs = {}
for d in os.listdir(runs):
    try:
        jobs[d] = (json.load(open(os.path.join(runs, d, "job.json"))),
                   json.load(open(os.path.join(runs, d, "result.json"))))
    except (OSError, ValueError):
        pass
refs = json.load(open(os.path.join(data, "references.json")))

report = {}
for ds, meta in cfg["datasets"].items():
    mu = meta.get("min_util")
    ok = [(d, j, r) for d, (j, r) in jobs.items() if j["dataset"] == ds and r["status"] == "ok"]
    ref = refs.get(f"{ds}|{mu}|hui")
    ref_id = ref["id"] if ref else None
    if ref_id is None:  # no reference job: our FHM at the largest budget
        ours = sorted([x for x in ok if x[1]["impl"] == "air" and x[1]["algo"] == "fhm"],
                      key=lambda x: x[1].get("budget_mb") or 0)
        ref_id = ours[-1][0] if ours else None
    tko = sorted([x for x in ok if x[1]["impl"] == "air" and x[1]["algo"] == "tko"],
                 key=lambda x: x[1].get("budget_mb") or 0)
    tko_id = tko[-1][0] if tko else None

    loaded = {}  # ref key -> output; each run is compared with its own reference (hui/closed/avg)
    T = load_output(tko_id) if tko_id else None
    cases = []  # (run_id, job, output, compared-with, kind, min_util)
    for d, j, r in ok:
        if j["impl"] != "spmf":
            continue
        S = load_output(d)
        if S is None:
            continue
        if j.get("k"):
            if T is not None:
                cases.append((d, j, S, T, "topk", None))
            continue
        key = j.get("ref_key") or f"{ds}|{mu}|hui"
        rid = refs[key]["id"] if key in refs else (ref_id if key.endswith("|hui") else None)
        if rid and key not in loaded:
            loaded[key] = load_output(rid)
        if rid and loaded[key] is not None:
            cases.append((d, j, S, loaded[key], key.rsplit("|", 1)[-1], int(key.split("|")[1])))
    if not cases:
        continue
    disputed = set()
    for d, j, S, base, kind, _ in cases:
        if kind == "topk":
            disputed |= set(S) | set(base)
        else:
            disputed |= {x for x in set(S) | set(base) if S.get(x) != base.get(x)}
    if not disputed:
        continue
    print(f"[{ds}] {len(disputed)} disputed itemsets, scanning {meta['path']} ...", file=sys.stderr, flush=True)
    truth = exact_utilities(meta["path"], sorted(disputed, key=sorted))
    rows = []
    for d, j, S, base, kind, cmu in cases:
        if kind == "avg":  # average utility, printed with 2 decimals
            tv = lambda x: round(truth[x] / len(x), 2)
        else:
            tv = lambda x: truth[x]
        if kind != "topk":
            miss = [x for x in base if x not in S]
            extra = [x for x in S if x not in base]
            wrong_u = [x for x in S if x in base and S[x] != base[x]]
            rows.append({
                "run": d, "algo": j["algo"], "budget_mb": j.get("budget_mb"), "kind": kind,
                "spmf_count": len(S), "ref_count": len(base),
                "missed_by_spmf": len(miss),
                "missed_are_true_huis": sum(tv(x) >= cmu for x in miss),
                "spmf_extra": len(extra), "spmf_extra_true_huis": sum(tv(x) >= cmu for x in extra),
                "utility_differs": len(wrong_u),
                "spmf_utility_correct": sum(tv(x) == S[x] for x in wrong_u),
                "ref_utility_correct": sum(tv(x) == base[x] for x in wrong_u),
                "ref_all_exact": all(tv(x) == u for x, u in base.items() if x in truth),
            })
        else:
            kth_ours = min(base.values())
            rows.append({
                "run": d, "algo": j["algo"], "budget_mb": j.get("budget_mb"), "kind": kind,
                "spmf_utilities_exact": sum(truth[x] == u for x, u in S.items()), "spmf_count": len(S),
                "ours_utilities_exact": sum(truth[x] == u for x, u in base.items()), "ours_count": len(base),
                "spmf_true_utility_below_ours_kth": sum(truth[x] < kth_ours for x in S),
                "ours_kth_utility": kth_ours,
            })
    report[ds] = {"min_util": mu, "reference_run": ref_id, "tko_run": tko_id, "rows": rows}
    print(json.dumps({ds: report[ds]}, indent=1), flush=True)

if out_path:
    json.dump(report, open(out_path, "w"), indent=1)
