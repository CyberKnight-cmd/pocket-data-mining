#!/usr/bin/env python3
"""Plan for the 1-thread timing table: every (algorithm, dataset) pair where both Air-HUIM and
SPMF finished in a finished run's `cmp` experiment at BUDGET MB, repeated REPS times with one
thread each. Report the median per pair.

usage: timing_plan.py OLD_PLAN.json OLD_DATA_DIR DATASET_DIR > timing_plan.json
"""
import json, os, sys

BUDGET, REPS, CAP_S = 1024, 5, 3600
old_plan, old_data, ddir = sys.argv[1], sys.argv[2], os.path.expanduser(sys.argv[3])
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import collect

plan = json.load(open(old_plan))
status = {}
for j in plan["jobs"]:
    j2 = dict(j, dataset_sha=plan["config"]["datasets"][j["dataset"]].get("sha256"))
    p = os.path.join(old_data, "runs", collect.job_id(j2), "result.json")
    if j["experiment"] == "cmp" and j.get("budget_mb") == BUDGET and os.path.exists(p):
        status[(j["impl"], j["algo"], j["dataset"])] = (json.load(open(p))["status"], j)
pairs = sorted({(a, d) for (i, a, d) in status
                if all(status.get((x, a, d), ("",))[0] == "ok" for x in ("air", "spmf"))})

out = []
for r in range(REPS):
    for a, d in pairs:
        for impl in ("air", "spmf"):
            j = dict(status[(impl, a, d)][1], experiment="timing", threads=1, rep=r, cap_s=CAP_S,
                     priority=r * 10000 + len(out))
            out.append(j)

cfg = plan["config"]
cfg["binaries"] = {"air": os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "target/release/air-huim"))}
cfg["java"] = "/usr/bin/java"
cfg["spmf_jar"] = os.path.expanduser("~/.cache/air-huim-bench/mhui/spmf.jar")
for name, ds in cfg["datasets"].items():
    ds["path"] = os.path.join(ddir, os.path.basename(ds["path"]))
print(f"{len(pairs)} pairs x {REPS} reps x 2 = {len(out)} jobs", file=sys.stderr)
json.dump({"config": cfg, "pending_algorithms": [], "jobs": out}, sys.stdout, indent=1)
