#!/usr/bin/env python3
"""Generate the experiment plan (jobs for bench/collect.py).

usage: plan.py calibration.json DATASET_DIR > plan.json

Experiments (priority order):
  ref       reference outputs (Air-HUIM FHM, large budget) for exactness checks
  cmp       Air-HUIM vs SPMF, same algorithm, same budget (SPMF heap = -Xmx budget)
  curve     runtime/memory vs budget (degradation curves), Air-HUIM
  ablation  original Air-HUIM code (before budget work) vs current, same budgets
  hard      hard limit: watchdog kills any process whose RSS exceeds the budget
  threads   Air-HUIM thread scaling
  spmf-free SPMF with no heap cap (its natural footprint)
`code_version` must be bumped for an algorithm whenever its code changes; finished
results of other algorithms are then kept (see collect.py job_id).
"""
import hashlib, json, os, sys

CAL = json.load(open(sys.argv[1])) if len(sys.argv) > 1 and os.path.exists(sys.argv[1]) else {}
# Average-utility (HAUI-Miner) thresholds are calibrated separately: u(X)/|X| needs a much
# smaller threshold than total utility to produce a meaningful result.
_cs = os.path.join(os.path.dirname(os.path.abspath(__file__)), "calibration_shuim.json")
CAL_SHUIM = json.load(open(_cs)) if os.path.exists(_cs) else {}
_ch = os.path.join(os.path.dirname(os.path.abspath(__file__)), "calibration_haui.json")
CAL_AVG = json.load(open(_ch)) if os.path.exists(_ch) else {}
DDIR = sys.argv[2] if len(sys.argv) > 2 else "~/datasets/clean"
# Cleaning report (same bytes on every machine) gives each dataset's SHA-256.
_cr = os.path.join(os.path.expanduser(sys.argv[3] if len(sys.argv) > 3 else DDIR), "CLEANING.json")
CLEANING = json.load(open(_cr)) if os.path.exists(_cr) else {}
ONLY = set(os.environ.get("PLAN_ONLY", "").split(",")) - {""}

# Algorithm -> code version. Bump when an algorithm's implementation changes.
CODE = {
    "two-phase": "tp-2", "ihup": "twu-2", "fhm": "ul-2", "fhm-plus": "ul-2", "hui-miner": "ul-2",
    "efim-closed": "proj-2", "tko": "ul-2",
    "hup-miner": "hup-1", "mhuiminer": "mhui-1", "haui-miner": "haui-1",
    "huim-ga": "heur-2", "huim-bpso": "heur-2", "mhui-aco": "heur-2",
    # re-implemented as distinct algorithms (round 2)
    "efim": "efim-merge-1", "up-growth": "upg-dlu-1", "up-growth-plus": "upgp-1",
    "hup-tree": "huptree-1", "hui-trie": "huitrie-1", "tku": "tku-1", "rept": "rept-1",
    "huim-mmu": "mmu-1", "shuim": "shuim-1", "incfhm": "incfhm-1",
}
PENDING = set()
# r2: constant-memory (streaming) joins, admission control, margin 7 MB + 3%.
RUNTIME = "r2"
HEURISTIC = {"huim-ga", "huim-bpso", "mhui-aco"}

SPMF = {  # air algo -> (SPMF name, args(min_util, k))
    "two-phase": ("Two-Phase", lambda mu, k: [mu]),
    "ihup": ("IHUP", lambda mu, k: [mu]),
    "up-growth": ("UPGrowth", lambda mu, k: [mu]),
    "up-growth-plus": ("UPGrowth+", lambda mu, k: [mu]),
    "hui-miner": ("HUI-Miner", lambda mu, k: [mu]),
    "fhm": ("FHM", lambda mu, k: [mu]),
    "fhm-plus": ("FHM+", lambda mu, k: [mu, 1, 1000]),
    "hup-miner": ("HUP-Miner", lambda mu, k: [mu, 8]),
    "mhuiminer": ("mHUIMiner", lambda mu, k: [mu]),
    "efim": ("EFIM", lambda mu, k: [mu]),
    "efim-closed": ("EFIM-Closed", lambda mu, k: [mu]),
    "tko": ("TKO", lambda mu, k: [k]),
    "tku": ("TKU", lambda mu, k: [k]),
    "haui-miner": ("HAUI-Miner", lambda mu, k: [mu]),
}

# Dataset -> (file, fallback percent of total utility if calibration has nothing).
DATASETS = {
    "foodmart": ("foodmart.txt", None), "chainstore": ("chainstore.txt", 0.05),
    "ecommerce": ("ecommerce_utility_no_timestamps.txt", 0.5), "fruithut": ("fruithut_utility.txt", 0.5),
    "liquor": ("liquor_11.txt", 0.5), "chicago": ("Chicago_Crimes_2001_to_2017_utility.txt", 0.05),
    "retail": ("retail_utility_spmf.txt", 0.05), "mushroom": ("mushroom_utility_spmf.txt", 5),
    "pumsb": ("pumsb_utility_spmf.txt", 10), "chess": ("chess_utility_spmf.txt", 10),
    "connect": ("connect_utility_spmf.txt", 20), "accidents": ("accidents_utility_spmf.txt", 10),
    "kosarak": ("kosarak_utility_spmf.txt", 0.2), "bms": ("BMS_utility_spmf.txt", 5),
}
FIXED_MU = {"foodmart": 3000}
# Dense datasets: percent of total utility, aligned with the ranges used in the HUIM
# literature (calibration at <=10% did not finish); pumsb: 25% gives 0 HUIs, 20% too slow.
FIXED_PCT = {"chess": 25, "connect": 32, "accidents": 12, "bms": 2.1, "pumsb": 23}
K = 100


def pick(name, fname):
    if name in FIXED_MU:
        return FIXED_MU[name], "fixed"
    if name in FIXED_PCT and fname in CAL:
        return int(CAL[fname]["total_utility"] * FIXED_PCT[name] / 100), f"fixed {FIXED_PCT[name]}%"
    c = CAL.get(fname)
    if c:
        ok = []
        for t in c["trials"]:
            times = [v[0] for v in (t["efim"], t["fhm"]) if v[1] >= 0]
            huis = max(t["efim"][1], t["fhm"][1])
            if times and min(times) <= 5 and huis >= 50:
                ok.append(t)
        if ok:
            t = ok[-1]  # lowest threshold that still finishes fast locally
            return t["min_util"], f"calibrated {t['pct']}%"
        return c["trials"][0]["min_util"], f"calibrated-fallback {c['trials'][0]['pct']}%"
    return None, "fallback-pct"


# Dense datasets (HAUI): local calibration at 2% did not finish; values from a second pass.
FIXED_AVG_PCT = {"chess_utility_spmf.txt": 5, "accidents_utility_spmf.txt": 5,
                 "connect_utility_spmf.txt": 10, "pumsb_utility_spmf.txt": 5}


def pick_avg(fname):
    if fname in FIXED_AVG_PCT and fname in CAL:
        return int(CAL[fname]["total_utility"] * FIXED_AVG_PCT[fname] / 100)
    c = CAL_AVG.get(fname)
    if not c:
        return None
    ok = [t for t in c["trials"] if 0 <= t["secs"] <= 5 and t["itemsets"] >= 50]
    return ok[-1]["min_util"] if ok else c["trials"][-1]["min_util"]


def main():
    jobs = []
    datasets = {}
    for name, (fname, pct) in DATASETS.items():
        mu, how = pick(name, fname)
        path = os.path.join(DDIR, fname)
        datasets[name] = {"path": path, "min_util": mu, "min_util_avg": pick_avg(fname),
                          "pct_fallback": pct, "threshold_source": how,
                          "sha256": CLEANING.get(fname, {}).get("sha256"), "stats": CLEANING.get(fname)}

    def J(**kw):
        if ONLY and kw["dataset"] not in ONLY:
            return
        if datasets[kw["dataset"]]["min_util"] is None and kw.get("k") is None:
            return  # not calibrated yet
        kw.setdefault("threads", 4)
        kw.setdefault("watchdog", False)
        d = datasets[kw["dataset"]]
        if kw["algo"] == "haui-miner" and d.get("min_util_avg"):
            kw["min_util"] = d["min_util_avg"]
            if kw["impl"] == "spmf":
                kw["spmf_args"] = [d["min_util_avg"]]
        if kw.get("k") is None and "min_util" not in kw:
            kw["min_util"] = d["min_util"]
            # SHUIM mines 1000-transaction windows; its threshold is calibrated separately
            # (scaling the global threshold by window size explodes: one window wrote 36 GB).
            if kw["algo"] == "shuim":
                c = CAL_SHUIM.get(kw["dataset"], {})
                first = (c.get("trials") or [{}])[0]
                if c.get("chosen"):
                    kw["min_util"] = c["chosen"]
                elif first.get("status") == "too-much-output":
                    kw["min_util"] = d["min_util"] * 4
        # Each result kind has its own reference: HUIs, closed HUIs, average-utility itemsets.
        kind = {"efim-closed": "closed", "haui-miner": "avg", "huim-mmu": "mmu", "shuim": "windows"}.get(kw["algo"], "hui")
        kw["ref_key"] = f"{kw['dataset']}|{kw.get('min_util')}|{kind}"
        if kw["algo"] in ("tko", "tku", "rept"):
            kw["compare"] = "none"
        elif kw["algo"] in HEURISTIC:
            kw["compare"] = "subset"
        else:
            kw["compare"] = "exact"
        if kw["algo"] == "haui-miner" and kw["impl"] == "air":
            kw["average_output"] = True
        if kw["impl"] in ("air", "air-baseline"):
            kw["code_version"] = CODE[kw["algo"]] if kw["impl"] == "air" else "baseline-dc6bd1b"
        if kw["impl"] == "air":
            # Changes that affect every algorithm (memory margin, streaming joins, admission
            # control) bump this, so all Air-HUIM runs in one analysis use the same runtime.
            kw["runtime_version"] = RUNTIME
        jobs.append(kw)

    budgets_cmp = [64, 256, 1024]
    order = ["foodmart", "chainstore", "retail", "kosarak", "accidents", "chicago", "mushroom", "chess",
             "bms", "ecommerce", "fruithut", "liquor", "connect", "pumsb"]
    for di, ds in enumerate(order):
        base = di * 1000
        # References first (one per result kind), then our runs on every dataset, then SPMF.
        for algo in ("fhm", "efim-closed", "haui-miner", "huim-mmu", "shuim"):
            J(experiment="ref", impl="air", algo=algo, dataset=ds, budget_mb=2048, cap_s=7200, priority=di,
              is_reference=True)
        for b in budgets_cmp:
            for algo in CODE:
                topk = {"k": K} if algo in ("tko", "tku", "rept") else {}
                J(experiment="cmp", impl="air", algo=algo, dataset=ds, budget_mb=b, cap_s=600,
                  priority=100 + di, **topk)
                if algo in SPMF:
                    sname, args = SPMF[algo]
                    mu = datasets[ds]["min_util"]
                    J(experiment="cmp", impl="spmf", algo=algo, dataset=ds, budget_mb=b, cap_s=600,
                      priority=200 + di, spmf_name=sname, spmf_args=args(mu, K),
                      code_version="spmf", **topk)
    # Degradation curves on the large / dense datasets.
    for ds in ["chainstore", "kosarak", "accidents", "retail", "chicago"]:
        for b in [32, 48, 64, 96, 128, 192, 256, 384, 512, 1024, 2048]:
            for algo in ["fhm", "efim", "hui-miner", "two-phase", "up-growth"]:
                J(experiment="curve", impl="air", algo=algo, dataset=ds, budget_mb=b, cap_s=1800,
                  priority=20000 + order.index(ds))
    # Ablation: the original implementation, same budgets.
    for ds in ["foodmart", "chainstore", "retail", "kosarak", "accidents"]:
        for b in budgets_cmp:
            for algo in ["fhm", "efim", "hui-miner", "two-phase", "up-growth", "ihup", "tko"]:
                topk = {"k": K} if algo == "tko" else {}
                J(experiment="ablation", impl="air-baseline", algo=algo, dataset=ds, budget_mb=b, cap_s=600,
                  priority=30000 + order.index(ds), **topk)
    # Hard limit (watchdog) comparison.
    for ds in ["chainstore", "retail", "kosarak", "accidents"]:
        for b in [64, 128]:
            for algo in ["fhm", "efim", "hui-miner", "up-growth"]:
                J(experiment="hard", impl="air", algo=algo, dataset=ds, budget_mb=b, cap_s=900, watchdog=True,
                  priority=40000)
                sname, args = SPMF[algo]
                J(experiment="hard", impl="spmf", algo=algo, dataset=ds, budget_mb=b, cap_s=900, watchdog=True,
                  priority=40001, spmf_name=sname, spmf_args=args(datasets[ds]["min_util"], K),
                  code_version="spmf")
    # Kernel-enforced hard limit (cgroup MemoryMax, no swap): both tools, same cap.
    for ds in ["foodmart", "chainstore", "retail", "kosarak", "accidents", "chicago"]:
        for b in [64, 128, 256]:
            for algo in ["fhm", "efim", "hui-miner", "up-growth", "two-phase"]:
                J(experiment="cgroup", impl="air", algo=algo, dataset=ds, budget_mb=b, cap_s=900, limit="cgroup",
                  priority=150 + order.index(ds))
                sname, args = SPMF[algo]
                J(experiment="cgroup", impl="spmf", algo=algo, dataset=ds, budget_mb=b, cap_s=900, limit="cgroup",
                  priority=250 + order.index(ds), spmf_name=sname,
                  spmf_args=args(datasets[ds]["min_util"], K), code_version="spmf")
    # Thread scaling.
    for ds in ["chainstore", "kosarak", "accidents"]:
        for t in [1, 2, 4]:
            for algo in ["fhm", "efim", "hui-miner"]:
                J(experiment="threads", impl="air", algo=algo, dataset=ds, budget_mb=1024, threads=t, cap_s=1800,
                  priority=50000)
    # SPMF without a heap cap.
    for ds in order:
        for algo in ["fhm", "efim", "hui-miner", "up-growth"]:
            sname, args = SPMF[algo]
            J(experiment="spmf-free", impl="spmf", algo=algo, dataset=ds, budget_mb=None, cap_s=600,
              priority=60000, spmf_name=sname, spmf_args=args(datasets[ds]["min_util"], K), code_version="spmf")

    plan = {
        "config": {
            "binaries": {"air": "~/air-huim/target/release/air-huim",
                         "air-baseline": "~/air-huim-baseline/target/release/air-huim"},
            "java": "~/bench/java/bin/java", "spmf_jar": "~/bench/spmf.jar",
            "datasets": datasets,
        },
        "pending_algorithms": sorted(PENDING),
        "jobs": jobs,
    }
    json.dump(plan, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
