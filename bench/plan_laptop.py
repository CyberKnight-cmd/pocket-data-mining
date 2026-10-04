#!/usr/bin/env python3
"""Large-budget plan (1-20 GB) on a workstation, for bench/collect.py.

usage: plan_laptop.py calibration_large.json AIR_BINARY SPMF_JAR > plan_laptop.json

Datasets: dunnhumby LGSR (all baskets, customer-weeks, first year), H&M (customer-days) and
chainstore replicated x10 / x40 (exact expected output: the x1 HUIs with utilities x N).

Experiments (priority order):
  ref         Air-HUIM FHM at 20 GB, 16 threads: reference outputs for exactness
  big-cmp     Air-HUIM vs SPMF (heap = -Xmx budget), budgets 1, 4, 16, 20 GB
  big-threads Air-HUIM thread scaling at 20 GB (1, 4, 8, 16 threads)
  big-cgroup  both tools under a kernel memory limit (cgroup MemoryMax), 1 and 4 GB
  big-size    LGSR first year vs all 117 weeks (data-size scaling), 1 and 20 GB
Runs are skipped (not recorded) while the machine has less than budget + 1 GB available.
"""
import json, os, sys

sys.argv, _argv = ["plan.py"], sys.argv  # import plan.py's tables without running it
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plan as base  # noqa: E402
sys.argv = _argv

LARGE = os.path.expanduser("~/.cache/air-huim-bench/large")
DATASETS = {
    "lgsr": f"{LARGE}/lgsr/lgsr.txt",
    "lgsr-custweek": f"{LARGE}/lgsr/lgsr_custweek.txt",
    "lgsr-1yr": f"{LARGE}/lgsr/lgsr_1yr.txt",
    "hm": f"{LARGE}/hm.txt",
    "chainstore-x10": f"{LARGE}/chainstore_x10.txt",
    "chainstore-x40": f"{LARGE}/chainstore_x40.txt",
}
# Scaled chainstore: the x1 threshold times N gives the same HUIs (utilities x N).
SCALED = {"chainstore-x10": 10, "chainstore-x40": 40}
CHAINSTORE_MU = 2609973
ALGOS = ["fhm", "efim", "hui-miner", "up-growth-plus", "efim-closed", "tko", "two-phase"]
BIG = [1024, 4096, 16384, 20480]
CAP = 1800


def main():
    cal = json.load(open(sys.argv[1]))
    air, jar = sys.argv[2], sys.argv[3]
    datasets = {}
    for name, path in DATASETS.items():
        if name in SCALED:
            mu, how = CHAINSTORE_MU * SCALED[name], f"chainstore x1 threshold x{SCALED[name]}"
        else:
            c = cal.get(name, {})
            mu, how = c.get("chosen"), f"calibrated {c.get('chosen_pct')}%"
        meta = path + ".json"
        sha = json.load(open(meta)).get("sha256") if os.path.exists(meta) else None
        datasets[name] = {"path": path, "min_util": mu, "threshold_source": how, "sha256": sha}
    jobs = []

    def J(**kw):
        d = datasets[kw["dataset"]]
        if d["min_util"] is None or not os.path.exists(d["path"]):
            return
        kw.setdefault("threads", 16 if kw["impl"] == "air" else 1)
        kw["min_util"] = d["min_util"]
        kind = "closed" if kw["algo"] == "efim-closed" else "hui"
        kw["ref_key"] = f"{kw['dataset']}|{d['min_util']}|{kind}"
        kw["compare"] = "none" if kw["algo"] in ("tko", "tku") else "exact"
        if kw["impl"] == "air":
            kw["code_version"] = base.CODE[kw["algo"]]
            kw["runtime_version"] = base.RUNTIME
        else:
            sname, args = base.SPMF[kw["algo"]]
            kw.update(spmf_name=sname, spmf_args=args(d["min_util"], base.K), code_version="spmf")
        if kw["algo"] == "tko":
            kw["k"] = base.K
        jobs.append(kw)

    main_sets = ["lgsr", "hm", "lgsr-custweek", "chainstore-x10"]
    for di, ds in enumerate(main_sets + ["chainstore-x40"]):
        for algo in ("fhm", "efim-closed"):
            J(experiment="ref", impl="air", algo=algo, dataset=ds, budget_mb=20480, cap_s=3600,
              priority=di, is_reference=True)
    # Most informative first: the largest and the smallest budget, then the middle ones.
    for bi, b in enumerate([20480, 1024, 4096, 16384]):
        for di, ds in enumerate(main_sets):
            for algo in ALGOS:
                for impl in ("air", "spmf"):
                    J(experiment="big-cmp", impl=impl, algo=algo, dataset=ds, budget_mb=b, cap_s=CAP,
                      priority=100 + bi * 10 + di)
    for ds in ("chainstore-x40",):
        for b in (2048, 20480):
            for algo in ("fhm", "efim"):
                for impl in ("air", "spmf"):
                    J(experiment="big-cmp", impl=impl, algo=algo, dataset=ds, budget_mb=b, cap_s=CAP, priority=150)
    for t in (1, 4, 8, 16):
        for algo in ("fhm", "efim"):
            J(experiment="big-threads", impl="air", algo=algo, dataset="lgsr", budget_mb=20480, threads=t,
              cap_s=CAP, priority=200)
    for b in (1024, 4096):
        for algo in ("fhm", "efim"):
            for impl in ("air", "spmf"):
                J(experiment="big-cgroup", impl=impl, algo=algo, dataset="lgsr", budget_mb=b, limit="cgroup",
                  cap_s=CAP, priority=300)
    for b in (1024, 20480):
        for algo in ("fhm", "efim"):
            for impl in ("air", "spmf"):
                J(experiment="big-size", impl=impl, algo=algo, dataset="lgsr-1yr", budget_mb=b, cap_s=CAP,
                  priority=400)

    plan = {"config": {"binaries": {"air": air}, "java": "java", "spmf_jar": jar, "datasets": datasets},
            "pending_algorithms": [], "jobs": jobs}
    json.dump(plan, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
