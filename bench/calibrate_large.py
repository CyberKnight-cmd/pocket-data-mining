#!/usr/bin/env python3
"""Pick thresholds for the large datasets (LGSR, H&M, scaled chainstore).

usage: calibrate_large.py BINARY OUT.json NAME=PATH[:TOTAL_UTILITY] ...

For each dataset, runs Air-HUIM FHM with a generous budget (20 GB, 16 threads, so nothing
spills) at decreasing fractions of the total utility, recording time and HUI count, and stops
once a run takes longer than 300 s or finds more than 100,000 HUIs. The chosen threshold is the
lowest one that finished within 120 s with at least 100 HUIs: big enough to be a real workload,
small enough that the slower algorithms and SPMF can finish within the per-run cap.
TOTAL_UTILITY defaults to a scan of the file.
"""
import json, os, subprocess, sys, tempfile, time

PCTS = [2, 1, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.005]


def total_utility(path):
    t = 0
    with open(path) as f:
        for line in f:
            t += int(line.split(":")[1])
    return t


def run(binary, path, mu):
    with tempfile.TemporaryDirectory(dir=os.path.expanduser("~/.cache/air-huim-bench")) as d:
        out = os.path.join(d, "o.txt")
        t0 = time.time()
        p = subprocess.run([binary, "mine", "-d", path, "-a", "fhm", "-m", str(mu), "-b", "20480",
                            "--threads", "16", "-o", out, "-c", os.path.join(d, "c")],
                           stdin=subprocess.DEVNULL, capture_output=True, timeout=900)
        secs = time.time() - t0
        n = sum(1 for _ in open(out)) if p.returncode == 0 and os.path.exists(out) else -1
        return round(secs, 2), n


def main():
    binary, out_path = sys.argv[1], sys.argv[2]
    res = json.load(open(out_path)) if os.path.exists(out_path) else {}
    for spec in sys.argv[3:]:
        name, rest = spec.split("=", 1)
        path, _, tu = rest.partition(":")
        if name in res and res[name].get("chosen"):
            continue
        tu = int(tu) if tu else total_utility(path)
        trials = []
        for pct in PCTS:
            mu = int(tu * pct / 100)
            try:
                secs, n = run(binary, path, mu)
            except subprocess.TimeoutExpired:
                secs, n = 900, -1
            trials.append({"pct": pct, "min_util": mu, "secs": secs, "huis": n})
            print(name, trials[-1], flush=True)
            if secs > 300 or n > 100_000 or n < 0:
                break
        ok = [t for t in trials if 0 <= t["secs"] <= 120 and t["huis"] >= 100]
        chosen = ok[-1] if ok else None
        res[name] = {"path": path, "total_utility": tu, "trials": trials,
                     "chosen": chosen["min_util"] if chosen else None,
                     "chosen_pct": chosen["pct"] if chosen else None}
        json.dump(res, open(out_path, "w"), indent=1)


if __name__ == "__main__":
    main()
