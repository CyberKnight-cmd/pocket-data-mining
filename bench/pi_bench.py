#!/usr/bin/env python3
"""Air-HUIM vs SPMF benchmark (built for the Raspberry Pi experiments).

For every (dataset, memory budget, algorithm) it runs Air-HUIM and, where SPMF has the
same algorithm, SPMF on Java with the heap capped at the same budget (-Xmx). Records:
  * peak RSS of the process (exact, from wait4 rusage — includes the JVM for SPMF),
  * wall time, and whether it finished within the time cap or failed (e.g. OOM),
  * exactness: the HUI set is compared with a reference (FHM's output at the largest budget).

usage: pi_bench.py --air BIN --spmf JAR --java JAVA --out results.csv \
           [--threads 4] [--cap 120] [--budgets 64,256,1024] [--only air|spmf]
Datasets are listed in DATASETS below (path, min_utility, top-k).
"""
import argparse, csv, os, shutil, subprocess, sys, time

DATASETS = [
    # name, path, min_utility, k (top-k algorithms)
    ("foodmart", os.path.expanduser("~/air-huim/foodmart.txt"), 3000, 500),
    ("chainstore", os.path.expanduser("~/air-huim/chainstore.txt"), 1000000, 100),
]

# Air-HUIM slug -> (SPMF name, extra SPMF args builder) for algorithms SPMF also implements
# with the same semantics. SPMF's GA/BPSO/ACO are approximate and its HAUI-Miner mines
# average utility, so those are not compared head-to-head.
SPMF = {
    "two-phase": ("Two-Phase", lambda mu, k: [mu]),
    "ihup": ("IHUP", lambda mu, k: [mu]),
    "up-growth": ("UPGrowth", lambda mu, k: [mu]),
    "up-growth-plus": ("UPGrowth+", lambda mu, k: [mu]),
    "hui-miner": ("HUI-Miner", lambda mu, k: [mu]),
    "fhm": ("FHM", lambda mu, k: [mu]),
    "fhm-plus": ("FHM+", lambda mu, k: [mu, 1, 1000]),
    "hup-miner": ("HUP-Miner", lambda mu, k: [mu, 2]),
    "mhuiminer": ("mHUIMiner", lambda mu, k: [mu]),
    "efim": ("EFIM", lambda mu, k: [mu]),
    "efim-closed": ("EFIM-Closed", lambda mu, k: [mu]),
    "tko": ("TKO", lambda mu, k: [k]),
    "tku": ("TKU", lambda mu, k: [k]),
}

AIR = ["two-phase", "ihup", "hup-tree", "up-growth", "up-growth-plus", "hui-trie",
       "fhm", "fhm-plus", "hui-miner", "hup-miner", "mhuiminer",
       "efim", "efim-closed", "haui-miner", "tko", "tku", "rept",
       "huim-mmu", "shuim", "incfhm", "huim-ga", "huim-bpso", "mhui-aco"]
TOPK = {"tko", "tku", "rept"}


def normalize(path):
    rows = set()
    try:
        with open(path) as f:
            for line in f:
                if "#UTIL:" not in line:
                    continue
                a, b = line.split("#UTIL:")
                rows.add((tuple(sorted(int(x) for x in a.split())), int(b.split()[0])))
    except FileNotFoundError:
        return None
    return rows


def run(cmd, cap, cwd, env=None):
    """Run cmd; return (status, seconds, peak_rss_mb, tail_of_output)."""
    # Peak RSS is read from /proc/<pid>/status VmHWM, which belongs to the address space
    # created at exec. (ru_maxrss from wait4 is NOT usable: Linux carries the pre-exec
    # high-water mark — i.e. this Python process's — into the child's maxrss.)
    t0 = time.time()
    logpath = os.path.join(cwd, "run.log")
    fa = [(os.POSIX_SPAWN_OPEN, 0, "/dev/null", os.O_RDONLY, 0),
          (os.POSIX_SPAWN_OPEN, 1, logpath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644),
          (os.POSIX_SPAWN_DUP2, 1, 2)]
    prev = os.getcwd()
    os.chdir(cwd)
    try:
        pid0 = os.posix_spawnp(cmd[0], cmd, env or os.environ, file_actions=fa, setsid=True)
    finally:
        os.chdir(prev)
    status = None
    hwm_kb = 0
    while True:
        try:
            with open(f"/proc/{pid0}/status") as f:
                for line in f:
                    if line.startswith("VmHWM:"):
                        hwm_kb = max(hwm_kb, int(line.split()[1]))
        except (FileNotFoundError, ProcessLookupError, ValueError):
            pass
        pid, st, ru = os.wait4(pid0, os.WNOHANG)
        if pid:
            status = "ok" if os.waitstatus_to_exitcode(st) == 0 else f"exit{os.waitstatus_to_exitcode(st)}"
            break
        if time.time() - t0 > cap:
            os.killpg(pid0, 9)
            pid, st, ru = os.wait4(pid0, 0)
            status = "cap"
            break
        time.sleep(0.02)
    dt = time.time() - t0
    tail = open(os.path.join(cwd, "run.log"), "rb").read()[-4000:].decode("utf8", "replace")
    if status != "ok" and ("OutOfMemoryError" in tail or "Java heap space" in tail):
        status = "OOM"
    return status, dt, hwm_kb / 1024.0, tail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--air", required=True)
    ap.add_argument("--spmf", required=True)
    ap.add_argument("--java", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--cap", type=float, default=120)
    ap.add_argument("--budgets", default="64,256,1024")
    ap.add_argument("--only", choices=["air", "spmf"])
    ap.add_argument("--datasets", default=",".join(d[0] for d in DATASETS))
    args = ap.parse_args()
    budgets = [int(b) for b in args.budgets.split(",")]
    work = os.path.expanduser("~/bench/work")
    refs = {}

    new = not os.path.exists(args.out)
    out = open(args.out, "a", newline="")
    w = csv.writer(out)
    if new:
        w.writerow(["dataset", "budget_mb", "impl", "algorithm", "status", "seconds", "peak_rss_mb",
                    "within_budget", "huis", "exact", "note"])

    for name, path, mu, k in DATASETS:
        if name not in args.datasets.split(","):
            continue
        # Reference HUI set: Air-HUIM FHM at the largest budget, uncapped. (FHM is verified
        # against brute force in tests/budget_integration.rs and against SPMF's output.)
        d = os.path.join(work, f"{name}_reference")
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
        ref_out = os.path.join(d, "out.txt")
        st, dt, _, _ = run([args.air, "mine", "-d", path, "-a", "fhm", "-b", str(max(budgets)), "-m", str(mu),
                            "-o", ref_out, "-c", os.path.join(d, "chunks"), "--threads", str(args.threads)], 10 ** 9, d)
        refs[name] = normalize(ref_out)
        print(f"reference for {name}: {len(refs[name])} HUIs ({dt:.1f}s, {st})", flush=True)
        shutil.rmtree(d, ignore_errors=True)

        for budget in sorted(budgets, reverse=True):
            for impl in ("air", "spmf"):
                if args.only and impl != args.only:
                    continue
                algos = AIR if impl == "air" else list(SPMF)
                for algo in algos:
                    d = os.path.join(work, f"{name}_{budget}_{impl}_{algo}")
                    shutil.rmtree(d, ignore_errors=True)
                    os.makedirs(d)
                    outp = os.path.join(d, "out.txt")
                    if impl == "air":
                        cmd = [args.air, "mine", "-d", path, "-a", algo, "-b", str(budget),
                               "-o", outp, "-c", os.path.join(d, "chunks"), "--threads", str(args.threads)]
                        cmd += ["--top-k", str(k)] if algo in TOPK else ["-m", str(mu)]
                    else:
                        sname, extra = SPMF[algo]
                        cmd = [args.java, f"-Xmx{budget}m", "-jar", args.spmf, "run", sname, path, outp] \
                              + [str(x) for x in extra(mu, k)]
                    status, dt, rss, tail = run(cmd, args.cap, d)
                    rows = normalize(outp) if status == "ok" else None
                    huis = len(rows) if rows is not None else ""
                    exact = ""
                    if rows is not None and name in refs:
                        if algo in TOPK:
                            exact = "n/a(top-k)"
                        elif algo == "efim-closed":
                            exact = "closed"
                        elif algo == "huim-mmu":
                            exact = "n/a(windows)"
                        else:
                            exact = "yes" if rows == refs[name] else "NO"
                    note = ""
                    if impl == "air":
                        for line in tail.splitlines():
                            if line.startswith("Memory:") or line.startswith("Error:"):
                                note = line.strip()
                    elif status not in ("ok", "cap"):
                        note = [l for l in tail.splitlines() if l.strip()][-1][:160] if tail.strip() else ""
                    w.writerow([name, budget, impl, algo, status, f"{dt:.1f}", f"{rss:.1f}",
                                "yes" if rss <= budget else "NO", huis, exact, note])
                    out.flush()
                    print(f"{name:10} {budget:5} {impl:4} {algo:15} {status:6} {dt:7.1f}s {rss:8.1f}MB huis={huis} exact={exact}", flush=True)
                    shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()
