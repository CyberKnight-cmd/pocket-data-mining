#!/usr/bin/env python3
"""Resumable, data-rich experiment runner (designed for the Raspberry Pi).

Every job is identified by a hash of everything that determines its result
(implementation + binary hash, algorithm, dataset hash, threshold, budget, threads,
mode). A job whose result already exists is never re-run, so the queue can be stopped,
extended and restarted without redundant work.

Per job, under DATA/runs/<job_id>/:
  job.json      the job spec
  env.json      system state at start/end (temps, clocks, throttling, load, free mem, versions)
  stdout.log    full program output (Air-HUIM: incl. memlog lines every 250 ms)
  ts.csv        time series sampled every ~100 ms: rss/hwm/vsz, cpu user/sys, page faults,
                I/O bytes, threads, context switches, spill-dir bytes, SoC temp, ARM clock
  output.norm.gz  normalized HUI set ("sorted items<TAB>utility" per line, sorted)
  result.json   summary: status, wall/cpu time, peak RSS, I/O, outputs count+sha256,
                exactness/precision/recall vs reference, ledger peak, watchdog kills
DATA/results.jsonl gets one line per finished job (append-only).

usage: collect.py run PLAN.json [--data DIR] [--max-hours H]
       collect.py status PLAN.json [--data DIR]
"""
import argparse, gzip, hashlib, json, os, shutil, signal, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")


def sha256_file(path, cache={}):
    key = (path, os.path.getmtime(path), os.path.getsize(path))
    if key not in cache:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for b in iter(lambda: f.read(1 << 20), b""):
                h.update(b)
        cache[key] = h.hexdigest()
    return cache[key]


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return ""


def soc():
    """SoC temperature (C) and ARM clock (MHz), cheap sysfs reads."""
    t = f = None
    try:
        t = int(open("/sys/class/thermal/thermal_zone0/temp").read()) / 1000
    except Exception:
        pass
    try:
        f = int(open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq").read()) / 1000
    except Exception:
        pass
    return t, f


def env_snapshot():
    t, f = soc()
    return {
        "time": time.time(), "soc_temp_c": t, "arm_mhz": f,
        "throttled": sh("vcgencmd get_throttled"), "loadavg": open("/proc/loadavg").read().split()[:3],
        "meminfo": {l.split(":")[0]: l.split(":")[1].strip() for l in open("/proc/meminfo") if l.split(":")[0] in
                    ("MemTotal", "MemFree", "MemAvailable", "Cached", "SwapTotal", "SwapFree", "Dirty")},
    }


def static_env():
    return {
        "uname": sh("uname -a"), "model": sh("cat /proc/device-tree/model 2>/dev/null | tr -d '\\0'"),
        "os": sh(". /etc/os-release; echo $PRETTY_NAME"), "cpus": os.cpu_count(),
        "storage": sh("df -h ~ | tail -1"), "tmpfs_tmp": sh("findmnt -n -o FSTYPE /tmp"),
        "java": sh(os.path.expanduser("~/bench/java/bin/java -version 2>&1 | head -1")),
        "python": sys.version.split()[0],
    }


def proc_sample(pid):
    s = {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                k, _, v = line.partition(":")
                if k in ("VmRSS", "VmHWM", "VmSize", "Threads", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"):
                    s[k] = int(v.split()[0])
        st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        s["minflt"], s["majflt"] = int(st[7]), int(st[9])
        s["utime"], s["stime"] = int(st[11]) / CLK_TCK, int(st[12]) / CLK_TCK
        try:
            for line in open(f"/proc/{pid}/io"):
                k, _, v = line.partition(":")
                if k in ("read_bytes", "write_bytes", "rchar", "wchar"):
                    s[k] = int(v)
        except PermissionError:
            pass
    except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
        return None
    return s


def dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except FileNotFoundError:
                pass
    return total


def normalize_output(path, dst_gz, average=False):
    """Write sorted normalized itemsets to dst_gz; return (raw count, distinct count, sha256).
    Lines are "sorted items<TAB>utility". For average-utility mining the value is the
    average utility with 2 decimals: SPMF writes it as #AUTIL, ours writes the total
    utility as #UTIL (converted here)."""
    rows = []
    try:
        with open(path) as f:
            for line in f:
                if "#AUTIL:" in line:
                    a, b = line.split("#AUTIL:")
                    items = sorted(int(x) for x in a.split())
                    rows.append(f"{' '.join(map(str, items))}\t{float(b.split()[0]):.2f}")
                    continue
                if "#UTIL:" not in line:
                    continue
                a, b = line.split("#UTIL:")
                items = sorted(int(x) for x in a.split())
                u = int(b.split()[0])
                val = f"{u / len(items):.2f}" if average else str(u)
                rows.append(f"{' '.join(map(str, items))}\t{val}")
    except FileNotFoundError:
        return None, None, None
    raw = len(rows)
    rows = sorted(set(rows))
    data = ("\n".join(rows) + "\n").encode() if rows else b""
    with gzip.open(dst_gz, "wb", compresslevel=6) as g:
        g.write(data)
    return raw, len(rows), hashlib.sha256(data).hexdigest()


def load_norm(gz):
    with gzip.open(gz, "rt") as g:
        return set(line.rstrip("\n") for line in g if line.strip())


def job_id(job):
    """Identity of a result. Uses the per-algorithm `code_version` (bumped when that
    algorithm's code changes) rather than the binary hash, so rebuilding the binary to add
    new algorithms does not invalidate finished runs of unchanged ones. The exact binary
    hash is still recorded in each result for traceability."""
    # Only what determines the run's behaviour; evaluation metadata (which reference to compare
    # against, how) is excluded — exactness can be recomputed later from stored outputs.
    key = {k: v for k, v in job.items()
           if k not in ("priority", "cap_s", "experiment", "ref_key", "compare", "is_reference", "average_output")}
    return hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()[:16]


def build_cmd(job, d, cfg):
    out = os.path.join(d, "out.txt")
    ds = cfg["datasets"][job["dataset"]]["path"]
    impl = job["impl"]
    if impl in ("air", "air-baseline"):
        binp = cfg["binaries"][impl]
        cmd = [binp, "mine", "-d", ds, "-a", job["algo"], "-b", str(job["budget_mb"]),
               "-o", out, "-c", os.path.join(d, "spill"), "--threads", str(job["threads"])]
        cmd += ["--top-k", str(job["k"])] if job.get("k") else ["-m", str(job["min_util"])]
        cmd += [str(x) for x in job.get("extra", [])]
    elif impl == "spmf":
        heap = [f"-Xmx{job['budget_mb']}m"] if job.get("budget_mb") else []
        cmd = [cfg["java"]] + heap + job.get("jvm_args", []) + ["-jar", cfg["spmf_jar"], "run", job["spmf_name"], ds, out] \
              + [str(x) for x in job["spmf_args"]]
    else:
        raise ValueError(impl)
    # Kernel-enforced hard limit: run inside a transient cgroup scope with MemoryMax = budget
    # and no swap. Exceeding it gets the process OOM-killed by the kernel (needs the memory
    # controller: cgroup_enable=memory on the Pi kernel command line).
    if job.get("limit") == "cgroup" and job.get("budget_mb"):
        cmd = ["systemd-run", "--user", "--scope", "--quiet", "-p", f"MemoryMax={job['budget_mb']}M",
               "-p", "MemorySwapMax=0"] + cmd
    return cmd, out


def run_job(job, cfg, data_dir, refs):
    jid = job["id"]
    d = os.path.join(data_dir, "runs", jid)
    if os.path.exists(os.path.join(d, "result.json")):
        return None
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    json.dump(job, open(os.path.join(d, "job.json"), "w"), indent=1)
    env0 = env_snapshot()
    cmd, outp = build_cmd(job, d, cfg)
    env = dict(os.environ, AIR_HUIM_MEMLOG="1", AIR_HUIM_MEMLOG_MS="250")
    log = open(os.path.join(d, "stdout.log"), "wb")
    ts = open(os.path.join(d, "ts.csv"), "w")
    cols = ["t", "VmRSS", "VmHWM", "VmSize", "utime", "stime", "minflt", "majflt", "read_bytes", "write_bytes",
            "rchar", "wchar", "Threads", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches",
            "spill_bytes", "soc_temp_c", "arm_mhz"]
    ts.write(",".join(cols) + "\n")
    t0 = time.time()
    p = subprocess.Popen(cmd, cwd=d, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, env=env,
                         start_new_session=True)
    budget_kb = job["budget_mb"] * 1024 if job.get("budget_mb") else None
    watchdog = job.get("watchdog", False)
    hwm = 0
    peak_spill = 0
    last_slow = 0
    spill_b = 0
    temp = mhz = None
    last = None
    status = None
    killed_reason = None
    rusage = None
    while True:
        now = time.time()
        s = proc_sample(p.pid)
        if now - last_slow >= 1.0:
            spill_b = dir_bytes(d) if job["impl"] != "spmf" else 0
            peak_spill = max(peak_spill, spill_b)
            temp, mhz = soc()
            last_slow = now
        if s:
            last = s
            hwm = max(hwm, s.get("VmHWM", 0), s.get("VmRSS", 0))
            ts.write(",".join(str(x) for x in [round(now - t0, 3)] + [s.get(c, "") for c in cols[1:15]]
                              + [spill_b, temp, mhz]) + "\n")
            if watchdog and budget_kb and s.get("VmRSS", 0) > budget_kb:
                killed_reason = f"watchdog: RSS {s['VmRSS'] // 1024} MB > budget {job['budget_mb']} MB"
                os.killpg(p.pid, signal.SIGKILL)
        pid, st, ru = os.wait4(p.pid, os.WNOHANG)
        if pid:
            rusage = ru
            code = os.waitstatus_to_exitcode(st)
            status = "ok" if code == 0 else (f"signal{-code}" if code < 0 else f"exit{code}")
            break
        if now - t0 > job["cap_s"]:
            os.killpg(p.pid, signal.SIGKILL)
            _, st, rusage = os.wait4(p.pid, 0)
            status = "cap"
            break
        time.sleep(0.1)
    wall = time.time() - t0
    log.close()
    ts.close()
    if killed_reason:
        status = "budget-kill"
    elif job.get("limit") == "cgroup" and status in ("signal9", "exit137"):
        status = "kernel-oom"  # killed by the cgroup memory limit
    tail = open(os.path.join(d, "stdout.log"), "rb").read()[-20000:].decode("utf8", "replace")
    if status not in ("ok", "cap", "budget-kill") and ("OutOfMemoryError" in tail or "Java heap space" in tail
                                                      or "GC overhead" in tail):
        status = "OOM"

    bin_path = cfg["binaries"].get(job["impl"]) or cfg["spmf_jar"]
    res = {"id": jid, "job": job, "binary_sha256": sha256_file(bin_path), "status": status, "wall_s": round(wall, 3),
           "cpu_user_s": round(rusage.ru_utime, 3) if rusage else None,
           "cpu_sys_s": round(rusage.ru_stime, 3) if rusage else None,
           "peak_rss_mb": round(hwm / 1024, 2), "within_budget": (hwm <= budget_kb) if budget_kb else None,
           "peak_spill_mb": round(peak_spill / 1048576, 2), "killed_reason": killed_reason,
           "last_sample": last}
    if status == "ok":
        raw, n, digest = normalize_output(outp, os.path.join(d, "output.norm.gz"),
                                          average=job.get("average_output", False))
        res.update({"huis_raw": raw, "huis": n, "output_sha256": digest})
        ref = refs.get(job.get("ref_key"))
        if ref and n is not None and job.get("compare") in ("subset", "exact"):
            res["exact"] = digest == ref["sha"]
            if not res["exact"]:
                got = load_norm(os.path.join(d, "output.norm.gz"))
                exp = load_norm(ref["path"])
                tp = len(got & exp)
                res["precision"] = round(tp / len(got), 6) if got else 1.0
                res["recall"] = round(tp / len(exp), 6) if exp else 1.0
                res["missing_examples"] = sorted(exp - got)[:20]
                res["extra_examples"] = sorted(got - exp)[:20]
    for line in tail.splitlines():
        if line.startswith("Memory:"):
            res["air_memory_line"] = line.strip()
        if line.startswith("Error:"):
            res["error"] = line.strip()
    if status not in ("ok", "cap"):
        res["tail"] = tail[-1500:]
    try:
        os.remove(outp)
    except FileNotFoundError:
        pass
    shutil.rmtree(os.path.join(d, "spill"), ignore_errors=True)
    json.dump({"start": env0, "end": env_snapshot()}, open(os.path.join(d, "env.json"), "w"), indent=1)
    json.dump(res, open(os.path.join(d, "result.json"), "w"), indent=1)
    with open(os.path.join(data_dir, "results.jsonl"), "a") as f:
        f.write(json.dumps({k: v for k, v in res.items() if k not in ("tail", "last_sample")}) + "\n")
    # A finished reference job defines the expected output for its ref_key.
    if job.get("is_reference") and status == "ok":
        refs[job["ref_key"]] = {"sha": res["output_sha256"], "path": os.path.join(d, "output.norm.gz"), "id": jid}
        json.dump(refs, open(os.path.join(data_dir, "references.json"), "w"), indent=1)
    return res


def load_plan(plan_path):
    plan = json.load(open(plan_path))
    cfg = plan["config"]
    for k, v in list(cfg["binaries"].items()):
        cfg["binaries"][k] = os.path.expanduser(v)
    cfg["java"] = os.path.expanduser(cfg["java"])
    cfg["spmf_jar"] = os.path.expanduser(cfg["spmf_jar"])
    for ds in cfg["datasets"].values():
        ds["path"] = os.path.expanduser(ds["path"])
    shas = {k: sha256_file(v) for k, v in cfg["binaries"].items() if os.path.exists(v)}
    shas["spmf"] = sha256_file(cfg["spmf_jar"])
    jobs = []
    seen = set()
    for job in plan["jobs"]:
        job = dict(job)
        job["dataset_sha"] = cfg["datasets"][job["dataset"]].get("sha256")
        job["id"] = job_id(job)
        if job["id"] in seen:  # same run requested by two experiments: run once
            continue
        seen.add(job["id"])
        jobs.append(job)
    jobs.sort(key=lambda j: j.get("priority", 100))
    return cfg, jobs, shas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "status"])
    ap.add_argument("plan")
    ap.add_argument("--data", default=os.path.expanduser("~/bench/data"))
    ap.add_argument("--max-hours", type=float, default=1e9)
    args = ap.parse_args()
    cfg, jobs, shas = load_plan(args.plan)
    os.makedirs(os.path.join(args.data, "runs"), exist_ok=True)
    meta = os.path.join(args.data, "static_env.json")
    if not os.path.exists(meta):
        json.dump({"static": static_env(), "binaries": shas, "config": cfg}, open(meta, "w"), indent=1)
    refs_path = os.path.join(args.data, "references.json")
    refs = json.load(open(refs_path)) if os.path.exists(refs_path) else {}
    done = [j for j in jobs if os.path.exists(os.path.join(args.data, "runs", j["id"], "result.json"))]
    if args.cmd == "status":
        print(f"{len(done)}/{len(jobs)} jobs done")
        return
    t_end = time.time() + args.max_hours * 3600
    for i, job in enumerate(jobs):
        if time.time() > t_end:
            break
        res = run_job(job, cfg, args.data, refs)
        if res:
            print(f"[{i + 1}/{len(jobs)}] {job['experiment']:10} {job['dataset']:14} {job['impl']:12} {job['algo']:15} "
                  f"b={job.get('budget_mb')} t={job['threads']} -> {res['status']:11} {res['wall_s']:8.1f}s "
                  f"{res['peak_rss_mb']:8.1f}MB huis={res.get('huis')} exact={res.get('exact')}", flush=True)


if __name__ == "__main__":
    main()
