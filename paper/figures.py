#!/usr/bin/env python3
"""Figures for the paper, from the Pi run records (bench/collect.py output).

usage: figures.py PI_DATA_DIR [OUT_DIR]   (default OUT_DIR: paper/fig)
"""
import csv, json, os, sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

DATA = sys.argv[1]
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "fig")
os.makedirs(OUT, exist_ok=True)

# Validated categorical palette (dataviz reference instance, light mode).
AIR, SPMF, BOTH, NEITHER = "#2a78d6", "#eb6834", "#1baf7a", "#b9b8b0"
ALGO_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
INK, MUTED, GRID = "#1f1f1e", "#6b6a64", "#e4e3dd"
COL, FULL = 3.45, 7.1  # IEEE column and page widths, inches

plt.rcParams.update({
    "font.family": "serif", "font.serif": ["Nimbus Roman", "Times New Roman", "DejaVu Serif"],
    "font.size": 7.5, "axes.titlesize": 7.5, "axes.labelsize": 7.5, "legend.fontsize": 7,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "lines.linewidth": 1.4, "pdf.fonttype": 42, "savefig.bbox": "tight", "savefig.pad_inches": 0.02,
})
NAMES = {"fhm": "FHM", "efim": "EFIM", "hui-miner": "HUI-Miner", "up-growth": "UP-Growth",
         "two-phase": "Two-Phase", "efim-closed": "EFIM-Closed", "up-growth-plus": "UP-Growth+"}


def load():
    runs = []
    root = os.path.join(DATA, "runs")
    for d in os.listdir(root):
        try:
            j = json.load(open(os.path.join(root, d, "job.json")))
            r = json.load(open(os.path.join(root, d, "result.json")))
        except (OSError, ValueError):
            continue
        if j["impl"] == "air" and j.get("runtime_version") != "r6":
            continue  # only the final code; SPMF runs are unchanged across runtimes
        if j["impl"] not in ("air", "spmf"):
            continue
        runs.append((d, j, r))
    return runs


RUNS = load()
OOM = {"OOM", "kernel-oom", "budget-kill"}


def save(fig, name):
    fig.savefig(os.path.join(OUT, name))
    plt.close(fig)
    print("wrote", name)


def trace(run_id):
    t, rss = [], []
    with open(os.path.join(DATA, "runs", run_id, "ts.csv")) as f:
        for row in csv.DictReader(f):
            if not row["VmRSS"]:  # sample taken as the process exited
                continue
            t.append(float(row["t"]) / 60)
            rss.append(int(row["VmRSS"]) / 1024)
    return np.array(t), np.array(rss)


# 1. Memory over time: LGSR, FHM, 1 GB, on the Pi.
def fig_trace():
    pick = {}
    for d, j, r in RUNS:
        if j["dataset"] == "lgsr" and j["algo"] == "fhm" and j.get("budget_mb") == 1024 and j["experiment"] == "pi-large":
            pick[j["impl"]] = (d, r)
    fig, ax = plt.subplots(figsize=(COL, 1.75))
    for impl, color, label in (("spmf", SPMF, "SPMF"), ("air", AIR, "Air-HUIM")):
        d, r = pick[impl]
        t, rss = trace(d)
        ax.plot(t, rss, color=color, lw=1.2)
        end = "out of memory" if r["status"] in OOM else "exact result"
        ax.plot(t[-1], rss[-1], marker="X" if r["status"] in OOM else "o", ms=6, color=color,
                mec="white", mew=0.8, zorder=5)
        ax.annotate(f"{label}: {end} at {t[-1]:.0f} min", (t[-1], rss[-1]),
                    xytext=(-4, -9) if impl == "air" else (8, 6), textcoords="offset points",
                    fontsize=6.8, color=INK, va="top" if impl == "air" else "bottom",
                    ha="right" if impl == "air" else "left")
    ax.axhline(1024, color=INK, lw=0.8, ls=(0, (4, 3)))
    ax.text(47.5, 1024 - 30, "budget 1 GB", fontsize=6.8, color=INK, va="top", ha="right")
    ax.set_xlabel("time (min)")
    ax.set_ylabel("resident memory (MB)")
    ax.set_ylim(0, 1350)
    ax.set_xlim(0, 48)
    save(fig, "trace_lgsr.pdf")


# 2. Peak memory relative to the budget, every comparison run.
def fig_peak():
    fig, ax = plt.subplots(figsize=(COL, 1.9))
    rng = np.random.default_rng(1)
    budgets = [64, 256, 1024]
    for bi, b in enumerate(budgets):
        for k, (impl, color) in enumerate((("air", AIR), ("spmf", SPMF))):
            vals, ooms = [], []
            for d, j, r in RUNS:
                if j["experiment"] != "cmp" or j["impl"] != impl or j.get("budget_mb") != b or not r.get("peak_rss_mb"):
                    continue
                (ooms if r["status"] in OOM else vals).append(r["peak_rss_mb"] / b)
            x0 = bi * 3 + k
            ax.scatter(x0 + rng.uniform(-0.32, 0.32, len(vals)), vals, s=5, color=color, alpha=0.55, lw=0)
            ax.scatter(x0 + rng.uniform(-0.32, 0.32, len(ooms)), ooms, s=11, marker="x", color=color, lw=0.8)
    ax.axhline(1.0, color=INK, lw=0.8, ls=(0, (4, 3)))
    ax.text(8.45, 1.03, "budget", fontsize=6.8, color=INK, ha="right", va="bottom")
    ax.set_xticks([0.5, 3.5, 6.5])
    ax.set_xticklabels(["64 MB", "256 MB", "1 GB"])
    ax.set_xlim(-0.6, 7.6)
    ax.set_ylabel("peak RSS / budget")
    ax.set_yscale("log")
    ax.set_yticks([0.01, 0.1, 0.5, 1, 2, 5])
    ax.set_yticklabels(["0.01", "0.1", "0.5", "1", "2", "5"])
    ax.grid(axis="x", visible=False)
    h = [plt.Line2D([], [], ls="", marker="o", ms=4, color=AIR, label="Air-HUIM"),
         plt.Line2D([], [], ls="", marker="o", ms=4, color=SPMF, label="SPMF"),
         plt.Line2D([], [], ls="", marker="x", ms=4, color=MUTED, label="out of memory")]
    ax.legend(handles=h, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=3, frameon=False, handletextpad=0.3)
    save(fig, "peak_ratio.pdf")


# 3. Outcome of each Air-HUIM/SPMF pair per budget.
def fig_outcomes():
    st = {}
    for d, j, r in RUNS:
        if j["experiment"] == "cmp":
            st[(j["impl"], j["algo"], j["dataset"], j["budget_mb"])] = r["status"]
    rows = []
    for b in (64, 256, 1024):
        c = defaultdict(int)
        for (i, a, ds, bb), s in st.items():
            if i != "spmf" or bb != b or ("air", a, ds, b) not in st:
                continue
            ao, so = st[("air", a, ds, b)] == "ok", s == "ok"
            c["both" if ao and so else "air" if ao else "spmf" if so else "neither"] += 1
        rows.append((b, c))
    fig, ax = plt.subplots(figsize=(COL, 1.25))
    cats = [("both", BOTH, "both finished"), ("air", AIR, "only Air-HUIM"), ("spmf", SPMF, "only SPMF"),
            ("neither", NEITHER, "neither")]
    for y, (b, c) in enumerate(rows):
        left = 0
        for key, color, _ in cats:
            w = c[key]
            if w:
                ax.barh(y, w - 0.8, left=left + 0.4, color=color, height=0.62)
                if w >= 8:
                    ax.text(left + w / 2, y, str(w), ha="center", va="center", fontsize=6.8,
                            color="white" if key in ("air",) else INK)
            left += w
    ax.set_yticks(range(3))
    ax.set_yticklabels(["64 MB", "256 MB", "1 GB"])
    ax.invert_yaxis()
    ax.set_xlim(0, 196)
    ax.set_xlabel("pairs (same algorithm, dataset and budget)")
    ax.grid(axis="y", visible=False)
    h = [plt.Rectangle((0, 0), 1, 1, color=c, label=l) for _, c, l in cats]
    ax.legend(handles=h, loc="lower center", bbox_to_anchor=(0.45, 1.0), ncol=4, frameon=False,
              handlelength=1, handletextpad=0.4, columnspacing=0.8)
    save(fig, "outcomes.pdf")


# 4. Run time against budget (graceful degradation), small multiples per dataset.
def fig_curve():
    sets = ["chainstore", "kosarak", "accidents", "retail", "chicago"]
    algos = ["fhm", "hui-miner", "efim", "up-growth", "two-phase"]
    pts = defaultdict(dict)
    for d, j, r in RUNS:
        if j["impl"] == "air" and j["experiment"] in ("curve", "cmp") and j["dataset"] in sets \
                and j["algo"] in algos and j.get("threads") == 4 and j.get("budget_mb"):
            pts[(j["dataset"], j["algo"])][j["budget_mb"]] = (r["wall_s"], r["status"])
    fig, axes = plt.subplots(1, 5, figsize=(FULL, 1.6), sharey=True)
    for ax, ds in zip(axes, sets):
        for a, color in zip(algos, ALGO_COLORS):
            p = sorted(pts[(ds, a)].items())
            if not p:
                continue
            ok = [(b, w) for b, (w, s) in p if s == "ok"]
            ax.plot([b for b, _ in ok], [w for _, w in ok], color=color, lw=1.2, marker="o", ms=2.5, label=NAMES[a])
            cap = [(b, w) for b, (w, s) in p if s != "ok"]
            if cap:
                ax.scatter([b for b, _ in cap], [2500] * len(cap), marker="^", s=12, color=color, zorder=5, lw=0)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xticks([32, 128, 512, 2048])
        ax.set_xticklabels(["32", "128", "512", "2048"])
        ax.set_title(ds, color=INK)
        ax.set_xlabel("budget (MB)")
        ax.set_ylim(1, 4000)
    axes[0].set_ylabel("wall time (s)")
    h, l = axes[0].get_legend_handles_labels()
    h.append(plt.Line2D([], [], ls="", marker="^", ms=4, color=MUTED))
    l.append("not finished in the cap")
    fig.legend(h, l, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=6, frameon=False)
    fig.subplots_adjust(wspace=0.12)
    save(fig, "budget_curve.pdf")


# 5. LGSR on the Pi: Air-HUIM completion vs SPMF time to out-of-memory.
def fig_lgsr():
    res = {}
    for d, j, r in RUNS:
        if j["dataset"] == "lgsr" and j.get("budget_mb") in (1024, 2048) and j["experiment"] in ("pi-large", "ref"):
            res[(j["impl"], j["algo"], j["budget_mb"])] = r
    order = [(a, b) for b in (2048, 1024) for a in ("fhm", "efim", "efim-closed", "up-growth-plus")]
    fig, ax = plt.subplots(figsize=(COL - 0.75, 2.3))
    for y, (a, b) in enumerate(order):
        ra, rs = res.get(("air", a, b)), res.get(("spmf", a, b))
        if ra:
            ax.barh(y + 0.19, ra["wall_s"] / 60, height=0.34, color=AIR)
            if ra["status"] != "ok":
                ax.text(ra["wall_s"] / 60 + 3, y + 0.19, "4 h cap", va="center", fontsize=6.5, color=INK)
        if rs:
            ax.barh(y - 0.19, rs["wall_s"] / 60, height=0.34, color=SPMF)
    ax.set_yticks(range(len(order)))
    ax.set_yticklabels([f"{NAMES[a]}, {b // 1024} GB" for a, b in order])
    ax.invert_yaxis()
    ax.set_xlabel("wall time on the Raspberry Pi (min)")
    ax.set_xlim(0, 285)
    ax.grid(axis="y", visible=False)
    h = [plt.Rectangle((0, 0), 1, 1, color=AIR, label="Air-HUIM: finished, exact"),
         plt.Rectangle((0, 0), 1, 1, color=SPMF, label="SPMF: out of memory")]
    ax.legend(handles=h, loc="lower center", bbox_to_anchor=(0.35, 1.0), ncol=2, frameon=False)
    save(fig, "lgsr.pdf")


# 6. Thread scaling on the Pi (1, 2, 4 threads, 1 GB).
def fig_threads():
    t = defaultdict(dict)
    for d, j, r in RUNS:
        if j["impl"] == "air" and j.get("budget_mb") == 1024 and r["status"] == "ok" and \
                j["dataset"] in ("accidents", "chainstore", "kosarak") and j["algo"] in ("efim", "fhm", "hui-miner"):
            if j["experiment"] == "threads" or (j["experiment"] == "cmp" and j["threads"] == 4):
                t[(j["dataset"], j["algo"])][j["threads"]] = r["wall_s"]
    fig, axes = plt.subplots(1, 3, figsize=(COL, 1.45), sharey=True)
    for ax, ds in zip(axes, ("accidents", "chainstore", "kosarak")):
        ax.plot([1, 4], [1, 4], color=MUTED, lw=0.8, ls=(0, (3, 2)), label="ideal")
        for a, color in zip(("efim", "fhm", "hui-miner"), ALGO_COLORS):
            p = t[(ds, a)]
            if 1 not in p:
                continue
            xs = sorted(p)
            ax.plot(xs, [p[1] / p[x] for x in xs], color=color, marker="o", ms=3, label=NAMES[a])
        ax.set_xticks([1, 2, 4])
        ax.set_title(ds, color=INK)
        ax.set_xlabel("threads")
    axes[0].set_ylabel("speed-up")
    axes[0].set_yticks([1, 2, 3, 4])
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=4, frameon=False, handlelength=1.4)
    fig.subplots_adjust(wspace=0.1)
    save(fig, "threads_pi.pdf")


for f in (fig_trace, fig_peak, fig_outcomes, fig_curve, fig_lgsr, fig_threads):
    f()
