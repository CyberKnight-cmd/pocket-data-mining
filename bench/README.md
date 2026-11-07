# Benchmarks (Raspberry Pi)

All raw data is kept; tables are derived afterwards, so nothing has to be re-run to answer a
new question.

| file | role |
|---|---|
| `clean_datasets.py` | writes identical cleaned inputs for every tool (drops metadata lines, merges duplicate items, scales decimal utilities x100) + `CLEANING.json` with SHA-256 and stats |
| `calibration.json` | per-dataset threshold calibration (local machine, EFIM/FHM) |
| `plan.py` | generates the job list (`plan.json`) for all experiments |
| `collect.py` | resumable runner; one directory of raw data per job |
| `analyze.py` | derives `runs.csv` and `summary.md` from the raw data |
| `pi_bench.py`, `summarize.py` | first, summary-only harness (superseded) |

## Experiments (`plan.py`)
- **ref** — reference outputs per dataset (HUIs, closed HUIs, average-utility itemsets).
- **cmp** — Air-HUIM vs SPMF, same algorithm, budgets 64/256/1024 MB (SPMF: `-Xmx` = budget).
- **curve** — runtime and memory vs budget, 32 MB … 2 GB (degradation curves).
- **ablation** — the pre-budget implementation (commit dc6bd1b) at the same budgets.
- **hard** — a watchdog kills any process whose RSS exceeds the budget (the Pi kernel has no
  memory cgroup controller; enabling it needs `cgroup_enable=memory` in
  `/boot/firmware/cmdline.txt` and a reboot).
- **threads** — 1/2/4 threads.  **spmf-free** — SPMF without a heap cap.

## Per-run raw data (`~/bench/data/runs/<job_id>/`)
`job.json`, `env.json` (SoC temperature, clock, throttling, memory at start/end), `stdout.log`
(full log; Air-HUIM memory log every 250 ms), `ts.csv` (~100 ms samples: RSS/HWM/VSZ, user/sys
CPU, page faults, I/O bytes, threads, context switches, spill-directory bytes, temperature,
clock), `output.norm.gz` (normalized result set), `result.json` (summary).
`static_env.json` holds hardware/OS/Java versions and binary hashes.

Job IDs hash the run parameters and a per-algorithm `code_version` (not the binary hash):
rebuilding to add algorithms does not invalidate finished runs; bump `code_version` in
`plan.py` when an algorithm's code changes.

## Operating it on the Pi
```bash
python3 ~/air-huim/bench/collect.py status ~/bench/plan.json --data ~/bench/data
bash ~/bench/deploy.sh          # stop, install ~/bench/plan_full2.json, restart (resumes)
python3 ~/air-huim/bench/analyze.py ~/bench/data ~/bench/analysis
```
