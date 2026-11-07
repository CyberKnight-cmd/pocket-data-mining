# Benchmark results behind the paper

One line per run in each `results.jsonl.gz`: the job (algorithm, dataset, budget, threads),
status, wall and CPU time, peak RSS, peak spill, HUI count, output hash and exactness inputs.
Written by `bench/collect.py`; the raw per-run directories (time series, logs, outputs; 3.5 GB)
are kept outside the repository.

| Directory | Machine | What |
|---|---|---|
| `pi-r6/` | Raspberry Pi 4, 4 GB | 4505 runs: exactness, budget adherence, vs SPMF at 24 MB-2 GB, hard limits, LGSR. `brute_report.json` is `bench/brute_verify.py` output. `trace-*.csv.gz` are the 100 ms samples behind the paper's Fig. 1 (LGSR FHM, 1 GB). |
| `laptop-timing/` | Ryzen 7 7435HS, 23 GB | 1-thread Air-HUIM vs SPMF, 102 pairs x 5 repeats (`bench/timing_plan.py`). |
| `laptop-large/` | same | 1-20 GB runs on LGSR, H&M and scaled chainstore, and 1-16 thread scaling (`plan_threads.json`). |

Air-HUIM runs whose `job.runtime_version` is not `r6` predate the final runtime; the paper uses
r6 only. `static_env.json` records the machine, OS, Java and binary hashes.
