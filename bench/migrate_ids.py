import json, os, sys
sys.path.insert(0, "/home/pi/air-huim/bench")
from collect import job_id
runs = "/home/pi/bench/data/runs"
moved = 0
for d in os.listdir(runs):
    jf = os.path.join(runs, d, "job.json")
    if not os.path.exists(os.path.join(runs, d, "result.json")):
        continue
    job = json.load(open(jf))
    new = job_id({k: v for k, v in job.items() if k != "id"})
    if new != d and not os.path.exists(os.path.join(runs, new)):
        job["id"] = new
        json.dump(job, open(jf, "w"), indent=1)
        r = json.load(open(os.path.join(runs, d, "result.json")))
        r["id"] = new; r["job"]["id"] = new
        json.dump(r, open(os.path.join(runs, d, "result.json"), "w"), indent=1)
        os.rename(os.path.join(runs, d), os.path.join(runs, new))
        moved += 1
print("migrated", moved)
