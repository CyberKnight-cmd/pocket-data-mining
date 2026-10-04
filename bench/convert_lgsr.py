#!/usr/bin/env python3
"""Convert dunnhumby "Let's Get Sort-of-Real" (LGSR) to SPMF utility format.

Source: https://www.dunnhumby.com/source-files/ (9 zip parts, 117 weekly CSVs, 40.7 GB raw;
dunnhumby describes it as dummy data: simulated, realistic till transactions).

usage: convert_lgsr.py RAW_DIR OUT_DIR [--jobs N]

Each weekly CSV holds whole baskets, so baskets are grouped one week at a time (in parallel
across weeks) and written to one part per week; finished parts are kept (resumable). Then the
parts are concatenated into the variants:

  lgsr.txt           one transaction per basket (BASKET_ID), all 117 weeks
  lgsr_1yr.txt       the same, first 52 weeks
  lgsr_custweek.txt  one transaction per loyalty-card customer per week (CUST_CODE set)

Item = numeric part of PROD_CODE; utility = SPEND in pence (rounded); a product bought more
than once in a transaction is merged (utilities summed); lines with SPEND <= 0 are dropped
(counted). Items in a line are sorted. Stats and SHA-256 per output go to OUT_DIR/CONVERSION.json.
"""
import csv, glob, hashlib, io, json, os, sys, zipfile
from multiprocessing import Pool

PART_DIR = "parts"


def weekly_members(raw_dir):
    out = []
    for z in sorted(glob.glob(os.path.join(raw_dir, "*Full-Part-*-of-9_.zip"))):
        with zipfile.ZipFile(z) as zf:
            for n in zf.namelist():
                b = os.path.basename(n)
                if b.startswith("transactions_") and b.endswith(".csv"):
                    out.append((b[len("transactions_"):-4], z, n))
    return sorted(out)


def fmt(tx):
    items = sorted(tx)
    utils = [tx[i] for i in items]
    return f"{' '.join(map(str, items))}:{sum(utils)}:{' '.join(map(str, utils))}\n"


def convert_week(args):
    week, zpath, member, out_dir = args
    pdir = os.path.join(out_dir, PART_DIR)
    done = os.path.join(pdir, f"{week}.done")
    if os.path.exists(done):
        return json.load(open(done))
    baskets, custweek = {}, {}
    rows = dropped = 0
    with zipfile.ZipFile(zpath) as zf, zf.open(member) as f:
        r = csv.reader(io.TextIOWrapper(f, encoding="utf-8", newline=""))
        h = next(r)
        ix = {c: k for k, c in enumerate(h)}
        iq, isp, ip, ic, ib = ix["QUANTITY"], ix["SPEND"], ix["PROD_CODE"], ix["CUST_CODE"], ix["BASKET_ID"]
        for row in r:
            rows += 1
            u = round(float(row[isp]) * 100)
            if u <= 0:
                dropped += 1
                continue
            item = int(row[ip][3:])
            b = baskets.setdefault(row[ib], {})
            b[item] = b.get(item, 0) + u
            if row[ic]:
                c = custweek.setdefault(row[ic], {})
                c[item] = c.get(item, 0) + u
    stats = {"week": week, "rows": rows, "dropped_nonpositive": dropped,
             "baskets": len(baskets), "custweeks": len(custweek)}
    for name, txs in (("basket", baskets), ("custweek", custweek)):
        tmp = os.path.join(pdir, f"{week}.{name}.tmp")
        with open(tmp, "w") as o:
            for key in sorted(txs):
                o.write(fmt(txs[key]))
        os.replace(tmp, os.path.join(pdir, f"{week}.{name}.txt"))
    json.dump(stats, open(done, "w"))
    return stats


def concat(out_dir, weeks, kind, dest):
    h = hashlib.sha256()
    n_tx = n_items = total = max_len = 0
    items = set()
    tmp = dest + ".tmp"
    with open(tmp, "w") as o:
        for w in weeks:
            with open(os.path.join(out_dir, PART_DIR, f"{w}.{kind}.txt")) as f:
                for line in f:
                    o.write(line)
                    h.update(line.encode())
                    its = line.split(":", 1)[0].split()
                    n_tx += 1
                    n_items += len(its)
                    max_len = max(max_len, len(its))
                    total += int(line.split(":")[1])
                    items.update(its)
    os.replace(tmp, dest)
    return {"file": os.path.basename(dest), "bytes": os.path.getsize(dest), "sha256": h.hexdigest(),
            "transactions": n_tx, "distinct_items": len(items), "avg_length": n_items / max(n_tx, 1),
            "max_length": max_len, "total_utility": total, "weeks": len(weeks)}


def main():
    raw, out = sys.argv[1], sys.argv[2]
    jobs = int(sys.argv[sys.argv.index("--jobs") + 1]) if "--jobs" in sys.argv else max(1, os.cpu_count() // 2)
    os.makedirs(os.path.join(out, PART_DIR), exist_ok=True)
    members = weekly_members(raw)
    print(f"{len(members)} weekly files", flush=True)
    with Pool(jobs) as p:
        stats = []
        for s in p.imap_unordered(convert_week, [(w, z, m, out) for w, z, m in members]):
            stats.append(s)
            print(f"week {s['week']}: {s['rows']} rows, {s['baskets']} baskets", flush=True)
    weeks = sorted(s["week"] for s in stats)
    report = {"source": "dunnhumby Let's Get Sort-of-Real (dummy data), https://www.dunnhumby.com/source-files/",
              "utility": "SPEND in pence; repeated products merged; SPEND <= 0 dropped",
              "rows": sum(s["rows"] for s in stats),
              "dropped_nonpositive": sum(s["dropped_nonpositive"] for s in stats), "outputs": []}
    for kind, name, ws in (("basket", "lgsr.txt", weeks), ("basket", "lgsr_1yr.txt", weeks[:52]),
                           ("custweek", "lgsr_custweek.txt", weeks)):
        r = concat(out, ws, kind, os.path.join(out, name))
        print(json.dumps(r), flush=True)
        report["outputs"].append(r)
    json.dump(report, open(os.path.join(out, "CONVERSION.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
