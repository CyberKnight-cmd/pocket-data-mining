#!/usr/bin/env python3
"""Convert H&M Personalized Fashion Recommendations purchases to SPMF utility format.

Source: Kaggle "h-and-m-personalized-fashion-recommendations" transactions_train.csv
(31.8M purchases, Sep 2018 - Sep 2020; mirror: huggingface.co/datasets/einrafh/
hnm-fashion-recommendations-data, data/raw/transactions_train.csv, 3,488,002,253 bytes).

usage: convert_hm.py transactions_train.csv OUT.txt

One transaction per customer per day. Item = article_id; utility = price * 10^6 rounded
(prices are normalised floats, e.g. 0.0508); an article bought more than once that day is
merged (utilities summed). The file is ordered by date, so one day is grouped at a time.
Stats and SHA-256 go to OUT.txt.json.
"""
import csv, hashlib, json, os, sys


def main():
    src, dest = sys.argv[1], sys.argv[2]
    h = hashlib.sha256()
    rows = n_tx = n_items = total = max_len = dropped = 0
    items_seen = set()
    day, groups = None, {}
    tmp = dest + ".tmp"

    def flush(o):
        nonlocal n_tx, n_items, total, max_len
        for cust in sorted(groups):
            tx = groups[cust]
            its = sorted(tx)
            us = [tx[i] for i in its]
            line = f"{' '.join(map(str, its))}:{sum(us)}:{' '.join(map(str, us))}\n"
            o.write(line)
            h.update(line.encode())
            n_tx += 1
            n_items += len(its)
            max_len = max(max_len, len(its))
            total += sum(us)
            items_seen.update(its)
        groups.clear()

    with open(src, newline="") as f, open(tmp, "w") as o:
        r = csv.reader(f)
        hdr = next(r)
        ix = {c: k for k, c in enumerate(hdr)}
        idt, icu, iar, ipr = ix["t_dat"], ix["customer_id"], ix["article_id"], ix["price"]
        for row in r:
            rows += 1
            if row[idt] != day:
                if day is not None and row[idt] < day:
                    sys.exit(f"input not ordered by date at row {rows}: {row[idt]} after {day}")
                flush(o)
                day = row[idt]
            u = round(float(row[ipr]) * 1_000_000)
            if u <= 0:
                dropped += 1
                continue
            tx = groups.setdefault(row[icu], {})
            a = int(row[iar])
            tx[a] = tx.get(a, 0) + u
            if rows % 5_000_000 == 0:
                print(f"{rows} rows, day {day}", flush=True)
        flush(o)
    os.replace(tmp, dest)
    stats = {"source": "H&M Personalized Fashion Recommendations (Kaggle), transactions_train.csv",
             "transaction": "customer x day", "utility": "price * 1e6, rounded; repeated articles merged",
             "rows": rows, "dropped_nonpositive": dropped, "file": os.path.basename(dest),
             "bytes": os.path.getsize(dest), "sha256": h.hexdigest(), "transactions": n_tx,
             "distinct_items": len(items_seen), "avg_length": n_items / max(n_tx, 1),
             "max_length": max_len, "total_utility": total}
    json.dump(stats, open(dest + ".json", "w"), indent=1)
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
