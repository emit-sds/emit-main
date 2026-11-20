"""
One-off fix for acquisitions missed by v2_db_updates.py (falsy-zero bug).  Values come from the CSVs
(pulled from the pre-update DB backup) and are written to:
    products.l2a.01.mask.cloud_fraction
    products.mask.02.maskTf.cloud_fraction   (from cloud_fraction_02)
    products.mask.02.maskTf.nodata_fraction

Steps:
  1. Read CSVs.
  2. Pre-check: for every acquisition_id, count docs that are missing, already have the target field(s),
     or lack the parent object.  Exits without writing if any target is already set.
  3. Bulk update (only with --apply).  Dry run by default.

Usage:
    python fix_cloud_fractions.py -e test                      # pre-check + dry run
    python fix_cloud_fractions.py -e test --apply              # real run
Log is written to <csv-dir>/logs/.
"""

import argparse
import csv
import datetime as dt
import logging
import os
import sys

from pymongo import UpdateOne

from emit_main.database.database_manager import DatabaseManager

CSV_DIR = "/store/winstono/fix_cloud_fractions"

# name: (csv file, {csv column: target field}, parent path)
JOBS = {
    "cloud_fraction": (
        "acquisitions_with_cloud_fraction_0.csv",
        {"cloud_fraction": "cloud_fraction"},
        "products.l2a.01.mask",
    ),
    "maskTf": (
        "acquisitions_with_cloud_fraction_02_or_nodata_fraction_0.csv",
        {"cloud_fraction_02": "cloud_fraction", "nodata_fraction": "nodata_fraction"},
        "products.mask.02.maskTf",
    ),
}

log = logging.getLogger("fix_cloud_fractions")


def num(s):
    """CSV string -> int if it looks like one, else float."""
    try:
        return int(s)
    except ValueError:
        return float(s)


def read_csv(path, columns):
    rows = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            acq_id = r["acquisition_id"].strip()
            assert acq_id and acq_id not in rows, f"blank or duplicate acquisition_id: {acq_id!r}"
            rows[acq_id] = {col: num(r[col]) for col in columns}
    return rows


def precheck(coll, rows, targets, parent):
    """Return dict of counts and the list of ids that are safe to update."""
    ids = list(rows)
    counts = {"csv_rows": len(ids), "not_found": 0, "duplicate_docs": 0, "no_parent": 0, "already_set": 0, "ok": 0}
    ok_ids = []
    seen = {}
    proj = {"acquisition_id": 1, parent: 1}
    for i in range(0, len(ids), 1000):
        for d in coll.find({"acquisition_id": {"$in": ids[i:i + 1000]}}, proj):
            seen.setdefault(d["acquisition_id"], []).append(d)
    for acq_id in ids:
        docs = seen.get(acq_id, [])
        if not docs:
            counts["not_found"] += 1
            log.warning(f"PRECHECK {acq_id} not found")
            continue
        if len(docs) > 1:
            counts["duplicate_docs"] += 1
            log.warning(f"PRECHECK {acq_id} has {len(docs)} docs")
            continue
        cur = docs[0]
        for k in parent.split("."):
            cur = cur.get(k) if isinstance(cur, dict) else None
        if not isinstance(cur, dict):
            counts["no_parent"] += 1
            log.warning(f"PRECHECK {acq_id} missing {parent}")
        elif any(t in cur for t in targets):
            counts["already_set"] += 1
            log.warning(f"PRECHECK {acq_id} already has {[t for t in targets if t in cur]} under {parent}")
        else:
            counts["ok"] += 1
            ok_ids.append(acq_id)
    return counts, ok_ids


def main():
    parser = argparse.ArgumentParser(description="Fix cloud_fraction/nodata_fraction placement from CSVs")
    parser.add_argument("-e", "--env", default="test")
    parser.add_argument("--csv-dir", default=CSV_DIR)
    parser.add_argument("--apply", action="store_true", help="Write to the DB (default is dry run)")
    args = parser.parse_args()

    os.makedirs(os.path.join(args.csv_dir, "logs"), exist_ok=True)
    mode = "apply" if args.apply else "dryrun"
    log_path = os.path.join(args.csv_dir, "logs",
                            f"fix_cloud_fractions_{mode}_{args.env}_{dt.datetime.now():%Y%m%dT%H%M%S}.log")
    log.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    dbm = DatabaseManager(config_path)
    coll = dbm.db.acquisitions
    print(f"{'APPLY' if args.apply else 'DRY RUN'}  db={dbm.config['db_host']}/{dbm.config['db_name']}  log={log_path}")
    log.info(f"{mode} args={sys.argv}")

    # Pre-check everything before writing anything
    plans = {}
    for name, (fname, columns, parent) in JOBS.items():
        rows = read_csv(os.path.join(args.csv_dir, fname), columns)
        counts, ok_ids = precheck(coll, rows, list(columns.values()), parent)
        plans[name] = (rows, columns, parent, ok_ids)
        print(f"\n[{name}] -> {parent}\n  {counts}")
        log.info(f"[{name}] precheck {counts}")

    if any(len(p[3]) != len(p[0]) for p in plans.values()):
        print("\nPre-check found acquisitions that are not clean (see counts above / log).")
        print("Only the 'ok' acquisitions will be updated.")

    # Build updates.  Filter re-checks that the parent exists and targets are still unset.
    for name, (rows, columns, parent, ok_ids) in plans.items():
        ops = []
        for acq_id in ok_ids:
            flt = {"acquisition_id": acq_id, parent: {"$type": "object"}}
            flt.update({f"{parent}.{t}": {"$exists": False} for t in columns.values()})
            upd = {"$set": {f"{parent}.{t}": rows[acq_id][c] for c, t in columns.items()}}
            ops.append(UpdateOne(flt, upd))
            log.info(f"[{name}] {'UPDATE' if args.apply else 'WOULD UPDATE'} {flt} {upd}")
        for op in ops[:3]:
            print(f"  e.g. updateOne({op._filter}, {op._doc})")
        if args.apply and ops:
            matched = modified = 0
            for i in range(0, len(ops), 1000):
                res = coll.bulk_write(ops[i:i + 1000], ordered=False)
                matched += res.matched_count
                modified += res.modified_count
            msg = f"[{name}] attempted={len(ops)} matched={matched} modified={modified}"
            print(msg)
            log.info(msg)
            if modified != len(ops):
                print("  WARNING: modified count != attempted. See log.")
    print(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
