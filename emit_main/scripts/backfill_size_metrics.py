"""
Backfill missing/zero file-size metrics in trending_acquisitions using the average of
the non-zero values in the rest of the collection. Only rows that have rfl_img_size_bytes
are updated.

Dry run by default; pass --apply to write.
"""

import argparse
import datetime as dt

from emit_main.database.database_manager import DatabaseManager

CH4_FIELDS = [f"{p}_size_bytes" for p in (
    "ch4_img", "ch4_tif", "sensch4_img", "sensch4_tif", "uncertch4_img", "uncertch4_tif",
    "ch4d1_tif", "ch4d2_tif")]
CO2_FIELDS = [f.replace("ch4", "co2") for f in CH4_FIELDS]
MIN_FIELDS = ["min_nc_size_bytes", "minunc_nc_size_bytes"]
FRCOV_FIELDS = [f"{p}_tif_size_bytes" for p in (
    "frcovqc", "pv", "pvunc", "npv", "npvunc", "bare", "bareunc")]

# (label, fields, [(start, stop), ...])  -- stop is exclusive
AUG = (dt.datetime(2024, 8, 1), dt.datetime(2024, 9, 1))
TASKS = [
    ("L2B mineral", MIN_FIELDS, [AUG]),
    ("CH4", CH4_FIELDS, [AUG]),
    ("CO2", CO2_FIELDS, [AUG]),
    ("Fractional cover", FRCOV_FIELDS, [
        (dt.datetime(2024, 5, 1), dt.datetime(2024, 6, 1)),
        (dt.datetime(2024, 7, 1), dt.datetime(2024, 9, 1)),
        (dt.datetime(2024, 10, 15), dt.datetime(2024, 11, 1)),
    ]),
]


def field_average(coll, field):
    """Average of the non-zero values of `field` over the whole collection."""
    res = list(coll.aggregate([
        {"$match": {field: {"$type": "number", "$gt": 0}}},
        {"$group": {"_id": None, "avg": {"$avg": f"${field}"}, "n": {"$sum": 1}}},
    ]))
    return (round(res[0]["avg"]), res[0]["n"]) if res else (None, 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-e", "--env", default="ops")
    parser.add_argument("--apply", action="store_true", help="Write changes (default is dry run)")
    args = parser.parse_args()

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    coll = DatabaseManager(config_path).db.trending_acquisitions
    now = dt.datetime.now(tz=dt.timezone.utc)

    for label, fields, ranges in TASKS:
        for field in fields:
            avg, n = field_average(coll, field)
            if avg is None:
                print(f"[{label}] {field}: no non-zero values to average, skipping")
                continue
            for start, stop in ranges:
                query = {
                    "timestamp": {"$gte": start, "$lt": stop},
                    # Only scenes that went through processing (rfl img present)
                    "rfl_img_size_bytes": {"$exists": True, "$ne": None},
                    "$or": [{field: {"$exists": False}}, {field: None}, {field: 0}],
                }
                count = coll.count_documents(query)
                print(f"[{label}] {field}: avg={avg} (n={n}); {count} rows in {start:%Y-%m-%d}..{stop:%Y-%m-%d}")
                if args.apply and count:
                    coll.update_many(query, {"$set": {field: avg, "last_modified": now}})

    if not args.apply:
        print("Dry run only. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
