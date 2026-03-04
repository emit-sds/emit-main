"""
A script to compile metrics from various places and record them in a file or database table

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import os
import sys

import pandas as pd

from emit_main.database.database_manager import DatabaseManager


def query_db(collection, start_date, stop_date, product):
    pipeline = [
        {
            "$match": {
                product: {
                    "$gte": start_date,
                    "$lt": stop_date
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": f"${product}",
                        "unit": "day"
                    }
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id": 1}
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "count": 1
            }
        }
    ]

    return list(collection.aggregate(pipeline))

def build_series(results, column_name, full_index):
    df = pd.DataFrame(results)
    if len(results) > 0:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df = df.rename(columns={"count": column_name})
    df = df.reindex(full_index)
    return df

def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Compile metrics for tracking")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("--out_dir", default=".")
    args = parser.parse_args()

    env = args.env

    if args.dates is None:
        print("You must specify --dates")
        sys.exit()

    start, stop = args.dates.split(",")
    start_date = dt.datetime.strptime(start, "%Y%m%d")
    stop_date = dt.datetime.strptime(stop, "%Y%m%d")
    print(f"Using start and stop of {start} and {stop}, and start_date and stop_date of {start_date} and {stop_date}")

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    print(f"Using config_path {config_path}")
    dm = DatabaseManager(config_path)
    acq_coll = dm.db.acquisitions
    stream_coll = dm.db.streams
    orbit_coll = dm.db.orbits

    products = (
        "products.daac.ccsds_ummg.created",
        "products.l1b.att_ummg.created",
        "products.l1a.raw_ummg.created",
        "products.l1b.rdn_ummg.created",
        "products.l2a.rfl_ummg.created",
        "products.l2b.abun_ummg.created",
        "products.ghg.ch4.ch4_ummg.created",
        "products.ghg.co2.co2_ummg.created",
        "products.mask.maskTf_ummg.created",
        "products.frcov.frcov_ummg.created"
    )

    full_index = pd.date_range(start=start_date, end=stop_date, freq="D")

    dfs = []
    for p in products:
        print(f"Product: {p}")
        collection = acq_coll
        if "ccsds" in p:
            collection = stream_coll
        if "att" in p:
            collection = orbit_coll
        dfs.append(build_series(query_db(collection, start_date, stop_date, p), p.split(".")[-2].replace("_ummg", ""), full_index))

    combined = (
        pd.concat(dfs, axis=1)
        .fillna(0)
        .astype(int)
    )

    combined.index.name = "date"
    print(combined)
    outfile = os.path.join(args.out_dir, f"delivery_counts_by_day_{start}_{stop}.csv")
    combined.to_csv(outfile, sep=",")


if __name__ == '__main__':
    main()
