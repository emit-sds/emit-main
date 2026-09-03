"""
A script to compile metrics from various places and record them in a file or database table

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime
import datetime as dt
import glob
import json
import os
import requests
import sys

import pandas as pd

import spectral.io.envi as envi

from dateutil.relativedelta import relativedelta

from emit_main.database.database_manager import DatabaseManager


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Compile metrics for tracking")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("--delivery_dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    args = parser.parse_args()

    env = args.env

    if args.dates is None or args.delivery_dates is None:
        print("You must specify --dates and --delivery_dates")

    start, stop = None, None

    if args.dates is not None:
        start, stop = args.dates.split(",")

    start_date = dt.datetime.strptime(start, "%Y%m%d")
    stop_date = dt.datetime.strptime(stop, "%Y%m%d")

    if args.delivery_dates is not None:
        dstart, dstop = args.delivery_dates.split(",")

    dstart_date = dt.datetime.strptime(dstart, "%Y%m%d")
    dstop_date = dt.datetime.strptime(dstop, "%Y%m%d")


    print(f"Using start and stop of {start} and {stop}, and delivery start_date and stop_date of {dstart_date} and {dstop_date}")

    config_path = f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json"
    print(f"Using config_path {config_path}")
    dm = DatabaseManager(config_path)

    date_dirs = glob.glob(f"/store/emit/{env}/reports/cmr/*")
    start_dir = f"/store/emit/{env}/reports/cmr/{start}"
    stop_dir = f"/store/emit/{env}/reports/cmr/{stop}"
    date_dirs = [dir for dir in date_dirs if start_dir <= dir < stop_dir]
    date_dirs.sort()

    # Find published granule URs
    cmr_granules = set()
    for date_dir in date_dirs:
        cmr_jsons = glob.glob(f"{date_dir}/*json")
        for cmr_json in cmr_jsons:
            with open(cmr_json, "r") as f:
                granules = json.load(f)["items"]
                for g in granules:
                    cmr_granules.add(g["meta"]["native-id"])

    # Lookup all submitted granules by delivery date
    granule_reports_coll = dm.db.granule_reports
    deliveries = list(granule_reports_coll.find({"timestamp": {"$gte": dstart_date, "$lt": dstop_date}}))
    unique_deliveries = set()
    for d in deliveries:
        if d["collection"] in ("EMITL1BRAD", "EMITL2ARFL", "EMITL2BMIN"):
            unique_deliveries.add(d["granule_ur"])

    # Loop through deliveries and print out ones not in CMR
    missing = []
    for g in unique_deliveries:
        if g not in cmr_granules:
            missing.append(g)

    missing.sort()
    for m in missing:
        print(m)
    print(f"Total: {len(missing)}")


if __name__ == '__main__':
    main()
