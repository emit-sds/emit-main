"""
A script that performs typical daily checks on data processing and prints out the results

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import glob
import os
import subprocess
import sys
import yaml

from collections import OrderedDict

from emit_main.workflow.workflow_manager import WorkflowManager


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    args = parser.parse_args()

    env = args.env

    start = None
    stop = None
    if args.dates is not None:
        start, stop = args.dates.split(",")
        start = dt.datetime.strptime(start, "%Y%m%d")
        stop = dt.datetime.strptime(stop, "%Y%m%d")
        # Add a day to stop date to make it inclusive
        stop = stop + dt.timedelta(days=1)

    # Get workflow manager and db collections
    config_path = f"/store/emit/{env}/repos/emit-main/emit_main/config/{env}_sds_config.json"
    wm = WorkflowManager(config_path=config_path)
    db = wm.database_manager.db
    dc_coll = db.data_collections

    # Check for DCIDs with missing frames
    query = {
        "build_num": wm.config["build_num"],
        "frames_status": "incomplete",
        "associated_acquisitions": {"$exists": 0},
        "comments": {"$exists": 0}
    }
    results = list(dc_coll.find(query).sort("start_time", 1))
    print(f"Found {len(results)} data collections with \"frame status\" incomplete and no acquisitions or comments:")
    for r in results:
        print(f"DCID: {r['dcid']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}")


if __name__ == '__main__':
    main()
