"""
A script calculates time from acquisition to delivery

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt

from emit_main.workflow.workflow_manager import WorkflowManager


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Calculate delivery raw_times")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    args = parser.parse_args()

    start = None
    stop = None
    if args.dates is not None:
        start, stop = args.dates.split(",")
        start = dt.datetime.strptime(start, "%Y%m%d")
        stop = dt.datetime.strptime(stop, "%Y%m%d")
        # Add a day to stop date to make it inclusive
        # stop = stop + dt.timedelta(days=1)

    # Get workflow manager and db collections
    config_path = f"/store/emit/ops/repos/emit-main/emit_main/config/ops_sds_config.json"
    wm = WorkflowManager(config_path=config_path)
    db = wm.database_manager.db
    acq_coll = db.acquisitions

    query = {
        "build_num": wm.config["build_num"],
        "start_time": {"$gte": start, "$lt": stop},
        "products.l1a.raw_ummg.created": {"$exists": 1},
        "products.l1b.rdn_ummg.created": {"$exists": 1},
        "products.l2a.rfl_ummg.created": {"$exists": 1}
    }
    results = list(acq_coll.find(query))
    print(f"Found {len(results)} results")

    raw_times = [(r["products"]["l1a"]["raw_ummg"]["created"] - r["start_time"]).total_seconds() / 3600 for r in
                 results]
    rdn_times = [(r["products"]["l1b"]["rdn_ummg"]["created"] - r["start_time"]).total_seconds() / 3600 for r in
                 results]
    rfl_times = [(r["products"]["l2a"]["rfl_ummg"]["created"] - r["start_time"]).total_seconds() / 3600 for r in
                 results]
    for i in range(10):
        print(f"{results[i]['acquisition_id']} {results[i]['start_time']} {results[i]['products']['l1a']['raw_ummg']['created']} {raw_times[i]:.2f}")

    print(f"Raw mean: {sum(raw_times) / len(raw_times)}")
    print(f"Rdn mean: {sum(rdn_times) / len(rdn_times)}")
    print(f"Rfl mean: {sum(rfl_times) / len(rfl_times)}")


if __name__ == '__main__':
    main()
