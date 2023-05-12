"""
A script that performs typical daily checks on data processing and prints out the results

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt

from emit_main.workflow.workflow_manager import WorkflowManager


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    args = parser.parse_args()

    # TODO: Ignore results with comments. Pass in --ignore_comments flag

    env = args.env

    start = None
    stop = None
    if args.dates is not None:
        start, stop = args.dates.split(",")
        start = dt.datetime.strptime(start, "%Y%m%d")
        stop = dt.datetime.strptime(stop, "%Y%m%d")
        # Add a day to stop date to make it inclusive
        # stop = stop + dt.timedelta(days=1)

    # Get workflow manager and db collections
    config_path = f"/store/emit/{env}/repos/emit-main/emit_main/config/{env}_sds_config.json"
    wm = WorkflowManager(config_path=config_path)
    db = wm.database_manager.db
    dc_coll = db.data_collections
    orbit_coll = db.orbits
    acq_coll = db.acquisitions

    # Check for DCIDs with missing frames
    query = {
        "build_num": wm.config["build_num"],
        "frames_status": "incomplete",
        "associated_acquisitions": {"$exists": 0},
        "comments": {"$exists": 0}
    }
    if start is not None:
        query["start_time"] = {"$gte": start, "$lt": stop}
    results = list(dc_coll.find(query).sort("start_time", 1))
    print("---------------------")
    print(f"Description: Data collections with incomplete frames")
    print(f"Resolution: Investigate missing frames (python cron/missing_frames.py DCID). If they can't be recovered, "
          f"then run the command below.")
    print(f"Command: python run_workflow.py -c config/ops_sds_config.json -d DCID -p l1aframereport --ignore_missing_"
          f"frames")
    print(f"Query: {query}")
    print(f"Results: {len(results)}\n")
    if len(results) > 0:
        for r in results:
            print(f"dcid: {r['dcid']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}, orbit: {r['orbit']}")
        print("")

    # Check for orbits with missing BAD sto data
    query = {
        "build_num": wm.config["build_num"],
        "bad_status": "incomplete",
        "associated_bad_netcdf": {"$exists": 0},
        "comments": {"$exists": 0}
    }
    if start is not None:
        query["start_time"] = {"$gte": start, "$lt": stop}
    results = list(orbit_coll.find(query).sort("start_time", 1))
    print("---------------------")
    print(f"Description: Orbits with incomplete BAD .sto files")
    print(f"Resolution: Investigate missing .sto files. If they can't be recovered, then run the command below.")
    print(f"Command: python run_workflow.py -c config/ops_sds_config.json -p l1abad -o ORBIT_ID --ignore_missing_bad")
    print(f"Query: {query}")
    print(f"Results: {len(results)}\n")
    if len(results) > 0:
        for r in results:
            print(f"orbit_id: {r['orbit_id']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}")
        print("")

    # Check for orbits with missing raw
    query = {
        "build_num": wm.config["build_num"],
        "raw_status": "incomplete",
        "num_scenes": {"$exists": 0},
        "comments": {"$exists": 0}
    }
    if start is not None:
        query["start_time"] = {"$gte": start, "$lt": stop}
    results = list(orbit_coll.find(query).sort("start_time", 1))
    print("---------------------")
    print(f"Description: Orbits with incomplete raw that need DAAC scene numbers")
    print(f"Resolution: Investigate missing raw scenes in orbit (DCIDs not acquired?). If they can't be "
          f"recovered, then run the command below.")
    print(f"Command: python run_workflow.py -c config/ops_sds_config.json -p daacscenes -o ORBIT_ID --override_output")
    print(f"Query: {query}")
    print(f"Results: {len(results)}\n")
    if len(results) > 0:
        for r in results:
            print(f"orbit_id: {r['orbit_id']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}")
        print("")

    # Check for orbits with missing radiance
    query = {
        "build_num": wm.config["build_num"],
        "radiance_status": "incomplete",
        "products.l1b": {"$exists": 0},
        "comments": {"$exists": 0}
    }
    if start is not None:
        query["start_time"] = {"$gte": start, "$lt": stop}
    results = list(orbit_coll.find(query).sort("start_time", 1))
    print("---------------------")
    print(f"Description: Orbits with incomplete radiance that need geolocation")
    print(f"Resolution: Investigate missing radiance scenes in orbit (failed calibration?). If they can't be "
          f"recovered, then run the command below.")
    print(f"Command: python run_workflow.py -c config/ops_sds_config.json -o ORBIT_ID -p l1bgeo "
          f"--ignore_missing_radiance")
    print(f"Query: {query}")
    print(f"Results: {len(results)}\n")
    if len(results) > 0:
        for r in results:
            print(f"orbit_id: {r['orbit_id']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}")
        print("")

    # Check for orbits that failed to geocorrect
    query = {
        "build_num": wm.config["build_num"],
        "radiance_status": "complete",
        "products.l1b": {"$exists": 0},
        "comments": {"$exists": 0}
    }
    if start is not None:
        query["start_time"] = {"$gte": start, "$lt": stop}
    results = list(orbit_coll.find(query).sort("start_time", 1))
    print("---------------------")
    print(f"Description: Complete orbits that have not been geolocated")
    print(f"Resolution: Check to see if geolocation failed and what the error is. You can re-run using the command "
          f"below, but will most likely need to report this to Mike Smyth. ")
    print(f"Command: python run_workflow.py -c config/ops_sds_config.json -o ORBIT_ID -p l1bgeo")
    print(f"Query: {query}")
    print(f"Results: {len(results)}\n")
    if len(results) > 0:
        for r in results:
            print(f"orbit_id: {r['orbit_id']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}")
        print("")

    # Check for raw scenes with no radiance
    query = {
        "build_num": wm.config["build_num"],
        "submode": "science",
        "num_valid_lines": {"$gte": 2},
        "products.l1a.raw.img_path": {"$exists": 1},
        "products.l1b.rdn.img_path": {"$exists": 0}
    }
    if start is not None:
        query["start_time"] = {"$gte": start, "$lt": stop}
    results = list(acq_coll.find(query).sort("start_time", 1))
    print("---------------------")
    print(f"Description: Valid scenes with no radiance products")
    print(f"Resolution: Check to see if calibration failed and what the error is. You can re-run using the command "
          f"below.")
    print(f"Command: python run_workflow.py -c config/ops_sds_config.json -a ACQUISITION_ID -p l1bcal")
    print(f"Query: {query}")
    print(f"Results: {len(results)}\n")
    if len(results) > 0:
        for r in results:
            print(f"acquisition_id: {r['acquisition_id']}, start_time: {r['start_time']}, stop_time: {r['stop_time']}, "
                  f"orbit: {r['orbit']}")
        print("")

    print("---------------------")


if __name__ == '__main__':
    main()
