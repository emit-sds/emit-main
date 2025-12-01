"""
A script that performs typical daily checks on data processing and prints out the results

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import concurrent.futures
import datetime as dt
import glob
import json
import netrc
import os
import requests
import subprocess
import threading
import time

import earthaccess

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from emit_main.workflow.workflow_manager import WorkflowManager

BUCKET = "lp-prod-protected"
MAX_WORKERS = 40
MAX_RETRIES = 4

# Login using ~/.netrc
earthaccess.login(strategy="netrc")

def download_one(url, dir):
    """Download a single file with retries. Returns (url, success_bool)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            earthaccess.download(
                url,
                local_path=dir
            )
            return (url, True)

        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {url}: {e}")
            time.sleep(1 + attempt)

    return (url, False)


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the script")
    parser.add_argument("--work_dir", default=".", help="Working directory to download and manage files")
    args = parser.parse_args()

    env = args.env

    start = None
    stop = None
    if args.dates is not None:
        start, stop = args.dates.split(",")
        start_date = dt.datetime.strptime(start, "%Y%m%d")
        stop_date = dt.datetime.strptime(stop, "%Y%m%d")

    # Get workflow manager and db collections
    config_path = f"/store/emit/{env}/repos/emit-main/emit_main/config/{env}_sds_config.json"
    wm = WorkflowManager(config_path=config_path)
    db = wm.database_manager.db

    # Identify radiances that have been delivered based on .cmr.json files
    date_dirs = glob.glob(f"/store/emit/{env}/data/acquisitions/*")
    start_dir = f"/store/emit/{env}/data/acquisitions/{start}"
    stop_dir = f"/store/emit/{env}/data/acquisitions/{stop}"
    date_dirs = [dir for dir in date_dirs if start_dir <= dir < stop_dir]
    date_dirs.sort()
    # print(f"The filtered list of acquisition dirs to check is {date_dirs}")

    # Create mapping of local radiance paths and remote S3 paths
    acqs = {}
    # s3://lp-prod-protected/EMITL1BRAD.001/EMIT_L1B_RAD_001_20251112T052253_2531603_037/EMIT_L1B_RAD_001_20251112T052253_2531603_037.nc
    # https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/EMITL1BRAD.001/EMIT_L1B_RAD_001_20251118T003859_2532116_003/EMIT_L1B_RAD_001_20251118T003859_2532116_003.nc
    url_base = "https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/EMITL1BRAD.001"
    for dir in date_dirs:
        cmr_json_paths = glob.glob(f"{dir}/*/l1b/*cmr.json")
        for cmr_json_path in cmr_json_paths:
            acq_id = os.path.basename(cmr_json_path).split("_")[0]
            local_rdn_path = cmr_json_path.replace(".cmr.json", ".img")
            with open(cmr_json_path, "r") as f:
                meta = json.load(f)
                granule_ur = meta["GranuleUR"]
                url = f"{url_base}/{granule_ur}/{granule_ur}.nc"
            acqs[acq_id] = {
                "local_rdn_path": local_rdn_path,
                "url": url
            }

    # print(acqs)

    # Parallel execution
    urls = []
    for a in acqs:
        urls.append(acqs[a]["url"])
    urls = urls[:5]
    print(f"urls: {urls}")
    failed = []
    total = len(urls)
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(download_one, url, args.work_dir): url for url in urls}

        for future in as_completed(futures):
            url = futures[future]
            ok_url, success = future.result()

            if success:
                print(f"✅ Downloaded: {url}")
            else:
                print(f"❌ FAILED after retries: {url}")
                failed.append(url)

    # Save failed list
    if failed:
        with open(f"{args.work_dir}/failed.txt", "w") as f:
            for url in failed:
                f.write(url + "\n")

        print(f"\n❗ {len(failed)} downloads failed. See failed.txt")
    else:
        print("\n🎉 All downloads succeeded!")


if __name__ == '__main__':
    main()
