"""
A script to back up recent data products or products over a date range

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import os
import subprocess
import sys


def ts_print(msg):
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp}: {msg}")


def rclone(cmd, local_dir):
    if not os.path.exists(local_dir):
        ts_print(f"- Local directory doesn't exist. Skipping {local_dir}")
        return
    # Perform rclone
    ts_print(f"+ Backing up {local_dir}")
    output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
    if output.returncode != 0:
        ts_print(f"Failed to rclone {local_dir} with error: {output.stderr.decode('utf-8')}")


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Back up data products by date range")
    parser.add_argument("-d", "--days", help="How many days to backup starting from today")
    parser.add_argument("--start_date", help="Start date if using date range (YYYY-MM-DD)")
    parser.add_argument("--stop_date", help="Stop date if using date range (YYYY-MM-DD)")
    parser.add_argument("--env", default="ops", help="The environment to back up")
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args()

    if args.days is None and args.start_date is None and args.stop_date is None:
        ts_print("Please specify args, either --days or --start_date and --stop_date")
        sys.exit(1)

    if (args.start_date is None and args.stop_date is not None) or \
            (args.start_date is None and args.stop_date is not None):
        ts_print("ERROR: You must provide both start and stop time if one is given.")
        sys.exit(1)

    start_date = None
    stop_date = None

    if args.days:
        today = dt.datetime.utcnow()
        stop_date = today
        start_date = today - dt.timedelta(days=(int(args.days) - 1))

    # If start and/or stop date are given then they take precedence over --days
    if args.start_date and args.stop_date:
        start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d")
        stop_date = dt.datetime.strptime(args.stop_date, "%Y-%m-%d")

    delta = stop_date - start_date
    days = delta.days + 1
    dates = []
    for i in range(days):
        date = start_date + dt.timedelta(days=i)
        dates.append(date)

    date_strs = [d.strftime("%Y%m%d") for d in dates]
    ts_print(f"==================================")
    ts_print(f"Starting rclone backup using dates: {date_strs}")
    if args.dry_run:
        ts_print("--dry_run flag is set. No files will be backed up.")
    rclone_log = f"/store/shared/rclone/emit/logs/rclone_data_backups.log"

    # Time to back up /store/emit/<env>/data.  The directories are:
    # - acquisitions
    # - data_collections
    # - orbits
    # - planning_products
    # - streams

    # Acquisitions - Use dates to filter directories
    ts_print("Backing up acquisitions...")
    for date in dates:
        # Build command
        acq_filter = f"/store/shared/rclone/emit/filters/acquisition_filter.txt"
        date_str = date.strftime("%Y%m%d")
        local_dir = f"/store/emit/{args.env}/data/acquisitions/{date_str}"
        remote_dir = f"aws-jpl-ngis:jpl-ngis/EMIT/SDS/backups/data_products/{args.env}/data/acquisitions/{date_str}"
        cmd = ["/store/shared/rclone/bin/rclone", "sync", "-v", "--create-empty-src-dirs", "--filter-from", acq_filter]
        cmd += ["--log-file", rclone_log]
        if args.dry_run:
            cmd.append("--dry-run")
        cmd += [local_dir, remote_dir]
        rclone(cmd, local_dir)

    # Data collections - Use dates to figure out which DCIDs to sync
    ts_print("Backing up data collections (DCIDs)...")
    dcids = []
    for date in dates:
        date_str = date.strftime("%Y%m%d")
        dcid_date_dir = f"/store/emit/{args.env}/data/data_collections/by_date/{date_str}"
        if os.path.exists(dcid_date_dir):
            dcids = [d.split("_")[1] for d in os.listdir(dcid_date_dir)]
    if len(dcids) > 0:
        dcids = list(set(dcids))
        for dcid in dcids:
            date_str = date.strftime("%Y%m%d")
            local_dir = f"/store/emit/{args.env}/data/data_collections/by_dcid/{dcid[:5]}/{dcid}"
            remote_dir = f"aws-jpl-ngis:jpl-ngis/EMIT/SDS/backups/data_products/{args.env}/data/data_collections/by_dcid/{dcid[:5]}/{dcid}"
            cmd = ["/store/shared/rclone/bin/rclone", "sync", "-v", "--create-empty-src-dirs"]
            cmd += ["--log-file", rclone_log]
            if args.dry_run:
                cmd.append("--dry-run")
            cmd += [local_dir, remote_dir]
            rclone(cmd, local_dir)

    # Orbits - For now, just do the entire folder
    ts_print("Backing up orbits...")
    # Build command
    orbit_filter = f"/store/shared/rclone/emit/filters/orbit_filter.txt"
    local_dir = f"/store/emit/{args.env}/data/orbits"
    remote_dir = f"aws-jpl-ngis:jpl-ngis/EMIT/SDS/backups/data_products/{args.env}/data/orbits"
    cmd = ["/store/shared/rclone/bin/rclone", "sync", "-v", "--create-empty-src-dirs", "--filter-from", orbit_filter]
    cmd += ["--log-file", rclone_log]
    if args.dry_run:
        cmd.append("--dry-run")
    cmd += [local_dir, remote_dir]
    rclone(cmd, local_dir)

    # Planning Products - For now, just do the entire folder
    ts_print("Backing up planning products...")
    # Build command
    local_dir = f"/store/emit/{args.env}/data/planning_products"
    remote_dir = f"aws-jpl-ngis:jpl-ngis/EMIT/SDS/backups/data_products/{args.env}/data/planning_products"
    cmd = ["/store/shared/rclone/bin/rclone", "sync", "-v", "--create-empty-src-dirs"]
    cmd += ["--log-file", rclone_log]
    if args.dry_run:
        cmd.append("--dry-run")
    cmd += [local_dir, remote_dir]
    rclone(cmd, local_dir)

    # Streams - 1674, 1675, 1676, bad
    ts_print("Backing up streams...")
    streams = ["1674", "1675", "1676", "bad"]
    for stream in streams:
        for date in dates:
            # Build command
            date_str = date.strftime("%Y%m%d")
            local_dir = f"/store/emit/{args.env}/data/streams/{stream}/{date_str}"
            remote_dir = f"aws-jpl-ngis:jpl-ngis/EMIT/SDS/backups/data_products/{args.env}/data/streams/{stream}/{date_str}"
            cmd = ["/store/shared/rclone/bin/rclone", "sync", "-v", "--create-empty-src-dirs"]
            cmd += ["--log-file", rclone_log]
            if args.dry_run:
                cmd.append("--dry-run")
            cmd += [local_dir, remote_dir]
            rclone(cmd, local_dir)


if __name__ == '__main__':
    main()
