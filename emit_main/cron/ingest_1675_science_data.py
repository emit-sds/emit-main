"""
A script to ingest the 1675 science data HOSC files

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import logging
import os
import subprocess
import sys


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Ingest science data from egse1 remote server")
    parser.add_argument("-d", "--date", help="Date (YYYYMMDD)")
    parser.add_argument("-e", "--env", default="ops", help="Which env to use (ops, test, dev)")
    parser.add_argument("-l", "--level", default="INFO", help="Log level")
    args = parser.parse_args()

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("ingest-science-data")

    # Set up file handler logging
    handler = logging.FileHandler(f"/store/emit/{args.env}/logs/cron_ingest_science_data.log")
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # If date is left blank, assume we are ingesting yesterday's data
    if args.date is None:
        yesterday = dt.datetime.utcnow() - dt.timedelta(days=1)
        args.date = yesterday.strftime("%Y%m%d")
    date = args.date
    # Now get date + 1 day in order to get transfer log date
    log_date = dt.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    logger.info(f"Ingesting science data for date {date}...")

    # Define paths
    user = "emit-cron-ops"
    remote_server = "emit-egse1.jpl.nasa.gov"
    remote_dir = "/proj/emit/ops/data/emit-egse1/APID_1675"
    remote_log = f"{remote_dir}/epc-rsync-{log_date}.log"
    egse_dir = f"/store/emit/{args.env}/ingest/egse1"
    ingest_date_dir = f"{egse_dir}/{date}"

    # Check if the log file says "Transfer Completed"
    output = subprocess.run(f"ssh {user}@{remote_server} \"cat {remote_log} | grep -q 'Transfer Completed'\"",
                            shell=True)
    if output.returncode != 0:
        logger.info(f"The remote log file does not say 'Transfer Completed'. Exiting...")
        sys.exit()

    logger.info(f"Log file on emit-egse1 at {remote_log} says that the transfer completed.")
    logger.info(f"Copying files from {remote_server}:{remote_dir}/1675_{date}* to {ingest_date_dir}")
    if not os.path.exists(ingest_date_dir):
        os.makedirs(ingest_date_dir)
    subprocess.run(f"scp -p {user}@{remote_server}:{remote_dir}/1675_{date}* {ingest_date_dir}/", shell=True)

    files = os.listdir(ingest_date_dir)
    # Exit if directory is empty
    if len(files) == 0:
        logger.info(f"Did not find any files in {ingest_date_dir} after copying. Exiting...")
        sys.exit()

    # Check for complete set of files
    dir_list = [os.path.join(ingest_date_dir, f) for f in files]
    # First file in list does not sort correctly, so keep it separate for now
    first_file = None
    first_rpsm_file = None
    files = []
    rpsm_files = []
    for f in dir_list:
        if "-" not in os.path.basename(f):
            if f.endswith(".rpsm"):
                first_rpsm_file = f
            else:
                first_file = f
            continue
        if f.endswith(".rpsm"):
            rpsm_files.append(f)
        else:
            files.append(f)

    files.sort(key=lambda x: int(os.path.basename(x).split("-")[1]))
    rpsm_files.sort(key=lambda x: int(os.path.basename(x).split("-")[1].split(".")[0]))
    files.insert(0, first_file)
    rpsm_files.insert(0, first_rpsm_file)
    # logger.info("Found files: " + str(files))
    # logger.info("Found rpsm_files: " + str(rpsm_files))

    # Check that there is an rpsm file for each file
    for i in range(len(files)):
        if f"{files[i]}.rpsm" != rpsm_files[i]:
            logger.info(f"Did not find matching rpsm file for {files[i]}. Exiting...")
            sys.exit()

    # Check the sequence
    for i in range(len(files)):
        if i == 0:
            continue
        if str(i) != files[i].split("-")[1]:
            logger.info(f"Sequence error while inspecting files. Exiting...")
            sys.exit()

    # Check the file sizes
    for i in range(len(files) - 1):
        if os.path.getsize(files[i]) != 4194304992:
            logger.info(f"File {files[i]} is less than expected size of 4194304992. Exiting...")
            sys.exit()
    if os.path.getsize(files[-1]) >= 4194304992:
        logger.info(f"Last file {files[-1]} is greater than or equal to expected size of 4194304992. Exiting...")
        sys.exit()

    # if we made it this far, then we are okay to process the files
    logger.info("The copied files passed all checks. Renaming files, changing, permissions, and processing...")
    # Make rpsm dir and move rpsm files
    rpsm_dir = f"{ingest_date_dir}/rpsm"
    if not os.path.exists(rpsm_dir):
        os.makedirs(rpsm_dir)
    for rf in rpsm_files:
        subprocess.run(f"mv {rf} {rpsm_dir}/", shell=True)

    # Add _hsc.bin to all files
    for i in range(len(files)):
        if i == 0:
            subprocess.run(f"mv {files[i]} {files[i]}hsc.bin", shell=True)
        else:
            subprocess.run(f"mv {files[i]} {files[i]}_hsc.bin", shell=True)

    # Change permissions
    subprocess.run(f"chgrp -R emit-{args.env} {ingest_date_dir}", shell=True)
    subprocess.run(f"chmod -R g+w {ingest_date_dir}", shell=True)

    # Run all files in a bash for loop
    logger.info("The following stream files will be ingested in this order: ")
    subprocess.run(f"for file in `ls {ingest_date_dir}/1675* | sort -V`; do echo $file >> "
                   f"/store/emit/{args.env}/logs/cron_ingest_science_data.log; done", shell=True)
    cmd = f"for file in `ls {ingest_date_dir}/1675* | sort -V`; do " \
          f"python /store/emit/{args.env}/repos/emit-main/emit_main/run_workflow.py -c " \
          f"/store/emit/{args.env}/repos/emit-main/emit_main/config/{args.env}_sds_config.json -p l1aframe " \
          f"--partition cron --miss_pkt_thresh 0.1 -s $file; done"
    logger.info(f"Command to execute: {cmd}")
    subprocess.run(cmd, shell=True)


if __name__ == '__main__':
    main()
