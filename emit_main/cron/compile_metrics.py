"""
A script to compile metrics from various places and record them in a file or database table

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import glob
import os
import sys


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Compile metrics for tracking")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("-d", "--dates", default="20230801,20230805", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("-o", "--output", help="Output report path (use .csv extension)")
    args = parser.parse_args()

    env = args.env

    if args.dates is not None:
        start, stop = args.dates.split(",")

    if args.output is None:
        args.output = f"/store/emit/{env}/reports/trending/metrics.csv"

    outfile = open(args.output, "w")
    outfile.write("TIMESTAMP,APID,PACKETS,PSC_GAPS,MISSING_PACKETS,DUPLICATE_PACKETS\n")

    for apid in ["1675"]:
        date_dirs = glob.glob(f"/store/emit/{env}/data/streams/{apid}/*")
        start_dir = f"/store/emit/{env}/data/streams/{apid}/{start}"
        stop_dir = f"/store/emit/{env}/data/streams/{apid}/{stop}"
        date_dirs = [dir for dir in date_dirs if start_dir <= dir <= stop_dir]
        date_dirs.sort()
        print(f"The filtered list of dirs to check is {date_dirs}")

        for dir in date_dirs:
            l0_reports = glob.glob(f"{dir}/l0/*report.txt")
            for report in l0_reports:
                with open(report, "r") as f:
                    for line in f.readlines():
                        if "Packet Count" in line and "Duplicate" not in line:
                            packet_count = int(line.rstrip("\n").split(" ")[-1])
                        if "Missing PSC Count" in line:
                            missing_packets = int(line.rstrip("\n").split(" ")[-1])
                        if "PSC Errors" in line:
                            psc_gaps = int(line.rstrip("\n").split(" ")[-1])
                        if "Duplicate Packet Count" in line:
                            duplicate_packets = int(line.rstrip("\n").split(" ")[-1])
                timestamp_str = os.path.basename(report).split("_")[2]
                timestamp = dt.datetime.strptime(timestamp_str, "%Y%m%dt%H%M%S")
                timestamp_utc = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                outfile.write(",".join([timestamp_utc, apid, str(packet_count), str(psc_gaps), str(missing_packets),
                                        str(duplicate_packets)]) + "\n")

    outfile.close()


if __name__ == '__main__':
    main()
