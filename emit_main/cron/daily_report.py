"""
A script to generate a daily report

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

def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    parser.add_argument("-d", "--date", help="Date (YYYYMMDD)")
    parser.add_argument("-o", "--output", help="Output report path (use .yml extension)")
    args = parser.parse_args()

    env = args.env

    if args.date is None:
        today = dt.datetime.utcnow()
        args.date = today.strftime("%Y%m%d")
    date = args.date

    if args.output is None:
        report_path = f"/store/emit/{env}/reports/daily_{date}.yml"
    else:
        report_path = args.output

    report = OrderedDict()
    report = {
        "title": f"Daily Summary Report for {date}",
        "time": dt.datetime.now(),
        "streams": []
    }

    # Check streams
    for apid in ("1674", "1675", "1676"):
        total_packets = 0
        total_missing = 0
        total_gaps = 0
        report_paths = glob.glob(f"/store/emit/{env}/data/streams/{apid}/{date}/l0/*report.txt")
        files_with_gaps = []
        for p in report_paths:
            with open(p, "r") as f:
                for line in f.readlines():
                    if "Packet Count" in line:
                        packet_count = int(line.rstrip("\n").split(" ")[-1])
                    if "Missing PSC Count" in line:
                        missing_packets = int(line.rstrip("\n").split(" ")[-1])
                    if "PSC Errors" in line:
                        psc_gaps = int(line.rstrip("\n").split(" ")[-1])
            if psc_gaps > 0:
                file_with_gaps = OrderedDict()
                file_with_gaps = {
                    "file": p,
                    "missing_packets": missing_packets,
                    "psc_gaps": psc_gaps
                }
                files_with_gaps.append(file_with_gaps)
            total_packets += packet_count
            total_missing += missing_packets
            total_gaps += psc_gaps

        apid_report = OrderedDict()
        apid_report = {
            "apid": apid,
            "total_packets": total_packets,
            "missing_packets": total_missing,
            "psc_gaps": total_gaps
        }
        if len(files_with_gaps) > 0:
            apid_report["files_with_gaps"] = files_with_gaps

        report["streams"].append(apid_report)

    total_packets = 0
    total_missing = 0
    total_gaps = 0
    for apid in report["streams"]:
        total_packets += apid["total_packets"]
        total_missing += apid["missing_packets"]
        total_gaps += apid["psc_gaps"]

    stream_totals = {
        "total_packets": total_packets,
        "missing_packets": total_missing,
        "psc_gaps": total_gaps
    }
    report["stream_totals"] = stream_totals

    with open(report_path, "w") as f:
        yaml.dump(report, f, sort_keys=False)

if __name__ == '__main__':
    main()
