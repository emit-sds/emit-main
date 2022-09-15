"""
A script to list corrupt frames and frames with decompression errors

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import csv
import datetime as dt
import glob
import os
import subprocess
import sys

from collections import OrderedDict


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="List corrupt and decompression errors")
    parser.add_argument("-d", "--data_dir", default="/store/emit/ops/data", help="Data directory")
    parser.add_argument("-o", "--output_dir", default=".", help="Output report path (use .csv)")
    args = parser.parse_args()

    # List all corrupt frames
    l1a_reports = glob.glob(f"{args.data_dir}/streams/1675/*/l1a/*report.txt")
    l1a_reports.sort()
    rows = []
    for report in l1a_reports:
        date = report.split("/")[-3]
        with open(report, "r") as f:
            lines = []
            index = 0
            count = 0
            for line in f.readlines():
                lines.append(line.rstrip("\n"))
                if "Corrupt Frame Errors Encountered" in line:
                    index = count
                    num_corrupt = int(line.rstrip("\n").split(" ")[-1])
                count += 1

        corrupt_frames = lines[index + 2: index + 2 + num_corrupt]
        corrupt_frames.sort()
        for frame in corrupt_frames:
            toks = frame.split("_")
            rows.append([date, frame, f"{toks[0]}_{toks[2]}"])

    with open(f"{args.output_dir}/corrupt_frames.csv", "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["date", "frame", "dcid_framenum"])
        for row in rows:
            csvwriter.writerow(row)

    # List all decompression errors
    reassembly_reports = glob.glob(f"{args.data_dir}/data_collections/by_date/*/*/*decomp*/*reassembly_report.txt")
    reassembly_reports.sort()
    rows = []
    for report in reassembly_reports:
        date = report.split("/")[-4]
        dcid = 0
        with open(report, "r") as f:
            lines = []
            index = 0
            count = 0
            for line in f.readlines():
                lines.append(line.rstrip("\n"))
                if "DCID:" in line:
                    dcid = line.rstrip("\n").split(" ")[-1]
                if "Total decompression errors" in line:
                    index = count
                    num_errors = int(line.rstrip("\n").split(" ")[-1])
                count += 1

        error_frame_nums = lines[index + 2: index + 2 + num_errors]
        error_frame_nums.sort()
        for num in error_frame_nums:
            rows.append([date, f"{dcid}_{num}"])

    with open(f"{args.output_dir}/decompression_error_frames.csv", "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["date", "dcid_framenum"])
        for row in rows:
            csvwriter.writerow(row)

if __name__ == '__main__':
    main()