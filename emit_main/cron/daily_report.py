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
        "time_utc": dt.datetime.utcnow(),
        "time_local": dt.datetime.now(),
        "stream_totals": OrderedDict(),
        "streams": []
    }

    # Check streams
    for apid in ("1674", "1675", "1676"):
        total_packets_read = 0
        total_missing = 0
        total_gaps = 0
        report_paths = glob.glob(f"/store/emit/{env}/data/streams/{apid}/{date}/l0/*report.txt")
        report_paths.sort()
        files_with_gaps = []
        for p in report_paths:
            packet_count = 0
            missing_packets = 0
            psc_gaps = 0
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
                expected_total_packets = packet_count + missing_packets
                if expected_total_packets > 0:
                    percent_missing = (missing_packets / expected_total_packets) * 100
                else:
                    percent_missing = 0.0
                file_with_gaps = {
                    "file": p,
                    "packets_read": packet_count,
                    "missing_packets": missing_packets,
                    "psc_gaps": psc_gaps,
                    "expected_total_packets": expected_total_packets,
                    "percent_missing": f"{percent_missing:.2f}%"
                }
                files_with_gaps.append(file_with_gaps)
            total_packets_read += packet_count
            total_missing += missing_packets
            total_gaps += psc_gaps

        apid_report = OrderedDict()
        expected_total_packets = total_packets_read + total_missing
        if expected_total_packets > 0:
            percent_missing = (total_missing / expected_total_packets) * 100
        else:
            percent_missing = 0.0
        apid_report = {
            "apid": apid,
            "packets_read": total_packets_read,
            "missing_packets": total_missing,
            "psc_gaps": total_gaps,
            "expected_total_packets": expected_total_packets,
            "percent_missing": f"{percent_missing:.2f}%"
        }
        if len(files_with_gaps) > 0:
            apid_report["reports_showing_gaps"] = files_with_gaps

        report["streams"].append(apid_report)

    total_packets_read = 0
    total_missing = 0
    total_gaps = 0
    for apid in report["streams"]:
        total_packets_read += apid["packets_read"]
        total_missing += apid["missing_packets"]
        total_gaps += apid["psc_gaps"]

    expected_total_packets = total_packets_read + total_missing
    if expected_total_packets > 0:
        percent_missing = (total_missing / expected_total_packets) * 100
    else:
        percent_missing = 0.0
    stream_totals = {
        "packets_read": total_packets_read,
        "missing_packets": total_missing,
        "psc_gaps": total_gaps,
        "expected_total_packets": expected_total_packets,
        "percent_missing": f"{percent_missing:.2f}%"
    }
    report["stream_totals"] = stream_totals

    # Check depacketization
    report_paths = glob.glob(f"/store/emit/{env}/data/streams/1675/{date}/l1a/*report.txt")
    report_paths.sort()
    total_frames = 0
    total_corrupt = 0
    files_with_corrupt = []
    for p in report_paths:
        num_frames = 0
        corrupt_frames = 0
        with open(p, "r") as f:
            for line in f.readlines():
                if "Total Frames Read" in line:
                    num_frames = int(line.rstrip("\n").split(" ")[-1])
                if "Corrupt Frame Errors" in line:
                    corrupt_frames = int(line.rstrip("\n").split(" ")[-1])

        if corrupt_frames > 0:
            file_with_corrupt = OrderedDict()
            file_with_corrupt = {
                "file": p,
                "total_frames": num_frames,
                "corrupt_frames": corrupt_frames
            }
            files_with_corrupt.append(file_with_corrupt)

        total_frames += num_frames
        total_corrupt += corrupt_frames

    # Get frame times from log files
    log_paths = glob.glob(f"/store/emit/{env}/data/streams/1675/{date}/l1a/*pge.log")
    log_paths.sort()
    depack_frames = []
    if len(log_paths) > 0:
        with open(log_paths[0], "r") as f:
            for line in f.readlines():
                if " Writing frame to path" in line:
                    depack_frames.append(os.path.basename(line.split(" ")[8]))
        with open(log_paths[-1], "r") as f:
            for line in f.readlines():
                if " Writing frame to path" in line:
                    depack_frames.append(os.path.basename(line.split(" ")[8]))

    depacketization = OrderedDict()
    if len(depack_frames) > 0:
        depacketization = {
            "first_frame": depack_frames[0],
            "last_frame": depack_frames[-1],
            "total_frames": total_frames,
            "corrupt_frames": total_corrupt
        }
    else:
        depacketization = {
            "total_frames": total_frames,
            "corrupt_frames": total_corrupt
        }
    if len(files_with_corrupt) > 0:
        depacketization["reports_showing_corrupt_frames"] = files_with_corrupt

    report["frame_depacketization"] = depacketization

    # Check reassembly
    dcid_paths = glob.glob(f"/store/emit/{env}/data/data_collections/by_date/{date}/*")
    dcids = [os.path.basename(d).split("_")[1] for d in dcid_paths]
    dcids = list(set(dcids))
    dcids.sort()

    report_paths = []
    for dcid in dcids:
        match = glob.glob(f"/store/emit/{env}/data/data_collections/by_date/{date}/*_{dcid}/*decomp*/*allframesreport.txt")
        if len(match) > 0:
            report_paths.append(match[0])
    report_paths.sort()
    total_checks_fail = 0
    files_with_failures = []
    for p in report_paths:
        checks_fail = 0
        with open(p, "r") as f:
            for line in f.readlines():
                if "Check: FAIL" in line:
                    checks_fail += 1
                    files_with_failures.append(p)

        total_checks_fail += checks_fail

    reassembly = OrderedDict()
    reassembly = {
        "total_reassembled_dcids": len(report_paths),
        "all_frames_reports_with_failure": total_checks_fail
    }
    if len(files_with_failures) > 0:
        reassembly["reports_showing_frame_check_failures"] = files_with_failures

    report_paths = []
    for dcid in dcids:
        match = glob.glob(
            f"/store/emit/{env}/data/data_collections/by_date/{date}/*_{dcid}/*decomp*/*reassembly_report.txt")
        if len(match) > 0:
            report_paths.append(match[0])
    report_paths.sort()
    total_expected_frames = 0
    total_decompression_errors = 0
    total_missing_frames = 0
    total_corrupt_lines = 0
    total_cloudy_frames = 0
    files_with_errors = []
    for p in report_paths:
        expected_frames = 0
        decompression_errors = 0
        missing_frames = 0
        corrupt_lines = 0
        cloudy_frames = 0
        with open(p, "r") as f:
            for line in f.readlines():
                if "Total number of expected frames" in line:
                    expected_frames = int(line.rstrip("\n").split(" ")[-1])
                if "Total decompression errors" in line:
                    decompression_errors = int(line.rstrip("\n").split(" ")[-1])
                if "Total missing frames" in line:
                    missing_frames = int(line.rstrip("\n").split(" ")[-1])
                if "Total corrupt lines" in line:
                    corrupt_lines = int(line.rstrip("\n").split(" ")[-1])
                if "Total cloudy frames" in line:
                    cloudy_frames = int(line.rstrip("\n").split(" ")[-1])
        if decompression_errors > 0 or missing_frames > 0 or corrupt_lines > 0:
            file_with_errors = OrderedDict()
            file_with_errors = {
                "file": p,
                "total_expected_frames": expected_frames,
                "decompression_errors": decompression_errors,
                "missing_frames": missing_frames,
                "corrupt_lines": corrupt_lines
            }
            files_with_errors.append(file_with_errors)

        total_expected_frames += expected_frames
        total_decompression_errors += decompression_errors
        total_missing_frames += missing_frames
        total_corrupt_lines += corrupt_lines
        total_cloudy_frames += cloudy_frames

    reassembly["total_expected_frames"] = total_expected_frames
    reassembly["decompression_errors"] = total_decompression_errors
    reassembly["missing_frames"] = total_missing_frames
    reassembly["corrupt_lines"] = total_corrupt_lines
    reassembly["cloudy_frames"] = total_cloudy_frames

    if len(files_with_errors) > 0:
        reassembly["reports_showing_reassembly errors"] = files_with_errors

    report["reassembly"] = reassembly

    with open(report_path, "w") as f:
        yaml.dump(report, f, sort_keys=False)


if __name__ == '__main__':
    main()
