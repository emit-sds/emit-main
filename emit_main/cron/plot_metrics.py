"""
A script to plot metrics from the daily reports

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import glob
# import matplotlib.pyplot as plt
import os
import subprocess
import sys
import yaml

import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-r", "--reports_dir", default="/store/emit/ops/reports", help="Where to get report files")
    parser.add_argument("--options")
    parser.add_argument("-d", "--date", help="Date (YYYYMMDD)")
    parser.add_argument("-o", "--output", help="Output report path (use .yml extension)")
    args = parser.parse_args()

    option_choices = ("streams", "1674", "1675", "1676", "reassembly")

    # Plot daily reports
    daily_reports  = glob.glob(os.path.join(args.reports_dir, "daily*yml"))
    daily_reports.sort()
    dates = [dt.datetime.strptime(os.path.basename(p).split("_")[1].replace(".yml", ""), "%Y%m%d") for p in daily_reports]
    # dates = [os.path.basename(p).split("_").replace(".yml", "") for p in daily_reports]
    metrics = []
    for report in daily_reports:
        with open(report, "r") as f:
            metrics.append(yaml.load(f, Loader=yaml.FullLoader))

    dates = [d.strftime("%m/%d") for d in dates]

    # Depacketization totals
    packets = [int(m["stream_totals"]["packets_read"]) for m in metrics]
    gaps = [int(m["stream_totals"]["psc_gaps"]) for m in metrics]
    missing = [int(m["stream_totals"]["missing_packets"]) for m in metrics]
    percents = [float(m["stream_totals"]["percent_missing"].replace("%", "")) for m in metrics]

    frames = [int(m["frame_depacketization"]["total_frames"]) for m in metrics]
    corrupt_frames = [int(m["frame_depacketization"]["corrupt_frames"]) for m in metrics]

    if "streams" in args.options:
        plt.rcParams['figure.figsize'] = [10, 10]

        plt.subplot(4, 1, 1)
        plt.bar(dates, packets)
        plt.title("Packets Read")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(4, 1, 2)
        plt.bar(dates, gaps)
        plt.title("PSC Gaps")
        # for i in range(len(dates)):
        #     plt.annotate(gaps[i], xy=(dates[i], gaps[i]))
        plt.ylim([0, 30])
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(4, 1, 3)
        plt.bar(dates, missing)
        plt.title("Missing Packets")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(4, 1, 4)
        plt.bar(dates, percents)
        plt.title("Percent Missing")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplots_adjust(hspace=1)
        plt.suptitle(f"All APIDs", fontsize=12)
        plt.show()

    # APID Specific
    for i, apid in enumerate(["1674", "1675", "1676"]):
        packets = [int(m["streams"][i]["packets_read"]) for m in metrics]
        gaps = [int(m["streams"][i]["psc_gaps"]) for m in metrics]
        missing = [int(m["streams"][i]["missing_packets"]) for m in metrics]
        percents = [float(m["streams"][i]["percent_missing"].replace("%", "")) for m in metrics]

        rows = 6 if apid == "1675" else 4

        if apid in args.options:
            plt.rcParams['figure.figsize'] = [10, 10]

            plt.subplot(rows, 1, 1)
            plt.bar(dates, packets)
            plt.title("Packets Read")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 2)
            plt.bar(dates, gaps)
            plt.title("PSC Gaps")
            # for i in range(len(dates)):
            #     plt.annotate(gaps[i], xy=(dates[i], gaps[i]))
            plt.ylim([0, 30])
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 3)
            plt.bar(dates, missing)
            plt.title("Missing Packets")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 4)
            plt.bar(dates, percents)
            plt.title("Percent Missing")
            plt.xticks(rotation=45, fontsize=8)

            if apid == "1675":
                plt.subplot(6, 1, 5)
                plt.bar(dates, frames)
                plt.title("Depacketized Frames")
                plt.xticks(rotation=45, fontsize=8)

                plt.subplot(6, 1, 6)
                plt.bar(dates, corrupt_frames)
                plt.title("Corrupt Frames")
                plt.xticks(rotation=45, fontsize=8)

            plt.subplots_adjust(hspace=1)
            plt.suptitle(f"APID {apid}", fontsize=12)
            plt.show()

    # Reassembly totals
    dcids = [int(m["reassembly"]["total_reassembled_dcids"]) for m in metrics]
    expected_frames = [int(m["reassembly"]["total_expected_frames"]) for m in metrics]
    missing_frames = [int(m["reassembly"]["missing_frames"]) for m in metrics]
    decompression_errors = [int(m["reassembly"]["decompression_errors"]) for m in metrics]
    cloudy = [int(m["reassembly"]["cloudy_frames"]) for m in metrics]
    corrupt_lines = [int(m["reassembly"]["corrupt_lines"]) for m in metrics]

    percent_cloudy = []
    for i in range(len(cloudy)):
        if expected_frames[i] - missing_frames[i] > 0:
            percent = (cloudy[i] / (expected_frames[i] - missing_frames[i])) * 100
        else:
            percent = 0.0
        percent_cloudy.append(percent)

    if "reassembly" in args.options:
        plt.rcParams['figure.figsize'] = [10, 14]

        plt.subplot(7, 1, 1)
        plt.bar(dates, dcids)
        plt.title("Reassembled DCIDs")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(7, 1, 2)
        plt.bar(dates, expected_frames)
        plt.title("Expected Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(7, 1, 3)
        plt.bar(dates, missing_frames)
        plt.title("Missing Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(7, 1, 4)
        plt.bar(dates, decompression_errors)
        plt.title("Decompression Errors")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(7, 1, 5)
        plt.bar(dates, cloudy)
        plt.title("Cloudy Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(7, 1, 6)
        plt.bar(dates, percent_cloudy)
        plt.title("Percent Cloudy")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(7, 1, 7)
        plt.bar(dates, corrupt_lines)
        plt.title("Corrupt Lines")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplots_adjust(hspace=1.4)
        plt.suptitle("Reassembly", fontsize=12)
        plt.show()


if __name__ == '__main__':
    main()
