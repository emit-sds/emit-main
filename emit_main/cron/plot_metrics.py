"""
A script to plot metrics from the daily reports

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import csv
import datetime as dt
import glob
import matplotlib
import os
import subprocess
import sys
import yaml

matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-i", "--input_dir", default="/store/emit/ops/reports", help="Where to get report files")
    parser.add_argument("-p", "--plots", default="1675,reassembly",
                        help="Comma separate choices: streams,1674,1675,1676,reassembly")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("-o", "--output_dir", default="/store/emit/ops/reports/trending",
                        help="Output dir for plots and csvs")
    parser.add_argument("--timestamp", action="store_true")
    parser.add_argument("--show_plots", action="store_true")
    args = parser.parse_args()

    time_now = dt.datetime.now()
    time_now_str = time_now.strftime("%Y%m%dT%H%M%S")

    # Plot daily reports
    daily_reports = glob.glob(os.path.join(args.input_dir, "daily*yml"))
    daily_reports.sort()
    filtered_reports = []
    if args.dates is not None:
        start, stop = args.dates.split(",")
        start_name = f"daily_{start}.yml"
        stop_name = f"daily_{stop}.yml"
        for report in daily_reports:
            report_name = os.path.basename(report)
            if start_name <= report_name <= stop_name:
                filtered_reports.append(report)
    else:
        filtered_reports = daily_reports
    filtered_reports.sort()
    dates = [dt.datetime.strptime(os.path.basename(p).split("_")[1].replace(".yml", ""), "%Y%m%d") for p in filtered_reports]
    dates.sort()
    # dates = [os.path.basename(p).split("_").replace(".yml", "") for p in daily_reports]
    metrics = []
    for report in filtered_reports:
        with open(report, "r") as f:
            metrics.append(yaml.load(f, Loader=yaml.FullLoader))

    dates_str = [d.strftime("%m/%d") for d in dates]
    output_file_dates = f"{dates[0].strftime('%Y%m%d')}_{dates[-1].strftime('%Y%m%d')}"
    if args.timestamp:
        output_file_dates += output_file_dates + f"_{time_now_str}"

    # Depacketization totals
    packets = [int(m["stream_totals"]["packets_read"]) for m in metrics]
    gaps = [int(m["stream_totals"]["psc_gaps"]) for m in metrics]
    missing = [int(m["stream_totals"]["missing_packets"]) for m in metrics]
    percents = [float(m["stream_totals"]["percent_missing"].replace("%", "")) for m in metrics]

    frames = [int(m["frame_depacketization"]["total_frames"]) for m in metrics]
    corrupt_frames = [int(m["frame_depacketization"]["corrupt_frames"]) for m in metrics]

    first_timestamps = []
    last_timestamps = []
    for m in metrics:
        if "first_frame" in m["frame_depacketization"]:
            first_timestamps.append(dt.datetime.strptime(m["frame_depacketization"]["first_frame"].split("_")[1],
                                                         "%Y%m%dt%H%M%S").strftime("%Y-%m-%dT%H:%M:%S"))
            last_timestamps.append(dt.datetime.strptime(m["frame_depacketization"]["last_frame"].split("_")[1],
                                                        "%Y%m%dt%H%M%S").strftime("%Y-%m-%dT%H:%M:%S"))
        else:
            first_timestamps.append(None)
            last_timestamps.append(None)

    if "streams" in args.plots:
        # Create CSV too
        with open(os.path.join(args.output_dir, f"all_streams_{output_file_dates}.csv"), 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["dates", "packets_read", "psc_gaps", "missing_packets", "percent_missing"])
            for i in range(len(dates)):
                csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), packets[i], gaps[i], missing[i], percents[i]])

        plt.rcParams['figure.figsize'] = [10, 10]

        plt.subplot(4, 1, 1)
        plt.bar(dates_str, packets)
        plt.title("Packets Read")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(4, 1, 2)
        plt.bar(dates_str, gaps)
        plt.title("PSC Gaps")
        # for i in range(len(dates)):
        #     plt.annotate(gaps[i], xy=(dates[i], gaps[i]))
        # plt.ylim([0, 30])
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(4, 1, 3)
        plt.bar(dates_str, missing)
        plt.title("Missing Packets")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(4, 1, 4)
        plt.bar(dates_str, percents)
        plt.title("Percent Missing")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplots_adjust(hspace=1)
        plt.suptitle(f"All APIDs", fontsize=12)
        plt.savefig(os.path.join(args.output_dir, f"all_streams_{output_file_dates}.png"))
        if args.show_plots:
            plt.show()

    # APID Specific
    for i, apid in enumerate(["1674", "1675", "1676"]):
        packets = [int(m["streams"][i]["packets_read"]) for m in metrics]
        gaps = [int(m["streams"][i]["psc_gaps"]) for m in metrics]
        missing = [int(m["streams"][i]["missing_packets"]) for m in metrics]
        percents = [float(m["streams"][i]["percent_missing"].replace("%", "")) for m in metrics]

        rows = 6 if apid == "1675" else 4

        if apid in args.plots:
            # Create CSV too
            with open(os.path.join(args.output_dir, f"{apid}_{output_file_dates}.csv"), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                if apid == "1675":
                    csvwriter.writerow(["dates", "packets_read", "psc_gaps", "missing_packets", "percent_missing",
                                       "depacketized_frames", "corrupt_frames", "first_frame_time", "last_frame_time"])
                    for i in range(len(dates)):
                        csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), packets[i], gaps[i], missing[i],
                                            percents[i], frames[i], corrupt_frames[i], first_timestamps[i],
                                            last_timestamps[i]])
                else:
                    csvwriter.writerow(["dates", "packets_read", "psc_gaps", "missing_packets", "percent_missing"])
                    for i in range(len(dates)):
                        csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), packets[i], gaps[i], missing[i],
                                            percents[i]])

            plt.rcParams['figure.figsize'] = [10, 10]

            plt.subplot(rows, 1, 1)
            plt.bar(dates_str, packets)
            plt.title("Packets Read")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 2)
            plt.bar(dates_str, gaps)
            plt.title("PSC Gaps")
            # for i in range(len(dates)):
            #     plt.annotate(gaps[i], xy=(dates[i], gaps[i]))
            # plt.ylim([0, 30])
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 3)
            plt.bar(dates_str, missing)
            plt.title("Missing Packets")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 4)
            plt.bar(dates_str, percents)
            plt.title("Percent Missing")
            plt.xticks(rotation=45, fontsize=8)

            if apid == "1675":
                plt.subplot(6, 1, 5)
                plt.bar(dates_str, frames)
                plt.title("Depacketized Frames")
                plt.xticks(rotation=45, fontsize=8)

                plt.subplot(6, 1, 6)
                plt.bar(dates_str, corrupt_frames)
                plt.title("Corrupt Frames")
                plt.xticks(rotation=45, fontsize=8)

            plt.subplots_adjust(hspace=1)
            plt.suptitle(f"APID {apid}", fontsize=12)
            plt.savefig(os.path.join(args.output_dir, f"{apid}_{output_file_dates}.png"))
            if args.show_plots:
                plt.show()

    # Reassembly totals
    dcids = [int(m["reassembly"]["total_reassembled_dcids"]) for m in metrics]
    expected_frames = [int(m["reassembly"]["total_expected_frames"]) for m in metrics]
    missing_frames = [int(m["reassembly"]["missing_frames"]) for m in metrics]
    decompression_errors = [int(m["reassembly"]["decompression_errors"]) for m in metrics]
    cloudy = [int(m["reassembly"]["cloudy_frames"]) for m in metrics]
    corrupt_lines = [int(m["reassembly"]["corrupt_lines"]) for m in metrics]

    percent_missing = []
    for i in range(len(expected_frames)):
        if expected_frames[i] > 0:
            percent = ((missing_frames[i] + decompression_errors[i]) / expected_frames[i]) * 100
        else:
            percent = 0.0
        percent_missing.append(percent)

    percent_cloudy = []
    for i in range(len(cloudy)):
        if expected_frames[i] - missing_frames[i] > 0:
            percent = (cloudy[i] / (expected_frames[i] - missing_frames[i])) * 100
        else:
            percent = 0.0
        percent_cloudy.append(percent)

    if "reassembly" in args.plots:
        # Create CSV too
        with open(os.path.join(args.output_dir, f"reassembly_{output_file_dates}.csv"), 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["dates", "dcids", "expected_frames", "missing_frames", "decompression_errors",
                                "percent_missing", "cloudy_frames", "percent_cloudy", "corrupt_lines"])
            for i in range(len(dates)):
                csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), dcids[i], expected_frames[i], missing_frames[i],
                                    decompression_errors[i], percent_missing[i], cloudy[i], f"{percent_cloudy[i]:.2f}",
                                    corrupt_lines[i]])

        plt.rcParams['figure.figsize'] = [10, 14]

        plt.subplot(8, 1, 1)
        plt.bar(dates_str, dcids)
        plt.title("Reassembled DCIDs")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 2)
        plt.bar(dates_str, expected_frames)
        plt.title("Expected Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 3)
        plt.bar(dates_str, missing_frames)
        plt.title("Missing Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 4)
        plt.bar(dates_str, decompression_errors)
        plt.title("Frames with Errors (Corrupt Frames or Decompression Errors)")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 5)
        plt.bar(dates_str, percent_missing)
        plt.title("Percent of Frames Missing or Containing Errors")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 6)
        plt.bar(dates_str, cloudy)
        plt.title("Cloudy Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 7)
        plt.bar(dates_str, percent_cloudy)
        plt.title("Percent Cloudy")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(8, 1, 8)
        plt.bar(dates_str, corrupt_lines)
        plt.title("Corrupt Lines")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplots_adjust(hspace=1.4)
        plt.suptitle("Reassembly", fontsize=12)
        plt.savefig(os.path.join(args.output_dir, f"reassembly_{output_file_dates}.png"))
        if args.show_plots:
            plt.show()


if __name__ == '__main__':
    main()
