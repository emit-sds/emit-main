"""
A script to plot metrics from the daily reports

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import csv
import datetime as dt
import glob
import json
import matplotlib
import os
import subprocess
import sys
import yaml

matplotlib.use('Agg')
import matplotlib.pyplot as plt


def calc_cloud_metrics(input_json):
    with open(input_json, "r") as f:
        scenes = json.load(f)["features"]

    cloud_fraction = 0
    cloud_cirrus_fraction = 0
    clouds_buffer_fraction = 0
    screened_onboard_fraction = 0
    total_cloud_fraction = 0
    total_scenes_with_stats = 0
    above = 0
    for s in scenes:
        if "Cloud Fraction" in s["properties"]:
            cloud_fraction += s["properties"]["Cloud Fraction"]
            cloud_cirrus_fraction += s["properties"]["Cloud + Cirrus Fraction"]
            clouds_buffer_fraction += s["properties"]["Clouds & Buffer Fraction"]
            screened_onboard_fraction += s["properties"]["Screened Onboard Fraction"]
            total_cloud_fraction += s["properties"]["Total Cloud Fraction"]
            total_scenes_with_stats += 1

    # Calculate average
    cloud_percent = cloud_fraction / total_scenes_with_stats * 100
    cloud_cirrus_percent = cloud_cirrus_fraction / total_scenes_with_stats * 100
    clouds_buffer_percent = clouds_buffer_fraction / total_scenes_with_stats * 100
    screened_onboard_percent = screened_onboard_fraction / total_scenes_with_stats * 100
    total_cloud_percent = total_cloud_fraction / total_scenes_with_stats * 100

    return cloud_percent, cloud_cirrus_percent, clouds_buffer_percent, screened_onboard_percent, \
        total_cloud_percent



def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-i", "--input_dir", default="/store/emit/ops/reports", help="Where to get report files")
    parser.add_argument("-p", "--plots", default="1675,reassembly",
                        help="Comma separate choices: streams,1674,1675,1676,reassembly,status")
    parser.add_argument("-d", "--dates", help="Comma separated dates (YYYYMMDD,YYYYMMDD)")
    parser.add_argument("-o", "--output_dir", default="/store/emit/ops/reports/trending",
                        help="Output dir for plots and csvs")
    parser.add_argument("--tracking_json", default="/beegfs/scratch/brodrick/emit/emit-visuals/track_coverage.json",
                        help="Path to the tracking json file with cloud metrics")
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
    total_packets = sum(packets)
    gaps = [int(m["stream_totals"]["psc_gaps"]) for m in metrics]
    missing = [int(m["stream_totals"]["missing_packets"]) for m in metrics]
    total_missing_packets = sum(missing)
    percents = [float(m["stream_totals"]["percent_missing"].replace("%", "")) for m in metrics]
    duplicates = [int(m["stream_totals"]["duplicate_packets"]) for m in metrics]

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
            csvwriter.writerow(["dates", "packets_read", "duplicate_packets", "psc_gaps", "missing_packets",
                                "percent_missing"])
            for i in range(len(dates)):
                csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), packets[i], duplicates[i], gaps[i], missing[i],
                                    percents[i]])

        plt.rcParams['figure.figsize'] = [10, 12]

        plt.subplot(5, 1, 1)
        plt.bar(dates_str, packets)
        plt.title("Packets Read")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(5, 1, 2)
        plt.bar(dates_str, duplicates)
        plt.title("Duplicate Packets")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(5, 1, 3)
        plt.bar(dates_str, gaps)
        plt.title("PSC Gaps")
        # for i in range(len(dates)):
        #     plt.annotate(gaps[i], xy=(dates[i], gaps[i]))
        # plt.ylim([0, 30])
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(5, 1, 4)
        plt.bar(dates_str, missing)
        plt.title("Missing Packets")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(5, 1, 5)
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
        duplicates = [int(m["streams"][i]["duplicate_packets"]) for m in metrics]

        rows = 7 if apid == "1675" else 5

        if apid in args.plots:
            # Create CSV too
            with open(os.path.join(args.output_dir, f"{apid}_{output_file_dates}.csv"), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                if apid == "1675":
                    csvwriter.writerow(["dates", "packets_read", "duplicate_packets", "psc_gaps", "missing_packets",
                                        "percent_missing", "depacketized_frames", "corrupt_frames", "first_frame_time",
                                        "last_frame_time"])
                    for i in range(len(dates)):
                        csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), packets[i], duplicates[i], gaps[i],
                                            missing[i], percents[i], frames[i], corrupt_frames[i], first_timestamps[i],
                                            last_timestamps[i]])
                else:
                    csvwriter.writerow(["dates", "packets_read", "duplicate_packets", "psc_gaps", "missing_packets",
                                        "percent_missing"])
                    for i in range(len(dates)):
                        csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), packets[i], duplicates[i], gaps[i],
                                            missing[i], percents[i]])

            plt.rcParams['figure.figsize'] = [10, 12]

            plt.subplot(rows, 1, 1)
            plt.bar(dates_str, packets)
            plt.title("Packets Read")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 2)
            plt.bar(dates_str, duplicates)
            plt.title("Duplicate Packets")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 3)
            plt.bar(dates_str, gaps)
            plt.title("PSC Gaps")
            # for i in range(len(dates)):
            #     plt.annotate(gaps[i], xy=(dates[i], gaps[i]))
            # plt.ylim([0, 30])
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 4)
            plt.bar(dates_str, missing)
            plt.title("Missing Packets")
            plt.xticks(rotation=45, fontsize=8)

            plt.subplot(rows, 1, 5)
            plt.bar(dates_str, percents)
            plt.title("Percent Missing")
            plt.xticks(rotation=45, fontsize=8)

            if apid == "1675":
                plt.subplot(rows, 1, 6)
                plt.bar(dates_str, frames)
                plt.title("Depacketized Frames")
                plt.xticks(rotation=45, fontsize=8)

                plt.subplot(rows, 1, 7)
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
    corrupt_frames = [int(m["reassembly"]["corrupt_frames"]) for m in metrics]
    decompression_errors = [int(m["reassembly"]["decompression_errors"]) for m in metrics]
    cloudy = [int(m["reassembly"]["cloudy_frames"]) for m in metrics]
    corrupt_lines = [int(m["reassembly"]["corrupt_lines"]) for m in metrics]

    percent_missing = []
    for i in range(len(expected_frames)):
        if expected_frames[i] > 0:
            percent = ((missing_frames[i] + corrupt_frames[i] + decompression_errors[i]) / expected_frames[i]) * 100
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
            csvwriter.writerow(["dates", "dcids", "expected_frames", "missing_frames", "corrupt_frames",
                                "decompression_errors", "percent_missing", "cloudy_frames", "percent_cloudy",
                                "corrupt_lines"])
            for i in range(len(dates)):
                csvwriter.writerow([dates[i].strftime("%Y-%m-%d"), dcids[i], expected_frames[i], missing_frames[i],
                                    corrupt_frames[i], decompression_errors[i], percent_missing[i], cloudy[i],
                                    f"{percent_cloudy[i]:.2f}", corrupt_lines[i]])

        plt.rcParams['figure.figsize'] = [10, 18]

        plt.subplot(9, 1, 1)
        plt.bar(dates_str, dcids)
        plt.title("Reassembled DCIDs")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 2)
        plt.bar(dates_str, expected_frames)
        plt.title("Expected Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 3)
        plt.bar(dates_str, missing_frames)
        plt.title("Missing Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 4)
        plt.bar(dates_str, corrupt_frames)
        plt.title("Corrupt Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 5)
        plt.bar(dates_str, decompression_errors)
        plt.title("Frames with Decompression Errors")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 6)
        plt.bar(dates_str, percent_missing)
        plt.title("Percent of Frames Missing, Corrupt, or Containing Decompression Errors")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 7)
        plt.bar(dates_str, cloudy)
        plt.title("Cloudy Frames")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 8)
        plt.bar(dates_str, percent_cloudy)
        plt.title("Percent Cloudy")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplot(9, 1, 9)
        plt.bar(dates_str, corrupt_lines)
        plt.title("Corrupt Lines")
        plt.xticks(rotation=45, fontsize=8)

        plt.subplots_adjust(hspace=1.5)
        plt.suptitle("Reassembly", fontsize=12)
        plt.savefig(os.path.join(args.output_dir, f"reassembly_{output_file_dates}.png"))
        if args.show_plots:
            plt.show()

    # Calculate totals and produce a status report
    if "status" in args.plots:
        total_dcids = sum(dcids)
        total_cloudy = sum(cloudy)
        total_expected = sum(expected_frames)
        total_missing_frames = sum(missing_frames)
        total_corrupt = sum(corrupt_frames)
        percent_clouds_screened = total_cloudy / (total_expected - total_missing_frames) * 100
        eff_missing_clouds = total_cloudy / 40
        # 1486 is the number of missing frames due to IOC timing issues
        MISSING_FRAMES_IOC_TIMING = 1486
        # 1343 is the number missing frames due to H/S overflow
        MISSING_FRAMES_HS_OVERFLOW = 1343
        eff_missing_pkt_loss = (total_missing_frames + total_corrupt - MISSING_FRAMES_IOC_TIMING - MISSING_FRAMES_HS_OVERFLOW) / 40
        eff_missing_hs_overflow = MISSING_FRAMES_HS_OVERFLOW / 40
        eff_missing_ioc_timing = MISSING_FRAMES_IOC_TIMING / 40
        total_eff_missing = eff_missing_clouds + eff_missing_pkt_loss + eff_missing_hs_overflow + eff_missing_ioc_timing
        cloud_percent, cloud_cirrus_percent, clouds_buffer_percent, screened_onboard_percent, total_cloud_percent = calc_cloud_metrics(args.tracking_json)

        print(f"""
Percent clouds screened on orbit: {percent_clouds_screened:.1f}%
Percent clouds in scenes: {cloud_percent:.1f}%
Percent clouds + cirrus in scenes: {cloud_cirrus_percent:.1f}%
Percent clouds + cirrus + buffer in scenes: {clouds_buffer_percent:.1f}%
Effective number of missing scenes: {total_eff_missing:.1f}
Due to cloud screening: {eff_missing_clouds:.1f}
Due to packet loss: {eff_missing_pkt_loss:.1f}
Due to high-speed buffer overflow: {eff_missing_hs_overflow:.1f}
Due to acquisition timing issues during IOC: {eff_missing_ioc_timing:.1f}

Total orbit segments: {total_dcids}
Total frames: {total_expected}
Total missing frames: {total_cloudy + total_missing_frames + total_corrupt}
Due to cloud screening: {total_cloudy}
Due to packet loss: {total_missing_frames + total_corrupt - MISSING_FRAMES_HS_OVERFLOW - MISSING_FRAMES_IOC_TIMING}
Due to high-speed buffer overflow: {MISSING_FRAMES_HS_OVERFLOW}
Due to acquisition timing issues during IOC: {MISSING_FRAMES_IOC_TIMING}

Total packets read: {total_packets}
Total missing packets: {total_missing_packets}
Percent missing packets: {total_missing_packets / (total_packets + total_missing_packets) * 100:.3f}

""")


if __name__ == '__main__':
    main()
