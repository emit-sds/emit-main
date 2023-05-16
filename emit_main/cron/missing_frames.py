"""
A script that performs typical daily checks on data processing and prints out the results

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import os

from emit_main.workflow.workflow_manager import WorkflowManager


def lookup_depack_log(frame_path, dc, s_coll, wm):
    for path in dc["associated_ccsds"]:
        ccsds_name = os.path.basename(path)
        query = {
            "build_num": wm.config["build_num"],
            "ccsds_name": ccsds_name
        }
        stream = s_coll.find_one(query)
        s_frame_paths = stream["products"]["l1a"]["data_collections"][dc["dcid"]]["dcid_frame_paths"]
        if frame_path in s_frame_paths:
            # Get l1a name
            l1a_log = path.replace("/l0/", "/l1a/").replace("l0_ccsds", "l1a_frames").replace(".bin", "_pge.log")
            l1a_report = l1a_log.replace("_pge.log", "_report.txt")
            return l1a_report, l1a_log


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Find missing frames for DCID")
    parser.add_argument("dcid", help="The DCID to look up")
    parser.add_argument("-e", "--env", default="ops", help="Where to run the report")
    args = parser.parse_args()

    env = args.env

    # Get workflow manager and db collections
    config_path = f"/store/emit/{env}/repos/emit-main/emit_main/config/{env}_sds_config.json"
    wm = WorkflowManager(config_path=config_path)
    db = wm.database_manager.db
    dc_coll = db.data_collections
    s_coll = db.streams

    # Check for DCIDs with missing frames
    query = {
        "build_num": wm.config["build_num"],
        "dcid": args.dcid,
    }
    dc = dc_coll.find_one(query)

    frames = dc["products"]["l1a"]["frames"]
    frames.sort(key=lambda x: os.path.basename(x).split("_")[2])
    frame_nums = [int(os.path.basename(f).split("_")[2]) for f in frames]
    flookup = {}
    for i in range(len(frames)):
        flookup[frame_nums[i]] = frames[i]

    # Find duplicate frames
    total_frames = int(os.path.basename(frames[0]).split("_")[3])
    if len(frames) > total_frames:
        print(f"\nToo many frames. Expected {total_frames} and found {len(frames)}. Duplicates below:")

        # Get duplicates
        count_dict = {}
        for element in frame_nums:
            if element in count_dict:
                count_dict[element] += 1
            else:
                count_dict[element] = 1

        # Printing the duplicate elements
        duplicates = []
        for element, count in count_dict.items():
            if count > 1:
                duplicates.append(element)

        for dupe in duplicates:
            print(flookup[dupe])

    # Find missing frames
    seq_frame_nums = list(range(total_frames))
    missing_frame_nums = list(set(seq_frame_nums) - set(frame_nums))
    missing_frame_nums.sort()

    if len(missing_frame_nums) > 0:
        # Find the gap boundaries
        gaps = []
        start = 0
        stop = 0
        last = 0
        for i, num in enumerate(missing_frame_nums):
            if i == 0:
                start = num - 1
                last = num
                continue
            if num - last > 1:
                stop = last + 1
                gaps.append((start, stop))
                start = num - 1
            last = num

        gaps.append((start, missing_frame_nums[-1] + 1))

        print(f"\nFound missing frames. Expected {total_frames} and found {len(frames)}. Missing {len(missing_frame_nums)} nums are below:")
        print(missing_frame_nums)
        # print(gaps)

        print("\nFrames that bound each of the gaps:")
        for gap in gaps:
            print("-----------------------------------")
            if gap[0] < 0:
                print("\nBefore gap:")
                print(f"No prior frame")
            else:
                print(f"\nBefore gap:")
                print(f"{flookup[gap[0]]}")
                # Look up report file
                start_report, start_log = lookup_depack_log(flookup[gap[0]], dc, s_coll, wm)

            if gap[1] >= total_frames:
                print("After gap:")
                print(f"No subsequent frame")
            else:
                print(f"\nAfter gap:")
                print(f"{flookup[gap[1]]}")
                stop_report, stop_log = lookup_depack_log(flookup[gap[1]], dc, s_coll, wm)

            # Print reports
            print("\nDepacketization Report(s) (if more than one, then they correspond to frames before and after gap "
                  "respectively):")
            print(f"{start_report}")
            if stop_report != start_report:
                print(f"{stop_report}")
            print("\nDepacketization Log(s) (if more than one, then they correspond to frames before and after gap "
                  "respectively):")
            print(f"{start_log}")
            if stop_log != start_log:
                print(f"{stop_log}")

        print("-----------------------------------")


if __name__ == '__main__':
    main()
