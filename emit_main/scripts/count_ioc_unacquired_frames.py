"""
A script to count the IOC frames that were not acquired at the end of each acquisition

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import os


def main():

    frame_dirs = glob.glob("/store/emit/ops/data/data_collections/by_date/2022*/*/*frames*/")
    frame_dirs.sort()
    total_num_missing = 0
    dates = {}
    for dir in frame_dirs:
        date = dir.split("/")[7]
        frames = glob.glob(os.path.join(dir, "*1"))
        frames.sort(key=lambda x: os.path.basename(x).split("_")[2])
        last_frame = int(os.path.basename(frames[-1]).split("_")[2])
        total_frames = int(os.path.basename(frames[-1]).split("_")[3])
        num_missing = total_frames - last_frame - 1
        if num_missing > 0:
            print(f"{dir}: {num_missing}")
            if date in dates:
                dates[date] += num_missing
            else:
                dates[date] = num_missing
            total_num_missing += num_missing

    print(f"Total num missing: {total_num_missing}")
    print(dates)


if __name__ == '__main__':
    main()
