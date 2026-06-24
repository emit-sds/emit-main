"""
A script to read in the Flight Accountability Product (FAP)

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import glob
import json
import os


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Generate a daily report")
    parser.add_argument("-i", "--input_dir", help="Where to get FAP files")
    args = parser.parse_args()

    faps = glob.glob(os.path.join(args.input_dir, "*json"))
    faps.sort()
    dcids = set()
    acquired = set()
    processed = set()
    for fap in faps:
        if os.path.getsize(fap) > 0:
            with open(fap, "r") as f:
                fap_list = json.load(f)
                for i in fap_list:
                    dcid = i["dcid"]
                    if dcid in dcids:
                        print(f"DCID {dcid} has been seen before")
                    dcids.add(dcid)
                    if i["acquired"]:
                        acquired.add(dcid)
                    if i["processed"]:
                        processed.add(dcid)

    a_and_p = list(acquired.intersection(processed))
    a_and_p.sort()
    a_not_p = list(acquired - processed)
    a_not_p.sort()
    not_acquired = list(dcids - acquired)
    not_acquired.sort()
    dcids = list(dcids)
    dcids.sort()

    # print(f"DCIDS ({len(dcids)}):")
    # print(dcids)

    print(f"\nTOTAL DCIDS: {len(dcids)}")
    print(f"\nTOTAL ACQUIRED: {len(acquired)}")

    print(f"\nACQUIRED AND PROCESSED ({len(a_and_p)}):")
    print([str(d) for d in a_and_p])
    print(f"\nACQUIRED, NOT PROCESSED ({len(a_not_p)}):")
    print(([str(d) for d in a_not_p]))
    print(f"\nNOT ACQUIRED ({len(not_acquired)}):")
    print(([str(d) for d in not_acquired]))


if __name__ == '__main__':
    main()
