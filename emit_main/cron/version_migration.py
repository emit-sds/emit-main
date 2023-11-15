"""
A script that migrates files and database entries from one build version to another (prep for reprocessing)

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import logging
import os
import sys

from emit_main.workflow.workflow_manager import WorkflowManager


def remove_keys_from_dict(keys, d):
    for k in keys:
        if k in d:
            d.pop(k)
    return d


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Migrate from one version to another")
    parser.add_argument("--from_config", help="Path to config file to migrate data FROM", required=True)
    parser.add_argument("--to_config", help="Path to config file to migrate data TO", required=True)
    parser.add_argument("--start_time", help="Start time - only migrate data after this time", required=True)
    parser.add_argument("--stop_time", help="Stop time - only migrate data before this time", required=True)
    parser.add_argument("--move_not_copy", action="store_true", help="Move the data instead of copying it")
    parser.add_argument("-l", "--level", default="INFO", help="Log level")
    args = parser.parse_args()

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("version-migration")

    # Set up file handler logging
    handler = logging.FileHandler(f"version_migration.log")
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f"Executing version migration from {args.from_config} to {args.to_config} using range "
                f"{args.start_time} to {args.stop_time}")

    # Get start and stop dates
    start_time = dt.datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%S")
    stop_time = dt.datetime.strptime(args.stop_time, "%Y-%m-%dT%H:%M:%S")


    # Get workflow managers and database managers
    from_wm = WorkflowManager(config_path=args.from_config)
    to_wm = WorkflowManager(config_path=args.to_config)
    from_dm = from_wm.database_manager
    to_dm = to_wm.database_manager

    if from_wm.config["build_num"] == to_wm.config["build_num"]:
        logger.error("Both from and to build numbers are the same. No need to migrate. Exiting...")
        sys.exit()

    # Migrate data_collections in DB
    from_dcs = from_dm.find_data_collections_touching_date_range("planning_product.datetime", start_time, stop_time)
    logger.info(f"Found {len(from_dcs)} data collections to migrate!")
    for from_dc in from_dcs:
        # Prep metadata for migrated entry
        to_dc = remove_keys_from_dict(("_id", "created", "products"), from_dc)
        to_dc["build_num"] = to_wm.config["build_num"]
        to_dc["processing_version"] = to_wm.config["processing_version"]
        to_dc["processing_log"] = []

        # Insert or update data collection in DB
        dcid = to_dc["dcid"]
        if to_dm.find_data_collection_by_id(dcid):
            # TODO: Skip if already exists???
            # to_dm.update_data_collection_metadata(dcid, to_dc)
            logger.info(f"- Updated data collection in DB with {to_dc}")
        else:
            # to_dm.insert_data_collection(to_dc)
            logger.info(f"- Inserted data collection in DB with {to_dc}")

    # Migrate orbits

    # Migrate acquisitions


if __name__ == '__main__':
    main()
