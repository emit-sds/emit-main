"""
A script that migrates files and database entries from one build version to another (prep for reprocessing)

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import logging
import os
import sys

# Files to migrate
# - CCSDS (bin, pge.log, report.txt)
# - L1A raw (img, hdr, line_timestamps, pge.log, rawqa, report.txt, waterfall)
# - Uncorrected att/eph

from emit_main.workflow.workflow_manager import WorkflowManager


def remove_keys_from_dict(keys, d):
    for k in keys:
        if k in d:
            d.pop(k)
    return d


def update_filename_versions(path, from_build, to_build, from_processing_version):
    return path.replace(f"_b{from_build}_", f"_b{to_build}_").replace(f"_v{from_processing_version}", "")


def add_migration_entry(metadata, from_config, to_config, from_build, to_build):
    migration_entry = {
        "timestamp": dt.datetime.now(tz=dt.timezone.utc),
        "from_config": from_config,
        "to_config": to_config,
        "from_build": from_build,
        "to_build": to_build
    }
    if "migration_history" in metadata:
        metadata["migration_history"].append(migration_entry)
    else:
        metadata["migration_history"] = [migration_entry]
    return metadata


def main():
    # Set up args
    parser = argparse.ArgumentParser(description="Migrate from one version to another")
    parser.add_argument("--from_config", help="Path to config file to migrate data FROM", required=True)
    parser.add_argument("--to_config", help="Path to config file to migrate data TO", required=True)
    parser.add_argument("--start_time", help="Start time - only migrate data after this time", required=True)
    parser.add_argument("--stop_time", help="Stop time - only migrate data before this time", required=True)
    parser.add_argument("--move_not_copy", action="store_true", help="Move the data instead of copying it")
    parser.add_argument("-l", "--level", default="INFO", help="Log level")
    parser.add_argument("--log_path", default="version_migration.log", help="Log level")
    args = parser.parse_args()

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("version-migration")

    # Set up file handler logging
    handler = logging.FileHandler(args.log_path)
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
    pge = from_wm.pges["emit-main"]

    if from_wm.config["build_num"] == to_wm.config["build_num"]:
        logger.error("Both from and to build numbers are the same. No need to migrate. Exiting...")
        sys.exit()

    # Migrate streams - 1675 CCSDS files and APID 1675 in DB
    from_streams = from_dm.find_streams_touching_date_range("1675", "start_time", start_time, stop_time)
    logger.info(f"Found {len(from_streams)} 1675 streams to migrate!")
    for from_stream in from_streams:
        if "ccsds_name" not in from_stream:
            logger.warning(f"- Skipped stream with hosc_name {from_stream['hosc_name']} because it had no CCSDS name")
            continue
        stream_name = from_stream["ccsds_name"]
        if to_dm.find_stream_by_named(stream_name):
            # Skip if already exists
            logger.info(f"- Skipped stream with name {stream_name}. Already exists.")
            continue

        # Prep metadata for migrated entry
        to_stream = remove_keys_from_dict(("_id", "created"), from_stream)
        if "products" in to_stream and "raw" in to_stream["products"]:
            to_stream["products"].pop("raw")
        if "products" in to_stream and "l1a" in to_stream["products"]:
            to_stream["products"].pop("l1a")
        to_stream["build_num"] = to_wm.config["build_num"]
        to_stream["processing_version"] = to_wm.config["processing_version"]
        to_stream["processing_log"] = []
        # Update products in DB and move/copy on filesystem
        if "products" in to_stream and "l0" in to_stream["products"]:
            try:
                from_path = from_stream["products"]["l0"]["ccsds_path"]
                to_path = update_filename_versions(from_path,
                                                   from_wm.config["build_num"],
                                                   to_wm.config["build_num"],
                                                   from_wm.config["processing_version"])
                # TODO: Copy in place?  If not, then need to create destination paths before copy or move
                # Since we are copying to a temporary folder, use tmp path for copy
                # tmp_to_path = to_path.replace(f"/{from_wm.config['environment']}/", f"/{to_wm.config['environment']}/")
                if args.move_not_copy:
                    from_wm.move(from_path, to_path)
                    logger.info(f"Moved file: {from_path} to {to_path}")
                else:
                    from_wm.copy(from_path, to_path)
                    logger.info(f"Copied file: {from_path} to {to_path}")

                to_stream["products"]["l0"]["ccsds_path"] = to_path
            except Exception as e:
                logger.warning(f"Issue while migrating L0 CCSDS product for stream {stream_name}")
                # TODO: Take out the continue?  Is it better to add the DB entry or fail it entirely?
                continue

        to_stream = add_migration_entry(to_stream, args.from_config, args.to_config, from_wm.config["build_num"],
                                        to_wm.config["build_num"])
        # Insert or update stream in DB
        # to_dm.insert_stream(stream_name, to_stream)
        logger.info(f"- Inserted stream in DB with {to_stream}")

    # Migrate data_collections in DB
    from_dcs = from_dm.find_data_collections_touching_date_range("planning_product.datetime", start_time, stop_time)
    logger.info(f"Found {len(from_dcs)} data collections to migrate!")
    for from_dc in from_dcs:
        dcid = from_dc["dcid"]
        if to_dm.find_data_collection_by_id(dcid):
            # Skip if already exists
            # to_dm.update_data_collection_metadata(dcid, to_dc)
            logger.info(f"- Skipped data collection with DCID {dcid}. Already exists.")
            continue

        # Prep metadata for migrated entry
        to_dc = remove_keys_from_dict(("_id", "created", "products"), from_dc)
        to_dc["build_num"] = to_wm.config["build_num"]
        to_dc["processing_version"] = to_wm.config["processing_version"]
        to_dc["processing_log"] = []
        # Update associated_ccsds paths to new build number and remove processing version from filename
        if "associated_ccsds" in to_dc:
            to_dc["associated_ccsds"] = [
                update_filename_versions(p, from_wm.config["build_num"], to_wm.config["build_num"],
                                         from_wm.config["processing_version"]) for p in to_dc["associated_ccsds"]]
        to_dc = add_migration_entry(to_dc, args.from_config, args.to_config, from_wm.config["build_num"],
                                    to_wm.config["build_num"])
        # to_dm.insert_data_collection(to_dc)
        logger.info(f"- Inserted data collection in DB with {to_dc}")

    # Migrate orbits - Uncorrected att/eph files and in DB
    from_orbits = from_dm.find_orbits_touching_date_range("start_time", start_time, stop_time)
    logger.info(f"Found {len(from_orbits)} orbits to migrate!")
    for from_orbit in from_orbits:
        orbit_id = from_orbit["orbit_id"]
        if to_dm.find_orbit_by_id(orbit_id):
            # Skip if already exists
            logger.info(f"- Skipped orbit with id {orbit_id}. Already exists.")
            continue

        # Prep metadata for migrated entry
        to_orbit = remove_keys_from_dict(("_id", "created", "bad_status", "associated_bad_sto"), from_orbit)
        if "products" in to_orbit and "l1b" in to_orbit["products"]:
            to_orbit["products"].pop("l1b")
        to_orbit["build_num"] = to_wm.config["build_num"]
        to_orbit["processing_version"] = to_wm.config["processing_version"]
        to_orbit["processing_log"] = []
        # Update associated_bad_netcdf path to new build number and remove processing version from filename
        if "associated_bad_netcdf" in to_orbit:
            to_orbit["associated_bad_netcdf"] = update_filename_versions(from_orbit["associated_bad_netcdf"],
                                                                         from_wm.config["build_num"],
                                                                         to_wm.config["build_num"],
                                                                         from_wm.config["processing_version"])
        # Update products in DB and move/copy on filesystem
        if "products" in to_orbit and "l1a" in to_orbit["products"]:
            try:
                from_path = from_orbit["products"]["l1a"]["uncorr_att_eph_path"]
                to_path = update_filename_versions(from_path,
                                                   from_wm.config["build_num"],
                                                   to_wm.config["build_num"],
                                                   from_wm.config["processing_version"])
                # TODO: Copy in place?  If not, then need to create destination paths before copy or move
                # Since we are copying to a temporary folder, use tmp path for copy
                # tmp_to_path = to_path.replace(f"/{from_wm.config['environment']}/", f"/{to_wm.config['environment']}/")
                if args.move_not_copy:
                    from_wm.move(from_path, to_path)
                    logger.info(f"Moved file: {from_path} to {to_path}")
                else:
                    from_wm.copy(from_path, to_path)
                    logger.info(f"Copied file: {from_path} to {to_path}")
                to_orbit["products"]["l1a"]["uncorr_att_eph_path"] = to_path
            except Exception as e:
                logger.warning(f"Issue while migrating L1A uncorrected att/eph product for orbit id {orbit_id}")
                # TODO: Take out the continue?  Is it better to add the DB entry or fail it entirely?
                continue

        to_orbit = add_migration_entry(to_orbit, args.from_config, args.to_config, from_wm.config["build_num"],
                                       to_wm.config["build_num"])
        # Insert or update orbit in DB
        # to_dm.insert_orbit(to_orbit)
        logger.info(f"- Inserted orbit in DB with {to_orbit}")

    # Migrate acquisitions
    from_acqs = from_dm.find_all_acquisitions_touching_date_range("start_time", start_time, stop_time)
    logger.info(f"Found {len(from_acqs)} acquisitions to migrate!")
    for from_acq in from_acqs:
        acquisition_id = from_acq["acquisition_id"]
        if to_dm.find_acquisition_by_id(acquisition_id):
            # Skip if already exists
            logger.info(f"- Skipped acquisition with id {acquisition_id}. Already exists.")
            continue

        # Prep metadata for migrated entry
        to_acq = remove_keys_from_dict(("_id", "created"), from_acq)
        if "products" in to_acq and "l1b" in to_acq["products"]:
            to_acq["products"].pop("l1b")
        if "products" in to_acq and "l2a" in to_acq["products"]:
            to_acq["products"].pop("l2a")
        if "products" in to_acq and "l2b" in to_acq["products"]:
            to_acq["products"].pop("l2b")
        to_acq["build_num"] = to_wm.config["build_num"]
        to_acq["processing_version"] = to_wm.config["processing_version"]
        to_acq["processing_log"] = []
       # Update products in DB and move/copy on filesystem
        if "products" in to_acq and "l1a" in to_acq["products"]:
            try:
                # First remove some of unneeded entries (frames)
                if "frames" in to_acq["products"]["l1a"]:
                    to_acq["products"]["l1a"].pop("frames")
                if "decompressed_frames" in to_acq["products"]["l1a"]:
                    to_acq["products"]["l1a"].pop("decompressed_frames")

                # TODO: Turn this into a function like this
                # TODO: to_path = migrate_paths_fs_and_db(from_orbit["products"]["l1a"]["uncorr_att_eph_path"])
                from_path = from_orbit["products"]["l1a"]["uncorr_att_eph_path"]
                to_path = update_filename_versions(from_path,
                                                   from_wm.config["build_num"],
                                                   to_wm.config["build_num"],
                                                   from_wm.config["processing_version"])
                # TODO: Copy in place?  If not, then need to create destination paths before copy or move
                # Since we are copying to a temporary folder, use tmp path for copy
                # tmp_to_path = to_path.replace(f"/{from_wm.config['environment']}/", f"/{to_wm.config['environment']}/")
                if args.move_not_copy:
                    from_wm.move(from_path, to_path)
                    logger.info(f"Moved file: {from_path} to {to_path}")
                else:
                    from_wm.copy(from_path, to_path)
                    logger.info(f"Copied file: {from_path} to {to_path}")
                to_orbit["products"]["l1a"]["uncorr_att_eph_path"] = to_path
            except Exception as e:
                logger.warning(f"Issue while migrating L1A uncorrected att/eph product for orbit id {orbit_id}")
                # TODO: Take out the continue?  Is it better to add the DB entry or fail it entirely?
                continue

        to_orbit = add_migration_entry(to_orbit, args.from_config, args.to_config, from_wm.config["build_num"],
                                       to_wm.config["build_num"])
        # Insert or update orbit in DB
        # to_dm.insert_orbit(to_orbit)
        logger.info(f"- Inserted orbit in DB with {to_orbit}")

if __name__ == '__main__':
    main()
