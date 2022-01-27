"""
This code contains tasks for executing EMIT Level 0 PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import luigi
import os

from emit_main.workflow.output_targets import StreamTarget
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_utils import daac_converter

logger = logging.getLogger("emit-main")


class L0StripHOSC(SlurmJobTask):
    """
    Strips HOSC ethernet headers from raw data in apid-specific ingest folder
    :returns Ordered APID specific packet stream
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    miss_pkt_thresh = luigi.FloatParameter(default=0.1)

    task_namespace = "emit"

    def requires(self):
        logger.debug(f"{self.task_family} requires: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        if wm.stream is None:
            # Insert new stream in db
            dm = wm.database_manager
            dm.insert_hosc_stream(os.path.basename(self.stream_path))
        return None

    def output(self):
        logger.debug(f"{self.task_family} output: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):
        logger.debug(f"{self.task_family} work: {self.stream_path}")

        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream
        pge = wm.pges["emit-sds-l0"]

        # Build command and run
        sds_l0_exe = os.path.join(pge.repo_dir, "run_l0.sh")
        sds_packet_count_exe = os.path.join(pge.repo_dir, "packet_cnt_check.py")
        ios_l0_proc = os.path.join(wm.pges["emit-l0edp"].repo_dir, "target", "release", "emit_l0_proc")
        # Create input dir and copy stream file into dir
        tmp_input_dir = os.path.join(self.local_tmp_dir, "input")
        wm.makedirs(tmp_input_dir)
        tmp_input_path = os.path.join(tmp_input_dir, os.path.basename(self.stream_path))
        wm.copy(self.stream_path, tmp_input_path)
        # Create output dir and log file name
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        l0_pge_log_name = stream.hosc_name.replace(".bin", "_l0_pge.log")
        tmp_log = os.path.join(tmp_output_dir, l0_pge_log_name)
        # Set up command
        cmd = [sds_l0_exe, tmp_input_dir, tmp_output_dir, tmp_log, sds_packet_count_exe, ios_l0_proc]
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Get tmp ccsds and log names
        tmp_ccsds_path = glob.glob(os.path.join(tmp_output_dir, stream.apid + "*.bin"))[0]
        tmp_report_path = glob.glob(os.path.join(tmp_output_dir, stream.apid + "*_report.txt"))[0]

        # Check report file to see if missing PSCs exceed threshold
        packet_count = 0
        missing_packets = 0
        with open(tmp_report_path, "r") as f:
            for line in f.readlines():
                if "Packet Count" in line:
                    packet_count = int(line.rstrip("\n").split(" ")[-1])
                if "Missing PSC Count" in line:
                    missing_packets = int(line.rstrip("\n").split(" ")[-1])
        miss_pkt_percent = missing_packets / packet_count
        if missing_packets / packet_count >= self.miss_pkt_thresh:
            raise RuntimeError(f"Missing {missing_packets} packets out of {packet_count} total is greater than the "
                               f"missing packet threshold of {self.miss_pkt_thresh}")

        # TODO: Add check for "File Size Match" in PGE
        # TODO: Replace start/stop with CCSDS start and stop
        # Set up command to get CCSDS start/stop times
        # get_start_stop_exe = os.path.join(pge.repo_dir, "get_ccsds_start_stop_times.py")
        # start_stop_json = tmp_ccsds_path.replace(".bin", ".json")
        # cmd_timing = ["python", get_start_stop_exe, tmp_ccsds_path, start_stop_json]
        # pge.run(cmd_timing, tmp_dir=self.tmp_dir, env=env)

        # Get CCSDS start time and file name and report name
        tmp_ccsds_name = os.path.basename(tmp_ccsds_path)
        ccsds_start_time_str = tmp_ccsds_name.split("_")[1]
        ccsds_start_time = datetime.datetime.strptime(ccsds_start_time_str, "%Y-%m-%dT%H:%M:%S")
        ccsds_name = "_".join([
            wm.config["instrument"],
            stream.apid,
            ccsds_start_time.strftime("%Y%m%dt%H%M%S"),
            "l0",
            "ccsds",
            "b" + wm.config["build_num"],
            "v" + wm.config["processing_version"]
        ]) + ".bin"
        ccsds_path = os.path.join(stream.l0_dir, ccsds_name)
        report_path = ccsds_path.replace(".bin", "_report.txt")

        # Copy scratch CCSDS file and report back to store
        wm.copy(tmp_ccsds_path, ccsds_path)
        wm.copy(tmp_report_path, report_path)

        # Copy and rename log file
        log_path = ccsds_path.replace(".bin", "_pge.log")
        wm.copy(tmp_log, log_path)

        if "ingest" in self.stream_path:
            # Move HOSC file out of ingest folder
            wm.move(self.stream_path, stream.hosc_path)

        # Update DB
        metadata = {
            "ccsds_name": ccsds_name,
            "ccsds_start_time": ccsds_start_time,
            "products": {
                "raw": {
                    "hosc_path": stream.hosc_path,
                    "created": datetime.datetime.fromtimestamp(
                        os.path.getmtime(stream.hosc_path), tz=datetime.timezone.utc)
                },
                "l0": {
                    "ccsds_path": ccsds_path,
                    "created": datetime.datetime.fromtimestamp(os.path.getmtime(ccsds_path), tz=datetime.timezone.utc)
                }
            }
        }
        dm = wm.database_manager
        dm.update_stream_metadata(stream.hosc_name, metadata)

        doc_version = "Space Packet Protocol, CCSDS 133.0-B-1 (with Issue 1, Cor. 1, Sept. 2010 and Issue 1, Cor. 2, " \
                      "Sept. 2012 addendums)"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ingested_hosc_path": self.stream_path,
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": datetime.datetime.fromtimestamp(
                os.path.getmtime(ccsds_path), tz=datetime.timezone.utc),
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "raw_hosc_path": stream.hosc_path,
                "l0_ccsds_path": ccsds_path,
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)


class L0IngestBAD(SlurmJobTask):
    """
    Ingests BAD STO files
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):
        logger.debug(f"{self.task_family} requires: {self.stream_path}")

        return None

    def output(self):
        logger.debug(f"{self.task_family} output: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):
        logger.debug(f"{self.task_family} work: {self.stream_path}")

        # Insert BAD stream into DB
        wm = WorkflowManager(config_path=self.config_path)
        dm = wm.database_manager
        dm.insert_bad_stream(os.path.basename(self.stream_path))
        logger.debug(f"Inserted BAD stream file into DB using path {self.stream_path}")

        # Get workflow manager again, now with stream_path
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream
        pge = wm.pges["emit-ios"]

        # Generate CSV version of file
        sto_to_csv_exe = os.path.join(pge.repo_dir, "emit", "bin", "emit_iss_sto_to_csv.py")
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        cmd = [sto_to_csv_exe, self.stream_path, "--outpath", tmp_output_dir]
        env = os.environ.copy()
        env["AIT_ROOT"] = pge.repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Find orbits that touch this stream file and update them
        orbits = dm.find_orbits_touching_date_range("start_time", stream.start_time, stream.stop_time) + \
            dm.find_orbits_touching_date_range("stop_time", stream.start_time, stream.stop_time) + \
            dm.find_orbits_encompassing_date_range(stream.start_time, stream.stop_time)
        orbit_symlink_paths = []
        # orbit_l1a_dirs = []
        if len(orbits) > 0:
            # Get unique orbit ids
            orbit_ids = [o["orbit_id"] for o in orbits]
            orbit_ids = list(set(orbit_ids))
            # Update orbit DB to include associated bad paths and stream object to include associated orbits
            for orbit_id in orbit_ids:
                # Update stream metadata
                if "associated_orbit_ids" in stream.metadata and stream.metadata["associated_orbit_ids"] is not None:
                    if orbit_id not in stream.metadata["associated_orbit_ids"]:
                        stream.metadata["associated_orbit_ids"].append(orbit_id)
                else:
                    stream.metadata["associated_orbit_ids"] = [orbit_id]
                dm.update_stream_metadata(stream.bad_name,
                                          {"associated_orbit_ids": stream.metadata["associated_orbit_ids"]})

                # Update orbit metadata
                wm_orbit = WorkflowManager(config_path=self.config_path, orbit_id=orbit_id)
                orbit = wm_orbit.orbit
                if "associated_bad_sto" in orbit.metadata and orbit.metadata["associated_bad_sto"] is not None:
                    if stream.bad_path not in orbit.metadata["associated_bad_sto"]:
                        orbit.metadata["associated_bad_sto"].append(stream.bad_path)
                else:
                    orbit.metadata["associated_bad_sto"] = [stream.bad_path]
                dm.update_orbit_metadata(orbit_id, {"associated_bad_sto": orbit.metadata["associated_bad_sto"]})

                # Save symlink paths to use after moving the file
                orbit_symlink_paths.append(os.path.join(orbit.raw_dir, stream.bad_name))
                # orbit_l1a_dirs.append(orbit.l1a_dir)

        # TODO: Get start/stop from CSV file (which column(s) to use?)

        # Move BAD file out of ingest folder
        if "ingest" in self.stream_path:
            wm.move(self.stream_path, stream.bad_path)

        # Symlink to this file from the orbits' raw dirs
        for symlink in orbit_symlink_paths:
            wm.symlink(stream.bad_path, symlink)

        # Copy CSV file to l0 dir
        tmp_csv_path = os.path.join(tmp_output_dir, "iss_bad_data_joined.csv")
        l0_bad_csv_path = os.path.join(stream.l0_dir, stream.bad_name.replace(".sto", ".csv"))
        wm.copy(tmp_csv_path, l0_bad_csv_path)

        # Symlink from the BAD l1a folder to the orbits l1a folders
        # for dir in q:
        #     orbit_id = os.path.basename(os.path.basename((dir)))
        #     dir_symlink = os.path.join(stream.l1a_dir, f"orbit_{orbit_id}_l1a_b{stream.config['build_num']}_"
        #     f"v{stream.config['processing_version']}")
        #     wm.symlink(dir, dir_symlink)

        # Update DB
        creation_time_raw = datetime.datetime.fromtimestamp(os.path.getmtime(stream.bad_path), tz=datetime.timezone.utc)
        creation_time_csv = datetime.datetime.fromtimestamp(os.path.getmtime(l0_bad_csv_path), tz=datetime.timezone.utc)
        metadata = {
            "products": {
                "raw": {
                    "bad_path": stream.bad_path,
                    "created": creation_time_raw
                },
                "l0": {
                    "bad_csv_path": l0_bad_csv_path,
                    "created": creation_time_csv
                }
            }
        }
        dm = wm.database_manager
        dm.update_stream_metadata(stream.bad_name, metadata)

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ingested_bad_path": self.stream_path,
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": "N/A",
            "product_creation_time": creation_time_raw,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "raw_bad_path": stream.bad_path,
                "l0_bad_csv_path": l0_bad_csv_path
            }
        }
        dm.insert_stream_log_entry(stream.bad_name, log_entry)


class L0ProcessPlanningProduct(SlurmJobTask):
    """
    Reads planning product and inserts/updates acquisitions in the DB
    """

    config_path = luigi.Parameter()
    plan_prod_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.plan_prod_path}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.plan_prod_path}")
        return None

    def work(self):

        logger.debug(f"{self.task_family} work: {self.plan_prod_path}")
        wm = WorkflowManager(config_path=self.config_path)
        dm = wm.database_manager
        pge = wm.pges["emit-main"]

        horizon_start_time = None
        with open(self.plan_prod_path, "r") as f:
            events = json.load(f)
            orbit_num = None
            orbit_ids = []
            dcids = []
            for e in events:
                # Check for starting orbit number
                if e["name"].lower() == "planning horizon start":
                    orbit_num = e["orbitId"]
                    horizon_start_time = e["datetime"]

                # Check for orbit object
                if e["name"].lower() == "start orbit":
                    # Raise error if we don't have a starting orbit number
                    if orbit_num is None:
                        raise RuntimeError(f"Planning product {self.plan_prod_path} is missing starting orbit number")

                    # Construct orbit object
                    orbit_id = str(orbit_num).zfill(5)
                    start_time = datetime.datetime.strptime(e["datetime"], "%Y-%m-%dT%H:%M:%S")
                    orbit_meta = {
                        "orbit_id": orbit_id,
                        "build_num": wm.config["build_num"],
                        "processing_version": wm.config["processing_version"],
                        "start_time": start_time
                    }

                    # Insert or update orbit in DB
                    if dm.find_orbit_by_id(orbit_id):
                        dm.update_orbit_metadata(orbit_id, orbit_meta)
                        logger.debug(f"Updated orbit in DB with {orbit_meta}")
                    else:
                        dm.insert_orbit(orbit_meta)
                        logger.debug(f"Inserted orbit in DB with {orbit_meta}")

                    # Update the stop_time of the previous orbit in DB
                    if orbit_num > 0:
                        prev_orbit_id = str(orbit_num - 1).zfill(5)
                        prev_orbit = dm.find_orbit_by_id(prev_orbit_id)
                        if prev_orbit is None:
                            raise RuntimeError(f"Unable to find previous orbit stop time while trying to update orbit "
                                               f"{orbit_id}")
                        prev_orbit["stop_time"] = start_time
                        dm.update_orbit_metadata(prev_orbit_id, prev_orbit)

                    # Keep track of orbit_ids for log entry
                    orbit_ids.append(orbit_id)

                    # Increment orbit_num to continue processing file
                    orbit_num += 1

                # Check for data collection (i.e. acquisition)
                # TODO: Add "dark" in when its ready
                if e["name"].lower() in ("science", "dark"):
                    # Raise error if we don't have a starting orbit number
                    if orbit_num is None:
                        raise RuntimeError(f"Planning product {self.plan_prod_path} is missing starting orbit number")

                    # Construct data collection metadata
                    dcid = str(e["dcid"]).zfill(10)
                    dc_meta = {
                        "dcid": dcid,
                        "build_num": wm.config["build_num"],
                        "processing_version": wm.config["processing_version"],
                        "planned_start_time": datetime.datetime.strptime(e["datetime"], "%Y-%m-%dT%H:%M:%S"),
                        "planned_stop_time": datetime.datetime.strptime(e["endDatetime"], "%Y-%m-%dT%H:%M:%S"),
                        "orbit": str(e["orbit number"]).zfill(5),
                        "scene": str(e["scene number"]).zfill(3),
                        "submode": e["name"].lower(),
                        "betaangle": e["betaangle"],
                        "comments": e["comments"],
                        "latboresightstart": e["latboresightstart"],
                        "lonboresightstart": e["lonboresightstart"],
                        "parameters": e["parameters"],
                        "sza": e["sza"],
                        "frames_status": ""
                    }

                    # Insert or update data collection in DB
                    if dm.find_data_collection_by_id(dcid):
                        dm.update_data_collection_metadata(dcid, dc_meta)
                        logger.debug(f"Updated data collection in DB with {dc_meta}")
                    else:
                        dm.insert_data_collection(dc_meta)
                        logger.debug(f"Inserted data collection in DB with {dc_meta}")

                    # Keep track of dcid for log entry
                    dcids.append(dcid)

            # Copy/move processed file to archive
            target_pp_path = os.path.join(wm.planning_products_dir, os.path.basename(self.plan_prod_path))
            wm.move(self.plan_prod_path, target_pp_path)

            # Add processing log entry for orbits and data collections
            log_entry = {
                "task": self.task_family,
                "pge_name": pge.repo_url,
                "pge_version": pge.version_tag,
                "pge_input_files": self.plan_prod_path,
                "pge_run_command": "N/A - database updates only",
                "documentation_version": "N/A",
                "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
                "completion_status": "SUCCESS",
                "output": {
                    "raw_planning_product_path": target_pp_path
                }
            }
            for orbit_id in orbit_ids:
                dm.insert_orbit_log_entry(orbit_id, log_entry)
            for dcid in dcids:
                dm.insert_data_collection_log_entry(dcid, log_entry)


class L0Deliver(SlurmJobTask):
    """
    Creates UMM-G JSON file for DAAC delivery, stages the files for delivery, and submits a notification to the DAAC
    :returns: DAAC notification and staged CCSDS and UMM-G files
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    miss_pkt_thresh = luigi.FloatParameter(default=0.1)

    task_namespace = "emit"

    def requires(self):
        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L0StripHOSC(config_path=self.config_path, stream_path=self.stream_path, level=self.level,
                           partition=self.partition, miss_pkt_thresh=self.miss_pkt_thresh)

    def output(self):
        logger.debug(f"{self.task_family} output: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):
        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream
        pge = wm.pges["emit-main"]

        # First create the UMM-G file
        name_split = stream.ccsds_name.split("_")
        granule_ur = f"EMITL0.0{wm.config['processing_version']}.{name_split[0]}_{name_split[1]}_{name_split[2]}"
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(stream.ccsds_path), tz=datetime.timezone.utc)
        ummg = daac_converter.initialize_ummg(granule_ur, creation_time.strftime("%Y-%m-%dT%H:%M:%S%z"), "EMITL0")
        ummg = daac_converter.add_data_file_ummg(ummg, stream.ccsds_path)
        # ummg = daac_converter.add_boundary_ummg(ummg, boundary_points_list)
        daac_ummg_json_path = stream.ccsds_path.replace(".bin", "_ummg.cmr.json")
        daac_converter.dump_json(ummg, daac_ummg_json_path)
        wm.change_group_ownership(daac_ummg_json_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={stream.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{stream.daac_staging_dir}/"
        group = f"emit-{wm.config['environment']}" if wm.config["environment"] in ("test", "ops") else "emit-dev"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{stream.daac_staging_dir}'", "];", "then", "mkdir", f"{stream.daac_staging_dir};",
                           "chgrp", group, f"{stream.daac_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)

        for path in (stream.ccsds_path, daac_ummg_json_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = os.path.basename(stream.ccsds_path).replace(".bin", f"_{utc_now.strftime('%Y%m%dt%H%M%S')}")
        cnm_submission_path = os.path.join(stream.l0_dir, cnm_submission_id + "_cnm.json")
        notification = {
            "collection": "EMITL0",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": granule_ur,
                "dataVersion": f"0{wm.config['processing_version']}",
                "files": [
                    {
                        "name": os.path.basename(stream.ccsds_path),
                        "uri": stream.daac_uri_base + os.path.basename(stream.ccsds_path),
                        "type": "data",
                        "size": os.path.getsize(stream.ccsds_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(stream.ccsds_path, "sha512")
                    },
                    {
                        "name": os.path.basename(daac_ummg_json_path),
                        "uri": stream.daac_uri_base + os.path.basename(daac_ummg_json_path),
                        "type": "metadata",
                        "size": os.path.getsize(daac_ummg_json_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_ummg_json_path, "sha512")
                    }
                ]
            }
        }

        # Write notification submission to file
        with open(cnm_submission_path, "w") as f:
            f.write(json.dumps(notification, indent=4))
        wm.change_group_ownership(cnm_submission_path)

        # Submit notification via AWS SQS
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", wm.config["daac_submission_url"],
                   "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"]]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
        cnm_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(cnm_submission_path),
                                                            tz=datetime.timezone.utc)

        # Record delivery details in DB for reconciliation report
        dm = wm.database_manager
        for file in notification["product"]["files"]:
            delivery_report = {
                "timestamp": utc_now,
                "collection": notification["collection"],
                "version": notification["product"]["dataVersion"],
                "filename": file["name"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
                "submission_status": "submitted"
            }
            dm.insert_granule_report(delivery_report)

        # Update db with products and log entry
        product_dict_ummg = {
            "ummg_json_path": daac_ummg_json_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(daac_ummg_json_path), tz=datetime.timezone.utc)
        }
        dm.update_stream_metadata(stream.ccsds_name, {"products.daac.ccsds_ummg": product_dict_ummg})

        if "ccsds_daac_submissions" in stream.metadata["products"]["daac"] and \
                stream.metadata["products"]["daac"]["ccsds_daac_submissions"] is not None:
            stream.metadata["products"]["daac"]["ccsds_daac_submissions"].append(cnm_submission_path)
        else:
            stream.metadata["products"]["daac"]["ccsds_daac_submissions"] = [cnm_submission_path]
        dm.update_stream_metadata(
            stream.ccsds_name,
            {"products.daac.ccsds_daac_submissions": stream.metadata["products"]["daac"]["ccsds_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ccsds_path": stream.ccsds_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l0_ccsds_ummg_path": daac_ummg_json_path,
                "l0_ccsds_cnm_submission_path": cnm_submission_path
            }
        }

        dm.insert_stream_log_entry(stream.ccsds_name, log_entry)
