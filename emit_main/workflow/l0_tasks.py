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
    miss_pkt_thresh = luigi.FloatParameter(default=0.01)

    memory = 18000

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

        wm = WorkflowManager(config_path=self.config_path)
        pge = wm.pges["emit-sds-l0"]

        # Build command and run
        sds_l0_exe = os.path.join(pge.repo_dir, "run_l0.sh")
        sds_packet_count_exe = os.path.join(pge.repo_dir, "packet_cnt_check.py")
        ios_l0_proc = os.path.join(wm.pges["emit-l0edp"].repo_dir, "target", "release", "emit_l0_proc")
        # Create input dir and copy stream file into dir
        tmp_input_dir = os.path.join(self.local_tmp_dir, "input")
        wm.makedirs(tmp_input_dir)
        hosc_name = os.path.basename(self.stream_path)
        if hosc_name.lower().startswith("emit"):
            apid = hosc_name.split("_")[1]
        else:
            apid = hosc_name.split("_")[0]
        tmp_input_path = os.path.join(tmp_input_dir, hosc_name)
        wm.copy(self.stream_path, tmp_input_path)
        # Create output dir and log file name
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        l0_pge_log_name = os.path.basename(tmp_input_path).replace(".bin", "_l0_pge.log")
        tmp_log = os.path.join(tmp_output_dir, l0_pge_log_name)
        # Set up command
        cmd = [sds_l0_exe, tmp_input_dir, tmp_output_dir, tmp_log, sds_packet_count_exe, ios_l0_proc]
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Get tmp ccsds and log name
        tmp_ccsds_path = glob.glob(os.path.join(tmp_output_dir, apid + "*.bin"))[0]
        tmp_report_path = glob.glob(os.path.join(tmp_output_dir, apid + "*_report.txt"))[0]

        # Check report file to see if missing PSCs exceed threshold
        packet_count = 0
        missing_packets = 0
        with open(tmp_report_path, "r") as f:
            for line in f.readlines():
                if "Packet Count" in line:
                    packet_count = int(line.rstrip("\n").split(" ")[-1])
                if "Missing PSC Count" in line:
                    missing_packets = int(line.rstrip("\n").split(" ")[-1])
        total_expected_packets = packet_count + missing_packets
        miss_pkt_ratio = missing_packets / total_expected_packets
        if miss_pkt_ratio >= self.miss_pkt_thresh:
            raise RuntimeError(f"Packets read: {packet_count}, missing packets: {missing_packets}. Ratio of missing "
                               f"packets ({missing_packets}) to total expected ({total_expected_packets}) is "
                               f"{miss_pkt_ratio} which is greater than or equal to the missing packet threshold of "
                               f"{self.miss_pkt_thresh}")

        # Set up command to get CCSDS start/stop times
        get_start_stop_exe = os.path.join(pge.repo_dir, "get_ccsds_start_stop_times.py")
        start_stop_json = os.path.join(tmp_output_dir, os.path.basename(tmp_ccsds_path).replace(".bin", ".json"))
        cmd_timing = ["python", get_start_stop_exe, tmp_ccsds_path, start_stop_json]
        pge.run(cmd_timing, tmp_dir=self.tmp_dir, env=env)
        with open(start_stop_json, "r") as f:
            timing = json.load(f)

        # Insert new stream in DB
        dm = wm.database_manager
        metadata = {
            "apid": apid,
            "start_time": datetime.datetime.strptime(timing["start_time"], "%Y-%m-%dT%H:%M:%S"),
            "stop_time": datetime.datetime.strptime(timing["stop_time"], "%Y-%m-%dT%H:%M:%S"),
            "build_num": wm.config["build_num"],
            "processing_version": wm.config["processing_version"],
            "hosc_name": hosc_name,
            "processing_log": []
        }
        dm.insert_stream(hosc_name, metadata)

        # Get the workflow manager again with the stream object
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream

        # Build the CCSDS file name and report name using the UTC start time derived from the data
        ccsds_name = "_".join([
            wm.config["instrument"],
            stream.apid,
            stream.start_time.strftime("%Y%m%dt%H%M%S"),
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

        # Symlink the start time and stop times onto the HOSC filename for convenience
        hosc_symlink = "_".join([
            stream.apid,
            stream.start_time.strftime("%Y%m%dT%H%M%S"),
            stream.stop_time.strftime("%Y%m%dT%H%M%S")
        ])
        hosc_symlink += stream.hosc_name.replace(stream.apid, "")
        hosc_symlink = os.path.join(stream.raw_dir, hosc_symlink)
        wm.symlink(stream.hosc_name, hosc_symlink)

        # Update DB
        metadata = {
            "ccsds_name": ccsds_name,
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

    memory = 18000

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

        # Get workflow manager
        wm = WorkflowManager(config_path=self.config_path)
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

        # Get tmp output path for CSV
        tmp_csv_path = os.path.join(tmp_output_dir, "iss_bad_data_joined.csv")

        # Set up command to get BAD start/stop times
        pge_l0 = wm.pges["emit-sds-l0"]
        get_start_stop_exe = os.path.join(pge_l0.repo_dir, "get_bad_start_stop_times.py")
        start_stop_json = os.path.join(tmp_output_dir, os.path.basename(tmp_csv_path).replace(".csv", ".json"))
        cmd_timing = ["python", get_start_stop_exe, tmp_csv_path, start_stop_json]
        pge.run(cmd_timing, tmp_dir=self.tmp_dir, env=env)
        with open(start_stop_json, "r") as f:
            timing = json.load(f)

        # Insert new stream in DB
        dm = wm.database_manager
        start_time = datetime.datetime.strptime(timing["start_time"], "%Y-%m-%dT%H:%M:%S")
        stop_time = datetime.datetime.strptime(timing["stop_time"], "%Y-%m-%dT%H:%M:%S")
        bad_name = os.path.basename(self.stream_path)
        suffix = f"_{start_time.strftime('%Y%m%dT%H%M%S')}_{stop_time.strftime('%Y%m%dT%H%M%S')}.sto"
        extended_bad_name = os.path.basename(self.stream_path).replace(".sto", suffix)
        metadata = {
            "apid": "bad",
            "start_time": start_time,
            "stop_time": stop_time,
            "build_num": wm.config["build_num"],
            "processing_version": wm.config["processing_version"],
            "bad_name": bad_name,
            "extended_bad_name": extended_bad_name,
            "processing_log": []
        }
        dm.insert_stream(bad_name, metadata)

        # Get workflow manager again with stream metadata
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream

        # Find orbits that touch this stream file and update them
        orbits = dm.find_orbits_touching_date_range("start_time", stream.start_time,
                                                    stream.stop_time + datetime.timedelta(seconds=10)) + \
            dm.find_orbits_touching_date_range("stop_time", stream.start_time - datetime.timedelta(seconds=10),
                                               stream.stop_time) + \
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
                        stream.metadata["associated_orbit_ids"].sort()
                else:
                    stream.metadata["associated_orbit_ids"] = [orbit_id]
                dm.update_stream_metadata(stream.bad_name,
                                          {"associated_orbit_ids": stream.metadata["associated_orbit_ids"]})

                # Update orbit metadata
                wm_orbit = WorkflowManager(config_path=self.config_path, orbit_id=orbit_id)
                orbit = wm_orbit.orbit
                if "associated_bad_sto" in orbit.metadata and orbit.metadata["associated_bad_sto"] is not None:
                    if stream.extended_bad_path not in orbit.metadata["associated_bad_sto"]:
                        orbit.metadata["associated_bad_sto"].append(stream.extended_bad_path)
                        orbit.metadata["associated_bad_sto"].sort()
                else:
                    orbit.metadata["associated_bad_sto"] = [stream.extended_bad_path]
                dm.update_orbit_metadata(orbit_id, {"associated_bad_sto": orbit.metadata["associated_bad_sto"]})

                # Check if orbit has complete bad data
                if orbit.has_complete_bad_data():
                    dm.update_orbit_metadata(orbit_id, {"bad_status": "complete"})
                else:
                    dm.update_orbit_metadata(orbit_id, {"bad_status": "incomplete"})

                # Save symlink paths to use after moving the file
                orbit_symlink_paths.append(os.path.join(orbit.raw_dir, stream.extended_bad_name))
                # orbit_l1a_dirs.append(orbit.l1a_dir)

        # Move BAD file out of ingest folder
        if "ingest" in self.stream_path:
            wm.move(self.stream_path, stream.bad_path)

        # Symlink extended bad path to bad path
        wm.symlink(stream.bad_name, stream.extended_bad_path)

        # Symlink to this file from the orbits' raw dirs
        for symlink in orbit_symlink_paths:
            wm.symlink(stream.bad_path, symlink)

        # Copy CSV file to l0 dir
        # l0_bad_csv_name = "_".join(["emit", start_time.strftime("%Y%m%dt%H%M%S"), "l0", "bad",
        #                             "b" + wm.config["build_num"], "v" + wm.config["processing_version"]]) + ".csv"
        l0_bad_csv_path = os.path.join(stream.l0_dir, extended_bad_name.replace(".sto", ".csv"))
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
    test_mode = luigi.BoolParameter(default=False)

    memory = 18000

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

        # Read in pp
        with open(self.plan_prod_path, "r") as f:
            events = json.load(f)

        # Get start/stop times
        horizon_start_time = None
        horizon_end_time = None
        for e in events:
            if e["name"].lower() == "planning horizon start":
                horizon_start_time = datetime.datetime.strptime(e["datetime"], "%Y-%m-%dT%H:%M:%S")
            if e["name"].lower() == "planning horizon end":
                horizon_end_time = datetime.datetime.strptime(e["datetime"], "%Y-%m-%dT%H:%M:%S")

        if (horizon_start_time is None or horizon_end_time is None) and not self.test_mode:
            raise RuntimeError("Either the horizon start time or the horizon end time is not defined.  Aborting.  Use "
                               "--test_mode to bypass this error.")

        wm.print(__name__, f"Processing planning product with horizon start and end of {horizon_start_time} and "
                           f"{horizon_end_time}")

        # Throw error if in the past
        if horizon_start_time < datetime.datetime.utcnow() and not self.test_mode:
            raise RuntimeError("Planning product horizon start time is before now. There can be problems "
                               "updating orbits or DCIDs in the past. Use --test_mode flag to bypass.")

        # Check all orbits and DCIDS overlapping and throw error if they have any products
        overlapping_orbits = dm.find_orbits_touching_date_range("start_time", horizon_start_time, horizon_end_time) + \
            dm.find_orbits_touching_date_range("stop_time", horizon_start_time, horizon_end_time)
        for o in overlapping_orbits:
            if "products" in o:
                raise RuntimeError(f"Found an orbit overlapping the horizon start and end times that has some "
                                   f"products already defined. Check the planning product or adjust the DB as needed.")

        # Check both planned start and stop time dates and actual start and stop
        overlapping_dcs = dm.find_data_collections_touching_date_range("planning_product.datetime", horizon_start_time,
                                                                       horizon_end_time)
        overlapping_dcs += dm.find_data_collections_touching_date_range("planning_product.endDatetime",
                                                                        horizon_start_time, horizon_end_time)
        overlapping_dcs += dm.find_data_collections_touching_date_range("start_time", horizon_start_time,
                                                                        horizon_end_time)
        overlapping_dcs += dm.find_data_collections_touching_date_range("stop_time",
                                                                        horizon_start_time, horizon_end_time)
        for dc in overlapping_dcs:
            if "products" in dc:
                raise RuntimeError(f"Found a data collection overlapping the horizon start and end times that has some "
                                   f"products already defined. Check the planning product or adjust the DB as needed.")

        # If we made it this far, delete all orbits and DCIDs in DB overlapping time range (delete folders?)
        deleted = dm.delete_orbits_touching_date_range("start_time", horizon_start_time, horizon_end_time)
        deleted += dm.delete_orbits_touching_date_range("stop_time", horizon_start_time, horizon_end_time)
        if len(deleted) > 0:
            wm.print(__name__, f"Found {len(deleted)} orbits overlapping planning product date range. Deleting...")
            wm.print(__name__, f"Deleted {len(deleted)} orbits: {[d['orbit_id'] for d in deleted]}")
        # Now data collections
        deleted = dm.delete_data_collections_touching_date_range("planning_product.datetime", horizon_start_time,
                                                                 horizon_end_time)
        deleted += dm.delete_data_collections_touching_date_range("planning_product.endDatetime", horizon_start_time,
                                                                  horizon_end_time)
        deleted += dm.delete_data_collections_touching_date_range("start_time", horizon_start_time, horizon_end_time)
        deleted += dm.delete_data_collections_touching_date_range("stop_time", horizon_start_time, horizon_end_time)
        if len(deleted) > 0:
            wm.print(__name__, f"Found {len(deleted)} data collections overlapping planning product date range. "
                               f"Deleting...")
            wm.print(__name__, f"Deleted {len(deleted)} data collections: {[d['dcid'] for d in deleted]}")

        # Loop through events again and insert new orbits and DCIDS
        orbit_ids = []
        dcids = []
        orbits_with_science = set()
        orbits_with_dark = set()
        for e in events:
            # Check for orbit object
            if e["name"].lower() == "start orbit":
                # Construct orbit object
                start_time = datetime.datetime.strptime(e["datetime"], "%Y-%m-%dT%H:%M:%S")
                year = start_time.strftime("%y")
                orbit_num = int(e["orbitId"])
                orbit_id = year + str(orbit_num).zfill(5)

                orbit_meta = {
                    "orbit_id": orbit_id,
                    "build_num": wm.config["build_num"],
                    "processing_version": wm.config["processing_version"],
                    "start_time": start_time
                }

                # Insert or update orbit in DB
                if dm.find_orbit_by_id(orbit_id):
                    dm.update_orbit_metadata(orbit_id, orbit_meta)
                    wm.print(__name__, f"Updated orbit in DB with {orbit_meta}")
                else:
                    dm.insert_orbit(orbit_meta)
                    wm.print(__name__, f"Inserted orbit in DB with {orbit_meta}")

                # Update the stop_time of the previous orbit in DB
                if len(orbit_ids) > 0:
                    prev_orbit_id = orbit_ids[-1]
                    prev_orbit = dm.find_orbit_by_id(prev_orbit_id)
                    if prev_orbit is None:
                        raise RuntimeError(f"Unable to find previous orbit with id {prev_orbit_id}. Not able to set "
                                           f"stop time of previous orbit.")
                    prev_orbit["stop_time"] = start_time
                    dm.update_orbit_metadata(prev_orbit_id, prev_orbit)
                    wm.print(__name__, f"Updated orbit {prev_orbit_id} with stop time")

                # Keep track of orbit_ids for log entry
                orbit_ids.append(orbit_id)

            # Get final orbit end time from horizon end
            if e["name"].lower() == "planning horizon end":
                prev_orbit_id = orbit_ids[-1]
                prev_orbit = dm.find_orbit_by_id(prev_orbit_id)
                if prev_orbit is None:
                    raise RuntimeError(f"Unable to find previous orbit with id {prev_orbit_id}. Not able to set "
                                       f"stop time of previous orbit.")
                prev_orbit["stop_time"] = horizon_end_time
                dm.update_orbit_metadata(prev_orbit_id, prev_orbit)
                wm.print(__name__, f"Updated orbit {prev_orbit_id} with stop time")

            # Check for data collection (i.e. acquisition)
            if e["name"].lower() in ("science", "dark"):
                # Construct data collection metadata
                dcid = str(e["dcid"]).zfill(10)
                submode = e["name"].lower()
                # Convert date strings to datetime objects to store in DB
                e["datetime"] = datetime.datetime.strptime(e["datetime"], "%Y-%m-%dT%H:%M:%S")
                e["endDatetime"] = datetime.datetime.strptime(e["endDatetime"], "%Y-%m-%dT%H:%M:%S")
                year = e["datetime"].strftime("%y")
                dc_meta = {
                    "dcid": dcid,
                    "build_num": wm.config["build_num"],
                    "processing_version": wm.config["processing_version"],
                    "orbit": year + str(e["orbit number"]).zfill(5),
                    "scene": str(e["scene number"]).zfill(3),
                    "submode": submode,
                    "planning_product": e
                }

                # Insert or update data collection in DB
                if dm.find_data_collection_by_id(dcid):
                    dm.update_data_collection_metadata(dcid, dc_meta)
                    wm.print(__name__, f"Updated data collection in DB with {dc_meta}")
                else:
                    dc_meta["frames_status"] = ""
                    dm.insert_data_collection(dc_meta)
                    wm.print(__name__, f"Inserted data collection in DB with {dc_meta}")

                # Keep track of dcid for log entry
                dcids.append(dcid)

                # Keep track of orbits that have science and/or dark acquisitions
                if submode == "science":
                    orbits_with_science.add(dc_meta["orbit"])
                if submode == "dark":
                    orbits_with_dark.add(dc_meta["orbit"])

        # Update orbits to include flags for has_science and has_dark
        for orbit_id in orbit_ids:
            orbit_meta = {
                "has_science": False,
                "has_dark": False
            }
            if orbit_id in orbits_with_science:
                orbit_meta["has_science"] = True
            if orbit_id in orbits_with_dark:
                orbit_meta["has_dark"] = True
            dm.update_orbit_metadata(orbit_id, orbit_meta)
            wm.print(__name__, f"Updated orbit {orbit_id} in DB with {orbit_meta}")

        wm.print(__name__, f"Inserted a total of {len(orbit_ids)} orbits and {len(dcids)} data collections!")

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
    miss_pkt_thresh = luigi.FloatParameter(default=0.01)

    memory = 18000

    task_namespace = "emit"

    def requires(self):
        logger.debug(f"{self.task_family} requires: {self.stream_path}")
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

        # Create GranuleUR and DAAC paths
        # Delivery file format: EMIT_L0_<VVV>_<APID>_<YYYYMMDDTHHMMSS>.bin
        collection_version = f"0{wm.config['processing_version']}"
        start_time_str = stream.start_time.strftime("%Y%m%dT%H%M%S")
        granule_ur = f"EMIT_L0_{collection_version}_{stream.apid}_{start_time_str}"
        daac_ccsds_name = f"{granule_ur}.bin"
        daac_ummg_name = f"{granule_ur}.cmr.json"
        daac_ccsds_path = os.path.join(self.tmp_dir, daac_ccsds_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(stream.ccsds_path, daac_ccsds_path)

        # Create the UMM-G file
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(stream.ccsds_path), tz=datetime.timezone.utc)
        l0_pge = wm.pges["emit-sds-l0"]
        ummg = daac_converter.initialize_ummg(granule_ur, creation_time, "EMITL0", collection_version,
                                              wm.config["extended_build_num"], l0_pge.repo_name, l0_pge.version_tag)
        ummg = daac_converter.add_data_files_ummg(ummg, [daac_ccsds_path], "Unspecified", ["BINARY"])
        ummg = daac_converter.add_related_url(ummg, l0_pge.repo_url, "DOWNLOAD SOFTWARE")
        ummg_path = stream.ccsds_path.replace(".bin", ".cmr.json")
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={stream.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{stream.daac_staging_dir}/"
        # First set up permissions if needed
        group = f"emit-{wm.config['environment']}" if wm.config["environment"] in ("test", "ops") else "emit-dev"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{stream.daac_staging_dir}'", "];", "then", "mkdir", f"{stream.daac_staging_dir};",
                           "chgrp", group, f"{stream.daac_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)
        # Rsync the files
        for path in (daac_ccsds_path, daac_ummg_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(stream.l0_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_ccsds_name: os.path.basename(stream.ccsds_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        notification = {
            "collection": "EMITL0",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": granule_ur,
                "dataVersion": collection_version,
                "files": [
                    {
                        "name": daac_ccsds_name,
                        "uri": stream.daac_uri_base + daac_ccsds_name,
                        "type": "data",
                        "size": os.path.getsize(daac_ccsds_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_ccsds_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": stream.daac_uri_base + daac_ummg_name,
                        "type": "metadata",
                        "size": os.path.getsize(daac_ummg_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_ummg_path, "sha512")
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
                "extended_build_num": wm.config["extended_build_num"],
                "collection": notification["collection"],
                "collection_version": notification["product"]["dataVersion"],
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
                "submission_status": "submitted"
            }
            dm.insert_granule_report(delivery_report)

        # Update db with products and log entry
        product_dict_ummg = {
            "ummg_json_path": ummg_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(ummg_path), tz=datetime.timezone.utc)
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
                "l0_ccsds_ummg_path": ummg_path,
                "l0_ccsds_cnm_submission_path": cnm_submission_path
            }
        }

        dm.insert_stream_log_entry(stream.ccsds_name, log_entry)
