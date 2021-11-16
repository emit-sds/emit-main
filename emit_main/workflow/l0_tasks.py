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

from emit_main.workflow.stream_target import StreamTarget
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

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
        logger.debug(self.task_family + " requires")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        if wm.stream is None:
            # Insert new stream in db
            dm = wm.database_manager
            dm.insert_hosc_stream(os.path.basename(self.stream_path))
        return None

    def output(self):
        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):
        logger.debug(self.task_family + " work")

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

        logger.debug(self.task_family + " requires")
        return None

    def output(self):

        logger.debug(self.task_family + " output")
        return None

    def work(self):

        logger.debug(self.task_family + " work")
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
                        # TODO throw error if not found?
                        prev_orbit["stop_time"] = start_time
                        dm.update_orbit_metadata(prev_orbit_id, prev_orbit)

                    # Keep track of orbit_ids for log entry
                    orbit_ids.append(orbit_id)

                    # Increment orbit_num to continue processing file
                    orbit_num += 1

                # Check for data collection (i.e. acquisition)
                # TODO: Add "dark" in when its ready
                if e["name"].lower() in ("science",):
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
                        "sza": e["sza"]
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
        # TODO: Is there some unique portion of the name we can use here based on input file name?
        target_pp_path = os.path.join(
            wm.planning_products_dir,
            f"emit_{horizon_start_time.replace('-', '').replace('T', 't').replace(':', '')}_"
            f"raw_plan_b{wm.config['build_num']}_v{wm.config['processing_version']}.json")
        wm.move(self.plan_prod_path, target_pp_path)

        # Add processing log entry
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": planning_prod_path,
            "pge_run_command": "N/A - database updates only",
            "documentation_version": "N/A",
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "raw_planning_product_path": target_pp_path
            }
        }
        dm.insert_orbit_log_entry(orbit_id, log_entry)
        dm.insert_data_collection_log_entry(dcid, log_entry)
