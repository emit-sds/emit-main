"""
This code contains tasks for executing EMIT Level 0 PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import csv
import datetime
import glob
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
                "hosc_path": stream.hosc_path,
                "ccsds_path": ccsds_path,
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)


class L0ProcessPlanningProduct(SlurmJobTask):
    """
    Reads planning product and inserts/updates acquisitions in the DB
    """

    config_path = luigi.Parameter()
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
        planning_prod_paths = glob.glob(os.path.join(wm.ingest_dir, "*csv"))
        for planning_prod_path in planning_prod_paths:
            with open(planning_prod_path, "r") as csvfile:
                logger.debug(f"Processing planned observations from file {planning_prod_path}")
                csvreader = csv.reader(csvfile)
                header_row = next(csvreader)
                for row in csvreader:
                    # These times are already in UTC and will be stored in DB as UTC by default
                    dcid = row[0]
                    planned_start_time = datetime.datetime.strptime(row[1], "%Y%m%dT%H%M%S")
                    planned_stop_time = datetime.datetime.strptime(row[2], "%Y%m%dT%H%M%S")
                    dc_meta = {
                        "dcid": dcid,
                        "build_num": wm.config["build_num"],
                        "processing_version": wm.config["processing_version"],
                        "planned_start_time": planned_start_time,
                        "planned_stop_time": planned_stop_time,
                        "orbit": row[3],
                        "scene": row[4],
                        "submode": row[5].lower()
                    }

                    if dm.find_data_collection_by_id(dcid):
                        dm.update_data_collection_metadata(dcid, dc_meta)
                        logger.debug(f"Updated data collection in DB with DCID {dc_meta}")
                    else:
                        dm.insert_data_collection(dc_meta)
                        logger.debug(f"Inserted data collection in DB with DCID {dc_meta}")

                    # Add processing log entry
                    log_entry = {
                        "task": self.task_family,
                        "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
                        "completion_status": "SUCCESS"
                    }
                    dm.insert_data_collection_log_entry(dcid, log_entry)
