"""
This code contains tasks for executing EMIT Level 0 PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import logging
import luigi
import os
import shutil

from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


# TODO: Full implementation TBD
class L0StripHOSC(SlurmJobTask):
    """
    Strips HOSC ethernet headers from raw data in apid-specific ingest folder
    :returns Ordered APID specific packet stream
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):
        logger.debug(self.task_family + " requires")
        return None

    def output(self):
        logger.debug(self.task_family + " output")
        return luigi.LocalTarget("ccsds_path")

    def work(self):
        logger.debug(self.task_family + " work")

        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream
        pge = wm.pges["emit-sds-l0"]

        sds_l0_exe = os.path.join(pge.repo_dir, "run_l0.sh")
        sds_packet_count_exe = os.path.join(pge.repo_dir, "packet_cnt_check.py")
        ios_l0_proc = os.path.join(wm.pges["emit-l0edp"].repo_dir, "target", "release", "emit_l0_proc")
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        tmp_log = os.path.join(tmp_output_dir, stream.hosc_name + ".log")

        # Build command and run
        cmd = [sds_l0_exe, stream.path, tmp_output_dir, tmp_log, sds_packet_count_exe, ios_l0_proc]
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, env=env)

        if "ingest" in stream.path:
            # Move HOSC file out of ingest folder
            # TODO: Change to shutil.move
            shutil.copy(stream.path, stream.hosc_dir)
        # Copy scratch files back to store
        for file in glob.glob(os.path.join(tmp_output_dir, stream.apid + "*")):
            shutil.copy(file, stream.ccsds_dir)
        for file in glob.glob(os.path.join(tmp_output_dir, "*.log")):
            shutil.copy(file, stream.hosc_dir)
        # Get ccsds output filename
        ccsds_name = os.path.basename(glob.glob(os.path.join(tmp_output_dir, stream.apid + "*.bin"))[0])
        ccsds_path = os.path.join(stream.ccsds_dir, ccsds_name)

        # Update DB
        # TODO: Do I need build_num?
        start_time = datetime.datetime.strptime(stream.start_time_str, "%Y%m%d%H%M%S")
        stop_time = datetime.datetime.strptime(stream.stop_time_str, "%Y%m%d%H%M%S")
        metadata = {
            "apid": stream.apid,
            "hosc_name": stream.hosc_name,
            "ccsds_name": ccsds_name,
            "start_time": start_time,
            "stop_time": stop_time,
        }
        dm = wm.database_manager
        dm.insert_stream(metadata)

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_name,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ingested_hosc_file": stream.path,
            },
            "pge_run_command": " ".join(cmd),
            "product_creation_time": datetime.datetime.fromtimestamp(os.path.getmtime(ccsds_path)),
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "ccsds_path": ccsds_path,
            }
        }
        dm.insert_stream_log_entry(stream.apid, start_time, stop_time, log_entry)