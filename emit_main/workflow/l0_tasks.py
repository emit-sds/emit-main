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

from emit_main.workflow.stream_target import StreamTarget
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

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
        # TODO: Check for hosc filename and throw exception if incorrect
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
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        l0_pge_log_name = stream.hosc_name.replace(".bin", "_l0_pge.log")
        tmp_log = os.path.join(tmp_output_dir, l0_pge_log_name)

        cmd = [sds_l0_exe, self.stream_path, tmp_output_dir, tmp_log, sds_packet_count_exe, ios_l0_proc]
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        if "ingest" in self.stream_path:
            # Move HOSC file out of ingest folder
            # TODO: Change to shutil.move
            shutil.copy2(self.stream_path, stream.l0_dir)
            hosc_path = os.path.join(stream.l0_dir, stream.hosc_name)
        else:
            hosc_path = self.stream_path
        # Copy scratch files back to store
        for file in glob.glob(os.path.join(tmp_output_dir, stream.apid + "*")):
            shutil.copy2(file, stream.l0_dir)
        for file in glob.glob(os.path.join(tmp_output_dir, "*.log")):
            shutil.copy2(file, stream.l0_dir)
        # Get ccsds output filename
        ccsds_name = os.path.basename(glob.glob(os.path.join(tmp_output_dir, stream.apid + "*.bin"))[0])
        ccsds_path = os.path.join(stream.l0_dir, ccsds_name)
        # TODO: Change log name to match CCSDS name?

        # Update DB
        # TODO: Use stream_path for query
        query = {
            "apid": stream.apid,
            "start_time": stream.start_time,
            "stop_time": stream.stop_time,
            "build_num": wm.build_num,
            "processing_version": wm.processing_version
        }
        metadata = {
            "apid": stream.apid,
            "hosc_name": stream.hosc_name,
            "ccsds_name": ccsds_name,
            "start_time": stream.start_time,
            "stop_time": stream.stop_time
        }
        dm = wm.database_manager
        dm.update_stream_metadata(query, metadata)

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ingested_hosc_path": self.stream_path,
            },
            "pge_run_command": " ".join(cmd),
            "product_creation_time": datetime.datetime.fromtimestamp(os.path.getmtime(ccsds_path)),
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "hosc_path": hosc_path,
                "ccsds_path": ccsds_path,
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)
