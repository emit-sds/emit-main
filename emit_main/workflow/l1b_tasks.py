"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import os
import shutil

import luigi
import spectral.io.envi as envi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1a_tasks import L1AReassembleRaw
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


# TODO: Full implementation TBD
class L1BCalibrate(SlurmJobTask):
    """
    Performs calibration of raw data to produce radiance
    :returns: Spectrally calibrated radiance
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1AReassembleRaw(config_path=self.config_path, acquisition_id=self.acquisition_id)

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        os.makedirs(tmp_output_dir)
        tmp_rdn_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.rdn_img_path))
        log_name = os.path.basename(acq.rdn_img_path.replace(".img", "_pge.log"))
        tmp_log_path = os.path.join(tmp_output_dir, log_name)
        # TODO Add logic to check date ranges for proper config
        calibrations_dir = os.path.join(pge.repo_dir, "calibrations")
        l1b_config_path = os.path.join(calibrations_dir, "config_20210101_20210131.json")
        with open(l1b_config_path, "r") as f:
            config = json.load(f)
        # Set input, dark, and output paths in config
        config["input_file"] = acq.raw_img_path
        # TODO: Get dark frame for this acquisition
        config["dark_frame_file"] = acq.dark_img_path
        config["output_file"] = tmp_rdn_img_path

        input_files = {}
        for key, value in config.items():
            if "_file" in key and not key.startswith("/"):
                config[key] = os.path.abspath(os.path.join(calibrations_dir, value))
                if key != "output_file":
                    input_files[key] = config[key]

        tmp_config_path = os.path.join(self.tmp_dir, "l1b_config.json")
        with open(tmp_config_path, "w") as outfile:
            json.dump(config, outfile)

        emitrdn_exe = os.path.join(pge.repo_dir, "emitrdn.py")
        cmd = ["python", emitrdn_exe, tmp_config_path, acq.raw_img_path, tmp_rdn_img_path, "--log_file", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy output files to l1b dir
        for file in glob.glob(os.path.join(tmp_output_dir, "*")):
            shutil.copy2(file, acq.l1b_data_dir)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        hdr = envi.read_envi_header(acq.rdn_hdr_path)
        hdr["emit acquisition start time"] = datetime.datetime(2020, 1, 1, 0, 0, 0).strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit acquisition stop time"] = datetime.datetime(2020, 1, 1, 0, 11, 26).strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.build_num
        hdr["emit documentation version"] = "TBD"
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_img_path))
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit data product version"] = wm.processing_version

        envi.write_envi_header(acq.rdn_hdr_path, hdr)

        # PGE writes metadata to db
        dimensions = {
            "l1b": {
                "lines": hdr["lines"],
                "bands": hdr["bands"],
                "samples": hdr["samples"],
            }
        }
        dm = wm.database_manager
        dm.update_acquisition_dimensions(self.acquisition_id, dimensions)

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_path": acq.rdn_img_path,
                "l1b_rdn_hdr_path:": acq.rdn_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


# TODO: Full implementation TBD
class L1BGeolocate(SlurmJobTask):
    """
    Performs geolocation using BAD telemetry and counter-OS time pair file
    :returns: Geolocation files including GLT, OBS, LOC, corrected attitude and ephemeris
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id)

    def output(self):

        logger.debug(self.task_family + " output")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return (ENVITarget(acq.loc_img_path),
                ENVITarget(acq.obs_img_path),
                ENVITarget(acq.glt_img_path))

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        cmd = ["touch", acq.loc_img_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)
        cmd = ["touch", acq.loc_hdr_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)
        cmd = ["touch", acq.obs_img_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)
        cmd = ["touch", acq.obs_hdr_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)
        cmd = ["touch", acq.glt_img_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)
        cmd = ["touch", acq.glt_hdr_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)
