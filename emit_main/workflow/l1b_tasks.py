"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Authors: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
         Philip G. Brodrick, philip.brodrick@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import os
import subprocess

import luigi
import spectral.io.envi as envi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1a_tasks import L1AReassembleRaw
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils.daac_converter import calc_checksum
from emit_utils.file_checks import netcdf_ext


logger = logging.getLogger("emit-main")


class L1BCalibrate(SlurmJobTask):
    """
    Performs calibration of raw data to produce radiance
    :returns: Spectrally calibrated radiance
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L1AReassembleRaw(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                                partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_rdn_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.rdn_img_path))
        log_name = os.path.basename(acq.rdn_img_path.replace(".img", "_pge.log"))
        tmp_log_path = os.path.join(tmp_output_dir, log_name)
        with open(wm.config["l1b_config_path"], "r") as f:
            config = json.load(f)
        # Set input, dark, and output paths in config
        config["input_file"] = acq.raw_img_path
        # TODO: Get dark frame for this acquisition
        config["dark_frame_file"] = acq.dark_img_path
        config["output_file"] = tmp_rdn_img_path

        input_files = {}
        calibrations_dir = os.path.join(pge.repo_dir, "calibrations")
        for key, value in config.items():
            if "_file" in key and not key.startswith("/"):
                config[key] = os.path.abspath(os.path.join(calibrations_dir, value))
                if key != "output_file":
                    input_files[key] = config[key]

        tmp_config_path = os.path.join(self.tmp_dir, "l1b_config.json")
        with open(tmp_config_path, "w") as outfile:
            json.dump(config, outfile)

        emitrdn_exe = os.path.join(pge.repo_dir, "emitrdn.py")
        cmd = ["python", emitrdn_exe, tmp_config_path, acq.raw_img_path, tmp_rdn_img_path, "--log_file", tmp_log_path,
               "--level", self.level]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy output files to l1b dir
        for file in glob.glob(os.path.join(tmp_output_dir, "*")):
            wm.copy(file, os.path.join(acq.l1b_data_dir, os.path.basename(file)))

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L1B JPL-D 104187, Initial"
        hdr = envi.read_envi_header(acq.rdn_hdr_path)
        hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rdn_img_path), tz=datetime.timezone.utc)
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit data product version"] = wm.config["processing_version"]

        envi.write_envi_header(acq.rdn_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict = {
            "img_path": acq.rdn_img_path,
            "hdr_path": acq.rdn_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn": product_dict})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_img_path": acq.rdn_img_path,
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
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                            partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

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


class L1BFormat(SlurmJobTask):
    """
    Converts L1B (geolocation and radiance) to netcdf files
    :returns: L1B netcdf output for delivery
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L1BFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                         partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition

        pge = wm.pges["emit-sds-l1b"]

        output_generator_exe = os.path.join(pge.repo_dir, "output_conversion.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        tmp_daac_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l1b_rdn.nc")
        tmp_ummg_json_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l1b_rdn_ummg.json")
        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")

        cmd = ["python", output_generator_exe, tmp_daac_nc_path, acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path,
               acq.glt_img_path, "--ummg_file", tmp_ummg_json_path, "--log_file", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy and rename output files back to /store
        # EMITL1B_RAD.vVV_yyyymmddthhmmss_oOOOOO_sSSS_yyyymmddthhmmss.nc
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        daac_nc_path = f"{acq.daac_l1b_prefix}_{utc_now.strftime('%Y%m%dt%H%M%S')}.nc"
        daac_ummg_json_path = daac_nc_path.replace(".nc", "_ummg.json")
        log_path = daac_nc_path.replace(".nc", "_pge.log")
        wm.copy(tmp_daac_nc_path, daac_nc_path)
        wm.copy(tmp_ummg_json_path, daac_ummg_json_path)
        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        dm = wm.database_manager
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(daac_nc_path), tz=datetime.timezone.utc)
        product_dict_netcdf = {
            "netcdf_path": daac_nc_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_netcdf": product_dict_netcdf})

        product_dict_ummg = {
            "ummg_json_path": daac_ummg_json_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(daac_ummg_json_path), tz=datetime.timezone.utc)
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1b.rdn_ummg": product_dict_ummg})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": [acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path, acq.glt_img_path],
            "pge_run_command": " ".join(cmd),
            "documentation_version": "TBD",
            "product_creation_time": nc_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_netcdf_path": daac_nc_path,
                "l1b_rdn_ummg_path:": daac_ummg_json_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1BDeliver(SlurmJobTask):
    """
    Stages NetCDF and UMM-G files and submits notification to DAAC interface
    :returns: Staged L1B files
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"
    n_cores = 1
    memory = 30000

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]

        # Locate matching NetCDF and UMM-G files for this acquisition. If there is more than 1, get most recent
        daac_nc_path = sorted(glob.glob(os.path.join(acq.l1b_data_dir, acq.daac_l1b_prefix + "*.nc")))[-1]
        daac_ummg_json_path = sorted(glob.glob(os.path.join(acq.l1b_data_dir, acq.daac_l1b_prefix + "*ummg.json")))[-1]
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = os.path.basename(daac_nc_path).replace(".nc", f"_{utc_now.strftime('%Y%m%dt%H%M%S')}")
        cnm_submission_path = os.path.join(acq.l1b_data_dir, cnm_submission_id + ".json")

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={acq.partial_dir}"
        log_file_arg= f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{acq.daac_staging_dir}"
        cmd_nc = ["rysync", "-azv", partial_dir_arg, log_file_arg, daac_nc_path, target]
        cmd_json = ["rysync", "-azv", partial_dir_arg, log_file_arg, daac_ummg_json_path, target]
        pge.run(cmd_nc, tmp_dir=self.tmp_dir)
        pge.run(cmd_json, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        notification = {
            "collection": wm.config["collections"]["l1brad"],
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": os.path.basename(daac_nc_path).replace(".nc", ""),
                "dataVersion": wm.config["processing_version"],
                "files": [
                    {
                        "name": os.path.basename(daac_nc_path),
                        "uri": acq.daac_uri_base + daac_nc_path,
                        "type": "data",
                        "size": os.path.getsize(daac_nc_path),
                        "checksumType": "sha512",
                        "checksum": calc_checksum(daac_nc_path, "sha512")
                    },
                    {
                        "name": os.path.basename(daac_ummg_json_path),
                        "uri": acq.daac_uri_base + daac_ummg_json_path,
                        "type": "metadata",
                        "size": os.path.getsize(daac_ummg_json_path),
                        "checksumType": "sha512",
                        "checksum": calc_checksum(daac_ummg_json_path, "sha512")
                    }
                ]
            }
        }

        # Write notification submission to file
        with open(cnm_submission_path, "w") as f:
            json.dump(notification, f)
        wm.change_group_ownership(cnm_submission_path)

        # Submit notification via AWS SQS
        cmd_aws = ["aws", "sqs", "send-message", "--queue-url", wm.config["daac_submission_url"], "--message-body",
                   f"file://{cnm_submission_path}", "--profile", "default"]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
        cnm_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(cnm_submission_path),
                                                            tz=datetime.timezone.utc)

        # Update db with log entry
        dm = wm.database_manager
        if "rdn_daac_submissions" in acq.metadata["products"]["l1b"] and \
                acq.metadata["products"]["l1b"]["rdn_daac_submissions"] is not None:
            acq.metadata["products"]["l1b"]["rdn_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l1b"]["rdn_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {"products.l1b.rdn_daac_submissions": acq.metadata["products"]["l1b"]["rdn_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": [daac_nc_path, daac_ummg_json_path],
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1b_rdn_cnm_submission_path": cnm_submission_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
