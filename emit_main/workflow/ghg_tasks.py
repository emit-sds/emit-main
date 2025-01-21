"""
This code contains tasks for executing EMIT Methane Detection.

Authors:  Philip G. Brodrick, philip.brodrick@jpl.nasa.gov
          Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os
import time

import luigi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget, OrbitTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BGeolocate

from emit_utils.file_checks import envi_header

logger = logging.getLogger("emit-main")


class GHG(SlurmJobTask):
    """
    Performs greenhouse gas mapping on the EMIT SDS
    :returns: Matched filter form greenhouse gas estimation
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    dark_path = luigi.Parameter(default="")

    n_cores = 64
    memory = 360000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        # Don't look up dependencies here since the monitor guarantees that these have already run
        # return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
        #                      partition=self.partition),
        #         L1BGeolocate(config_path=self.config_path, orbit_id=acq.orbit, level=self.level,
        #                      partition=self.partition),
        #         L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
        #                      partition=self.partition),)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(f"{self.task_family} run: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-ghg"]
        emit_utils_pge = wm.pges["emit-utils"]
        dm = wm.database_manager

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.local_tmp_dir, "ghg")
        wm.makedirs(tmp_output_dir)
        env = os.environ.copy()
        env["RAY_worker_register_timeout_seconds"] = "600"
        env["PYTHONPATH"] = f"$PYTHONPATH:{pge.repo_dir}:{emit_utils_pge.repo_dir}"

        from files import Filenames # This might now work without path mod

        # Definte exe's
        process_exe = os.path.join(pge.repo_dir, "ghg_process.py")

        # Define local output files
        co2_target_file = os.path.join(tmp_output_dir, "co2_target.txt")
        ch4_target_file = os.path.join(tmp_output_dir, "ch4_target.txt")
        co2_log_file = os.path.join(tmp_output_dir, "co2_log.txt")
        ch4_log_file = os.path.join(tmp_output_dir, "ch4_log.txt")

        ch4_base = os.path.join(self.tmp_dir, acq.acquisition_id + '_ch4')
        co2_base = os.path.join(self.tmp_dir, acq.acquisition_id + '_co2')

        noise_file = os.path.join(pge.repo_dir, "instrument_noise_parameters","emit_noise.txt")

        input_files = {
            "radiance_file": acq.rdn_img_path,
            "obs_file": acq.obs_img_path,
            "loc_file": acq.loc_img_path,
            "glt_file": acq.glt_img_path,
            "bandmask_file": acq.bandmask_img_path,
            "mask_file": acq.mask_img_file,
            "state_subs_file": acq.statesubs_img_path,
        }

        # Create commands
        cmd = ["python", process_exe,
               acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path, acq.glt_img_path,
               acq.bandmask_img_path, acq.mask_img_file, ch4_base,
               '--state_subs', acq.statesubs_img_path,
               "--noise_file",noise_file,
               ]
        ch4_cmd = cmd + ['--lut_file', wm.config["ch4_lut_file"], "--logfile", ch4_log_file]
        co2_cmd = cmd + ['--lut_file', wm.config["co2_lut_file"], "--logfile", co2_log_file, "--co2"]
        co2_cmd[8] = co2_base

        # Run CH4
        pge.run(ch4_cmd, tmp_dir=self.tmp_dir, env=env)
        ch4_of = Filenames(ch4_base)

        # MF - CH4
        wm.copy(ch4_of.mf_file, acq.ch4_img_path)
        wm.copy(envi_header(ch4_of.mf_file), acq.ch4_hdr_path)
        wm.copy(ch4_of.mf_ort_file, acq.ortch4_tif_path)

        # Sensitivity - CH4
        wm.copy(ch4_of.mf_sens_file, acq.sensch4_img_path)
        wm.copy(envi_header(ch4_of.mf_sens_file), acq.sensch4_hdr_path)
        wm.copy(ch4_of.sens_ort_file, acq.sensortch4_tif_path)

        # Uncertainty - CH4
        wm.copy(ch4_of.mf_uncert_file, acq.uncertch4_img_path)
        wm.copy(envi_header(ch4_of.mf_uncert_file), acq.uncertch4_hdr_path)
        wm.copy(ch4_of.uncert_ort_file, acq.uncertortch4_tif_path)

        # Update db
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4": {
                "img_path" : acq.ch4_img_path,
                "hdr_path" : acq.ch4_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.sensch4": {
                "img_path" : acq.sensch4_img_path,
                "hdr_path" : acq.sensch4_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.uncertch4": {
                "img_path" : acq.uncertch4_img_path,
                "hdr_path" : acq.uncertch4_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ortch4": {
                "tif_path" : acq.ortch4_img_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ortsensch4": {
                "tif_path" : acq.ortsensch4_img_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ortuncertch4": {
                "tif_path" : acq.ortuncertch4_img_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})

        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.ortuncertch4_img_path), tz=datetime.timezone.utc)

        doc_version = "EMIT SDS L2B JPL-D........."  #TODO: Placeholder

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(ch4_cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "ghg_ch4_img_path": acq.ch4_img_path,
                "ghg_ch4_hdr_path:": acq.ch4_hdr_path,
                "ghg_sensch4_img_path": acq.sensch4_img_path,
                "ghg_sensch4_hdr_path": acq.sensch4_hdr_path,
                "ghg_uncertch4_img_path": acq.uncertch4_img_path,
                "ghg_uncertch4_hdr_path": acq.uncertch4_hdr_path,
                "ghg_ortch4_tif_path": acq.ortch4_tif_path,
                "ghg_ortsensch4_tif_path": acq.ortsensch4_tif_path,
                "ghg_ortuncertch4_tif_path": acq.ortuncertch4_tif_path,
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)

        ############ CO2  ###############
        start_time = time.time()
        pge.run(co2_cmd, tmp_dir=self.tmp_dir, env=env)
        co2_of = Filenames(co2_base)

        # MF - CO2
        wm.copy(co2_of.mf_file, acq.co2_img_path)
        wm.copy(envi_header(co2_of.mf_file), acq.co2_hdr_path)
        wm.copy(co2_of.mf_ort_file, acq.ortco2_tif_path)

        # Sensitivity - CO2
        wm.copy(co2_of.mf_sens_file, acq.sensco2_img_path)
        wm.copy(envi_header(co2_of.mf_sens_file), acq.sensco2_hdr_path)
        wm.copy(co2_of.sens_ort_file, acq.sensortco2_tif_path)

        # Uncertainty - CO2
        wm.copy(co2_of.mf_uncert_file, acq.uncertco2_img_path)
        wm.copy(envi_header(co2_of.mf_uncert_file), acq.uncertco2_hdr_path)
        wm.copy(co2_of.uncert_ort_file, acq.uncertortco2_tif_path)

        # Update db
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2": {
                "img_path" : acq.co2_img_path,
                "hdr_path" : acq.co2_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.sensco2": {
                "img_path" : acq.sensco2_img_path,
                "hdr_path" : acq.sensco2_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.uncertco2": {
                "img_path" : acq.uncertco2_img_path,
                "hdr_path" : acq.uncertco2_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ortco2": {
                "tif_path" : acq.ortco2_img_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ortsensco2": {
                "tif_path" : acq.ortsensco2_img_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ortuncertco2": {
                "tif_path" : acq.ortuncertco2_img_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})

        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.ortuncertco2_img_path), tz=datetime.timezone.utc)

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(co2_cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "ghg_co2_img_path": acq.co2_img_path,
                "ghg_co2_hdr_path:": acq.co2_hdr_path,
                "ghg_sensco2_img_path": acq.sensco2_img_path,
                "ghg_sensco2_hdr_path": acq.sensco2_hdr_path,
                "ghg_uncertco2_img_path": acq.uncertco2_img_path,
                "ghg_uncertco2_hdr_path": acq.uncertco2_hdr_path,
                "ghg_ortco2_tif_path": acq.ortco2_tif_path,
                "ghg_ortsensco2_tif_path": acq.ortsensco2_tif_path,
                "ghg_ortuncertco2_tif_path": acq.ortuncertco2_tif_path,
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
