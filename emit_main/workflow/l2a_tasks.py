"""
This code contains tasks for executing EMIT Level 2A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os
import time
import numpy as np

import luigi
import spectral.io.envi as envi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BGeolocate
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils.file_checks import envi_header, check_cloudfraction
from emit_utils import daac_converter

logger = logging.getLogger("emit-main")


class L2AReflectance(SlurmJobTask):
    """
    Performs atmospheric correction on radiance
    :returns: Surface reflectance and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    n_cores = 64
    memory = 360000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        return None
        # Don't look up dependencies here since the monitor guarantees that these have already run
        # return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
        #                      partition=self.partition),
        #         L1BGeolocate(config_path=self.config_path, orbit_id=acq.orbit, level=self.level,
        #                      partition=self.partition))

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(f"{self.task_family} run: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2a"]
        
        # Build PGE cmd for surface model
        isofit_pge = wm.pges["isofit"]
        surface_config_path = wm.config["isofit_surface_config"]
        tmp_surface_path = os.path.join(self.local_tmp_dir, f"{acq.acquisition_id}_surface.mat")

        # Generate surface model
        env = os.environ.copy()
        surf_cmd = ["isofit", "surface_model",
                    surface_config_path,
                    f"--wavelength_path={acq.rdn_hdr_path}",
                    f"--output_path={tmp_surface_path}"]
        pge.run(surf_cmd, tmp_dir=self.tmp_dir, env=env)

        # Build PGE cmd for apply_oe
        tmp_log_path = os.path.join(self.local_tmp_dir, "isofit.log")
        model_disc_file = os.path.join(isofit_pge.repo_dir, "data", "emit_model_discrepancy.mat")

        emulator_base = wm.config["isofit_emulator_base"]
        input_files = {
            "radiance_file": acq.rdn_img_path,
            "pixel_locations_file": acq.loc_img_path,
            "observation_parameters_file": acq.obs_img_path,
            "surface_model_config": surface_config_path
        }
        cmd = ["isofit", "-i", wm.config["isofit_ini_path"], "apply_oe", 
               acq.rdn_img_path, acq.loc_img_path, acq.obs_img_path, self.local_tmp_dir, "emit",
               "--presolve",
               "--analytical_line",
               "--emulator_base=" + emulator_base,
               "--n_cores", str(self.n_cores),
               "--surface_path", tmp_surface_path,
               "--ray_temp_dir", "/local/ray",
               "--log_file", tmp_log_path,
               "--logging_level", self.level,
               "--num_neighbors=100",
               "--num_neighbors=10",
               "--num_neighbors=10",
               "--model_discrepancy_path", model_disc_file,
               "--pressure_elevation"]

        env["SIXS_DIR"] = wm.config["isofit_sixs_dir"]
        env["RAY_worker_register_timeout_seconds"] = "600"
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy output files to l2a dir and rename
        tmp_rfl_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_rfl")
        tmp_rfl_png_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_rfl_rgb.png")
        tmp_rfl_hdr_path = envi_header(tmp_rfl_path)
        tmp_rfluncert_path = os.path.join(self.local_tmp_dir, "output",
                                          self.acquisition_id + "_uncert")
        tmp_rfluncert_hdr_path = envi_header(tmp_rfluncert_path)
        tmp_lbl_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_lbl")
        tmp_lbl_hdr_path = envi_header(tmp_lbl_path)
        tmp_statesubs_path = os.path.join(
            self.local_tmp_dir, "output", self.acquisition_id + "_subs_state")
        tmp_statesubsuncert_path = os.path.join(
            self.local_tmp_dir, "output", self.acquisition_id + "_subs_uncert")
        tmp_obssubs_path = os.path.join(
            self.local_tmp_dir, "input", self.acquisition_id + "_subs_obs")
        tmp_locsubs_path = os.path.join(
            self.local_tmp_dir, "input", self.acquisition_id + "_subs_loc")
        tmp_quality_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_rfl_quality.txt")
        tmp_atm_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_atm_interp")

        # ensure that the tmp_rfl_path has a nodata value set, before we make the quicklook
        hdr = envi.read_envi_header(tmp_rfl_hdr_path)
        hdr["data ignore value"] = -9999
        envi.write_envi_header(tmp_rfl_hdr_path, hdr)

        cmd = ["gdal_translate", tmp_rfl_path, tmp_rfl_png_path, "-b", "35", "-b", "23", "-b",
               "11", "-ot", "Byte", "-scale", "-exponent", "0.6", "-of", "PNG", "-co", "ZLEVEL=9"]
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        cmd = ["python", os.path.join(pge.repo_dir, "spectrum_quality.py"), tmp_rfl_path, tmp_quality_path]
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)
        quality_results = np.genfromtxt(tmp_quality_path)

        wm.copy(tmp_rfl_path, acq.rfl_img_path)
        wm.copy(tmp_rfl_hdr_path, acq.rfl_hdr_path)
        wm.copy(tmp_rfluncert_path, acq.rfluncert_img_path)
        wm.copy(tmp_rfluncert_hdr_path, acq.rfluncert_hdr_path)
        wm.copy(tmp_lbl_path, acq.lbl_img_path)
        wm.copy(tmp_lbl_hdr_path, acq.lbl_hdr_path)
        wm.copy(tmp_statesubs_path, acq.statesubs_img_path)
        wm.copy(envi_header(tmp_statesubs_path), acq.statesubs_hdr_path)
        wm.copy(tmp_statesubsuncert_path, acq.statesubsuncert_img_path)
        wm.copy(envi_header(tmp_statesubsuncert_path), acq.statesubsuncert_hdr_path)
        wm.copy(tmp_obssubs_path, acq.obssubs_img_path)
        wm.copy(envi_header(tmp_obssubs_path), acq.obssubs_hdr_path)
        wm.copy(tmp_locsubs_path, acq.locsubs_img_path)
        wm.copy(envi_header(tmp_locsubs_path), acq.locsubs_hdr_path)
        wm.copy(tmp_rfl_png_path, acq.rfl_png_path)
        wm.copy(tmp_quality_path, acq.quality_txt_path)

        wm.copy(tmp_atm_path, acq.atm_img_path)
        wm.copy(envi_header(tmp_atm_path), acq.atm_hdr_path)

        # Copy log file and rename
        log_path = acq.rfl_img_path.replace(".img", "_pge.log")
        wm.copy(tmp_log_path, log_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2A JPL-D 104236, Rev B"
        dm = wm.database_manager
        for img_path, hdr_path in [(acq.rfl_img_path, acq.rfl_hdr_path), (acq.rfluncert_img_path, acq.rfluncert_hdr_path)]:
            hdr = envi.read_envi_header(hdr_path)
            hdr["description"] = "{{EMIT L2A surface reflectance (0-1)}}"
            hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit pge name"] = pge.repo_url
            hdr["emit pge version"] = pge.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd)
            hdr["emit software build version"] = wm.config["extended_build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(img_path), tz=datetime.timezone.utc)
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit data product version"] = wm.config["processing_version"]
            hdr["emit acquisition daynight"] = acq.daynight
            hdr["emit spectral quality"] = '{' + ', '.join(quality_results.astype(str).tolist()) + '}'
            envi.write_envi_header(hdr_path, hdr)

            # Update product dictionary in DB
            product_dict = {
                "img_path": img_path,
                "hdr_path": hdr_path,
                "created": creation_time,
                "dimensions": {
                    "lines": hdr["lines"],
                    "samples": hdr["samples"],
                    "bands": hdr["bands"]
                }
            }
            if "_rfl_" in img_path:
                dm.update_acquisition_metadata(
                    acq.acquisition_id, {"products.l2a.rfl": product_dict})
            elif "_rfluncert_" in img_path:
                dm.update_acquisition_metadata(
                    acq.acquisition_id, {"products.l2a.rfluncert": product_dict})

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2a_rfl_img_path": acq.rfl_img_path,
                "l2a_rfl_hdr_path:": acq.rfl_hdr_path,
                "l2a_rfl_png_path:": acq.rfl_png_path,
                "l2a_rfluncert_img_path": acq.rfluncert_img_path,
                "l2a_rfluncert_hdr_path:": acq.rfluncert_hdr_path,
                "l2a_quality_txt_path:": acq.quality_txt_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)

class L2AMask(SlurmJobTask):
    """
    Creates masks
    :returns: Mask file
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    n_cores = 64
    memory = 360000

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                              partition=self.partition)
        # Only look include reflectance dependency for now.  The monitor guarantees that calibration has run
        # return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
        #                      partition=self.partition),
        #         L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
        #                        partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2a"]

        # Build PGE command for make_masks.py
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        wm.makedirs(tmp_output_dir)

        tmp_mask_path = os.path.join(tmp_output_dir, os.path.basename(acq.mask_img_path))
        tmp_mask_hdr_path = envi_header(tmp_mask_path)
        solar_irradiance_path = os.path.join(pge.repo_dir, "data", "kurudz_0.1nm.dat")
        make_masks_exe = os.path.join(pge.repo_dir, "make_emit_masks.py")

        input_files = {
            "radiance_file": acq.rdn_img_path,
            "pixel_locations_file": acq.loc_img_path,
            "atmosphere_file": acq.atm_img_path,
            "solar_irradiance_file": solar_irradiance_path
        }

        env = os.environ.copy()
        isofit_pge = wm.pges["isofit"]
        emit_utils_pge = wm.pges["emit-utils"]
        env["PYTHONPATH"] = f"$PYTHONPATH:{isofit_pge.repo_dir}:{emit_utils_pge.repo_dir}"

        env["RAY_worker_register_timeout_seconds"] = "600"

        cmd = ["python", make_masks_exe, acq.rdn_img_path, acq.loc_img_path, acq.atm_img_path,
               solar_irradiance_path, tmp_mask_path, "--n_cores", str(self.n_cores)]
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        cloud_fraction = check_cloudfraction(tmp_mask_path)

        # Copy mask files to l2a dir
        wm.copy(tmp_mask_path, acq.mask_img_path)
        wm.copy(tmp_mask_hdr_path, acq.mask_hdr_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2A JPL-D 104236, Rev B"
        hdr = envi.read_envi_header(acq.mask_hdr_path)
        hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["extended_build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.mask_img_path), tz=datetime.timezone.utc)
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit data product version"] = wm.config["processing_version"]
        hdr["emit acquisition daynight"] = acq.daynight
        hdr["emit acquisition cloudfraction"] = cloud_fraction
        envi.write_envi_header(acq.mask_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict = {
            "img_path": acq.mask_img_path,
            "hdr_path": acq.mask_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.mask": product_dict})
        dm.update_acquisition_metadata(acq.acquisition_id, {"cloud_fraction": cloud_fraction})

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2a_mask_img_path": acq.mask_img_path,
                "l2a_mask_hdr_path:": acq.mask_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2AFormat(SlurmJobTask):
    """
    Converts L2A (reflectance, reflectance uncertainty, and masks) to netcdf files
    :returns: L2A netcdf output for delivery
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 18000

    task_namespace = "emit"

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

        pge = wm.pges["emit-sds-l2a"]

        output_generator_exe = os.path.join(pge.repo_dir, "output_conversion.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_daac_rfl_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2a_rfl.nc")
        tmp_daac_rfl_unc_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2a_rfl_unc.nc")
        tmp_daac_mask_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2a_mask.nc")
        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")

        cmd = ["python", output_generator_exe, tmp_daac_rfl_nc_path, tmp_daac_rfl_unc_nc_path,
               tmp_daac_mask_nc_path, acq.rfl_img_path, acq.rfluncert_img_path,
               acq.mask_img_path, acq.bandmask_img_path, acq.loc_img_path, acq.glt_img_path,
               "V0" + str(wm.config["processing_version"]), wm.config["extended_build_num"],
               "--log_file", tmp_log_path]

        # Run this inside the emit-main conda environment to include emit-utils and other requirements
        main_pge = wm.pges["emit-main"]
        main_pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy and rename output files back to /store
        log_path = acq.rfl_nc_path.replace(".nc", "_nc_pge.log")
        wm.copy(tmp_daac_rfl_nc_path, acq.rfl_nc_path)
        wm.copy(tmp_daac_rfl_unc_nc_path, acq.rfluncert_nc_path)
        wm.copy(tmp_daac_mask_nc_path, acq.mask_nc_path)
        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rfl_nc_path), tz=datetime.timezone.utc)
        dm = wm.database_manager
        product_dict_netcdf = {
            "netcdf_rfl_path": acq.rfl_nc_path,
            "netcdf_rfl_unc_path": acq.rfluncert_nc_path,
            "netcdf_mask_path": acq.mask_nc_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.rfl_netcdf": product_dict_netcdf})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "rfl_img_path": acq.rfl_img_path,
                "rfluncert_img_path": acq.rfluncert_img_path,
                "mask_img_path": acq.mask_img_path,
                "loc_img_path": acq.loc_img_path,
                "glt_img_path": acq.glt_img_path
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": "TBD",
            "product_creation_time": nc_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2a_rfl_netcdf_path": acq.rfl_nc_path,
                "l2a_rfl_unc_netcdf_path": acq.rfluncert_nc_path,
                "l2a_mask_netcdf_path": acq.mask_nc_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2ADeliver(SlurmJobTask):
    """
    Stages NetCDF and UMM-G files and submits notification to DAAC interface
    :returns: Staged L2A files
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    daac_ingest_queue = luigi.Parameter(default="forward")
    override_output = luigi.BoolParameter(default=False)

    memory = 18000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L2AFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                         partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")

        if self.override_output:
            return None

        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]

        # Get local SDS names
        # nc_path = acq.rfl_img_path.replace(".img", ".nc")
        ummg_path = acq.rfl_nc_path.replace(".nc", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_rfl_nc_name = f"{acq.rfl_granule_ur}.nc"
        daac_rfluncert_nc_name = f"{acq.rfluncert_granule_ur}.nc"
        daac_mask_nc_name = f"{acq.mask_granule_ur}.nc"
        daac_browse_name = f"{acq.rfl_granule_ur}.png"
        daac_ummg_name = f"{acq.rfl_granule_ur}.cmr.json"
        daac_rfl_nc_path = os.path.join(self.tmp_dir, daac_rfl_nc_name)
        daac_rfluncert_nc_path = os.path.join(self.tmp_dir, daac_rfluncert_nc_name)
        daac_mask_nc_path = os.path.join(self.tmp_dir, daac_mask_nc_name)
        daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(acq.rfl_nc_path, daac_rfl_nc_path)
        wm.copy(acq.rfluncert_nc_path, daac_rfluncert_nc_path)
        wm.copy(acq.mask_nc_path, daac_mask_nc_path)
        wm.copy(acq.rfl_png_path, daac_browse_path)

        # Get the software_build_version (extended build num when product was created)
        hdr = envi.read_envi_header(acq.rfl_hdr_path)
        software_build_version = hdr["emit software build version"]

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.rfl_nc_path), tz=datetime.timezone.utc)
        l2a_pge = wm.pges["emit-sds-l2a"]
        ummg = daac_converter.initialize_ummg(acq.rfl_granule_ur, nc_creation_time, "EMITL2ARFL",
                                              acq.collection_version, acq.start_time,
                                              acq.stop_time, l2a_pge.repo_name, l2a_pge.version_tag,
                                              software_build_version=software_build_version,
                                              software_delivery_version=wm.config["extended_build_num"],
                                              doi=wm.config["dois"]["EMITL2ARFL"], orbit=int(acq.orbit),
                                              orbit_segment=int(acq.scene), scene=int(acq.daac_scene),
                                              solar_zenith=acq.mean_solar_zenith,
                                              solar_azimuth=acq.mean_solar_azimuth,
                                              cloud_fraction=acq.cloud_fraction)
        ummg = daac_converter.add_data_files_ummg(
            ummg,
            [daac_rfl_nc_path, daac_rfluncert_nc_path, daac_mask_nc_path, daac_browse_path],
            acq.daynight,
            ["NETCDF-4", "NETCDF-4", "NETCDF-4", "PNG"])
        # ummg = daac_converter.add_related_url(ummg, l2a_pge.repo_url, "DOWNLOAD SOFTWARE")
        ummg = daac_converter.add_boundary_ummg(ummg, acq.gring)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to S3 for staging
        for path in (daac_rfl_nc_path, daac_rfluncert_nc_path, daac_mask_nc_path, daac_browse_path, daac_ummg_path):
            cmd_aws_s3 = ["ssh", "ngishpc1", "'" + wm.config["aws_cli_exe"], "s3", "cp", path, acq.aws_s3_uri_base,
                          "--profile", wm.config["aws_profile"] + "'"]
            pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.rfl_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l2a_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_rfl_nc_name: os.path.basename(acq.rfl_nc_path),
            daac_rfluncert_nc_name: os.path.basename(acq.rfluncert_nc_path),
            daac_mask_nc_name: os.path.basename(acq.mask_nc_path),
            daac_browse_name: os.path.basename(acq.rfl_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        provider = wm.config["daac_provider_forward"]
        queue_url = wm.config["daac_submission_url_forward"]
        if self.daac_ingest_queue == "backward":
            provider = wm.config["daac_provider_backward"]
            queue_url = wm.config["daac_submission_url_backward"]
        notification = {
            "collection": "EMITL2ARFL",
            "provider": provider,
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.rfl_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_rfl_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_rfl_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_rfl_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_rfl_nc_path, "sha512")
                    },
                    {
                        "name": daac_rfluncert_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_rfluncert_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_rfluncert_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_rfluncert_nc_path, "sha512")
                    },
                    {
                        "name": daac_mask_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_mask_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_mask_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_mask_nc_path, "sha512")
                    },
                    {
                        "name": daac_browse_name,
                        "uri": acq.aws_s3_uri_base + daac_browse_name,
                        "type": "browse",
                        "size": os.path.getsize(daac_browse_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_browse_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": acq.aws_s3_uri_base + daac_ummg_name,
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
        cnm_submission_output = cnm_submission_path.replace(".json", ".out")
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", queue_url, "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"], ">", cnm_submission_output]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
        wm.change_group_ownership(cnm_submission_output)
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
                "granule_ur": acq.rfl_granule_ur,
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
                "submission_queue": queue_url,
                "submission_status": "submitted"
            }
            dm.insert_granule_report(delivery_report)

        # Update db with log entry
        product_dict_ummg = {
            "ummg_json_path": ummg_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(ummg_path), tz=datetime.timezone.utc)
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.rfl_ummg": product_dict_ummg})

        if "rfl_daac_submissions" in acq.metadata["products"]["l2a"] and \
                acq.metadata["products"]["l2a"]["rfl_daac_submissions"] is not None:
            acq.metadata["products"]["l2a"]["rfl_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l2a"]["rfl_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {"products.l2a.rfl_daac_submissions": acq.metadata["products"]["l2a"]["rfl_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "rfl_netcdf_path": acq.rfl_nc_path,
                "rfluncert_netcdf_path": acq.rfluncert_nc_path,
                "mask_netcdf_path": acq.mask_nc_path,
                "rfl_png_path": acq.rfl_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2a_rfl_ummg_path:": ummg_path,
                "l2a_rfl_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
