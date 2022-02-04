"""
This code contains tasks for executing EMIT Level 2A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os

import luigi
import spectral.io.envi as envi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BGeolocate
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils.file_checks import envi_header
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

    n_cores = 40
    memory = 180000
    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                             partition=self.partition),
                L1BGeolocate(config_path=self.config_path, orbit_id=acq.orbit, level=self.level,
                             partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2a"]

        # Build PGE cmd
        apply_oe_exe = os.path.join(wm.pges["isofit"].repo_dir, "isofit", "utils", "apply_oe.py")
        tmp_log_path = os.path.join(self.local_tmp_dir, "isofit.log")
        wavelength_path = wm.config["isofit_wavelength_path"]
        surface_path = wm.config["isofit_surface_path"]
        emulator_base = wm.config["isofit_emulator_base"]
        input_files = {
            "radiance_file": acq.rdn_img_path,
            "pixel_locations_file": acq.loc_img_path,
            "observation_parameters_file": acq.obs_img_path,
            "wavelength_file": wavelength_path,
            "surface_file": surface_path
        }
        cmd = ["python", apply_oe_exe, acq.rdn_img_path, acq.loc_img_path, acq.obs_img_path, self.local_tmp_dir, "emit",
               "--presolve=1", "--empirical_line=1", "--emulator_base=" + emulator_base,
               "--n_cores", str(self.n_cores),
               # "--wavelength_path", wavelength_path,
               "--surface_path", surface_path,
               "--ray_temp_dir", "/tmp/ray-" + os.path.basename(self.local_tmp_dir),
               "--log_file", tmp_log_path,
               "--logging_level", self.level]

        env = os.environ.copy()
        env["SIXS_DIR"] = wm.config["isofit_sixs_dir"]
        env["EMULATOR_DIR"] = emulator_base
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy output files to l2a dir and rename
        tmp_rfl_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_rfl")
        tmp_rfl_hdr_path = envi_header(tmp_rfl_path)
        tmp_uncert_path = os.path.join(self.local_tmp_dir, "output",
                                       self.acquisition_id + "_uncert")
        tmp_uncert_hdr_path = envi_header(tmp_uncert_path)
        tmp_lbl_path = os.path.join(self.local_tmp_dir, "output", self.acquisition_id + "_lbl")
        tmp_lbl_hdr_path = envi_header(tmp_lbl_path)
        tmp_statesubs_path = os.path.join(
            self.local_tmp_dir, "output", self.acquisition_id + "_subs_state")
        tmp_statesubs_hdr_path = envi_header(tmp_statesubs_path)
        wm.copy(tmp_rfl_path, acq.rfl_img_path)
        wm.copy(tmp_rfl_hdr_path, acq.rfl_hdr_path)
        wm.copy(tmp_uncert_path, acq.uncert_img_path)
        wm.copy(tmp_uncert_hdr_path, acq.uncert_hdr_path)
        wm.copy(tmp_lbl_path, acq.lbl_img_path)
        wm.copy(tmp_lbl_hdr_path, acq.lbl_hdr_path)
        wm.copy(tmp_statesubs_path, acq.statesubs_img_path)
        wm.copy(tmp_statesubs_hdr_path, acq.statesubs_hdr_path)

        # Copy log file and rename
        log_path = acq.rfl_img_path.replace(".img", "_pge.log")
        wm.copy(tmp_log_path, log_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2A JPL-D 104236, Rev B"
        dm = wm.database_manager
        for img_path, hdr_path in [(acq.rfl_img_path, acq.rfl_hdr_path), (acq.uncert_img_path, acq.uncert_hdr_path)]:
            hdr = envi.read_envi_header(hdr_path)
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
            elif "_uncert_" in img_path:
                dm.update_acquisition_metadata(
                    acq.acquisition_id, {"products.l2a.uncert": product_dict})

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
                "l2a_rfl_img_path": acq.rfl_img_path,
                "l2a_rfl_hdr_path:": acq.rfl_hdr_path,
                "l2a_uncert_img_path": acq.uncert_img_path,
                "l2a_uncert_hdr_path:": acq.uncert_hdr_path
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

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                             partition=self.partition),
                L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                               partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2a"]

        # Build PGE command for make_masks.py
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        wm.makedirs(tmp_output_dir)

        tmp_rho_path = os.path.join(tmp_output_dir, self.acquisition_id + "_rho")
        tmp_mask_path = os.path.join(tmp_output_dir, os.path.basename(acq.mask_img_path))
        tmp_mask_hdr_path = envi_header(tmp_mask_path)
        solar_irradiance_path = os.path.join(pge.repo_dir, "data", "kurudz_0.1nm.dat")
        make_masks_exe = os.path.join(pge.repo_dir, "make_emit_masks.py")

        input_files = {
            "radiance_file": acq.rdn_img_path,
            "pixel_locations_file": acq.loc_img_path,
            "subset_labels_file": acq.lbl_img_path,
            "state_subset_file": acq.statesubs_img_path,
            "solar_irradiance_file": solar_irradiance_path
        }

        cmd = ["python", make_masks_exe, acq.rdn_img_path, acq.loc_img_path, acq.lbl_img_path, acq.statesubs_img_path,
               solar_irradiance_path, tmp_rho_path, tmp_mask_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

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

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return (L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                               partition=self.partition),
                L2AMask(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                        partition=self.partition))

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
        tmp_daac_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2a.nc")
        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")

        cmd = ["python", output_generator_exe, tmp_daac_nc_path, acq.rfl_img_path, acq.uncert_img_path,
               acq.mask_img_path, acq.loc_img_path, acq.glt_img_path, "--log_file",
               tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy and rename output files back to /store
        nc_path = acq.rfl_img_path.replace(".img", ".nc")
        log_path = nc_path.replace(".nc", "_nc_pge.log")
        wm.copy(tmp_daac_nc_path, nc_path)
        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(nc_path), tz=datetime.timezone.utc)
        dm = wm.database_manager
        product_dict_netcdf = {
            "netcdf_path": nc_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2a.rfl_netcdf": product_dict_netcdf})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "rfl_img_path": acq.rfl_img_path,
                "rfl_unert_img_path": acq.uncert_img_path,
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
                "l2a_rfl_netcdf_path": nc_path
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

    task_namespace = "emit"
    n_cores = 1
    memory = 30000

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L2AFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                         partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]

        # Get local SDS names
        nc_path = acq.rfl_img_path.replace(".img", ".nc")
        ummg_path = nc_path.replace(".nc", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_nc_name = f"{acq.rfl_granule_ur}.nc"
        daac_ummg_name = f"{acq.rfl_granule_ur}.cmr.json"
        daac_nc_path = os.path.join(self.tmp_dir, daac_nc_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(nc_path, daac_nc_path)

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(nc_path), tz=datetime.timezone.utc)
        daynight = "Day" if acq.submode == "science" else "Night"
        ummg = daac_converter.initialize_ummg(acq.rfl_granule_ur, nc_creation_time, "EMITL2ARFL")
        ummg = daac_converter.add_data_file_ummg(ummg, daac_nc_path, daynight)
        # TODO: Add browse image
        # TODO: replace w/ database read or read from L1B Geolocate PGE
        tmp_boundary_points_list = [[-118.53, 35.85], [-118.53, 35.659], [-118.397, 35.659], [-118.397, 35.85]]
        ummg = daac_converter.add_boundary_ummg(ummg, tmp_boundary_points_list)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={acq.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{acq.daac_staging_dir}/"
        group = f"emit-{wm.config['environment']}" if wm.config["environment"] in ("test", "ops") else "emit-dev"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{acq.daac_staging_dir}'", "];", "then", "mkdir", f"{acq.daac_staging_dir};", "chgrp",
                           group, f"{acq.daac_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)

        for path in (daac_nc_path, daac_ummg_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.rfl_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l2a_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_nc_name: os.path.basename(nc_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        notification = {
            "collection": "EMITL2ARFL",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.rfl_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_nc_name,
                        "uri": acq.daac_uri_base + daac_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_nc_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": acq.daac_uri_base + daac_ummg_name,
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
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", wm.config["daac_submission_url"], "--message-body",
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
                "netcdf_path": nc_path
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
