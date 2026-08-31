"""
This code contains tasks for executing EMIT Level 3 PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os
import time

import luigi
import spectral.io.envi as envi
import h5netcdf.legacyapi as netCDF4

from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils import daac_converter

logger = logging.getLogger("emit-main")


class L3ReflectanceFormat(SlurmJobTask):
    """
    Converts L2A (reflectance, reflectance uncertainty, and masks) to L3 netcdf files
    :returns: L3 netcdf output for delivery
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    n_cores = 16
    memory = 90000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family,
                                 product_version=wm.config["prod_versions"]["l3rfl"])

    def work(self):

        pge_start_time = time.time()
        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition

        pge = wm.pges["emit-sds-l3rfl"]

        output_generator_exe = os.path.join(pge.repo_dir, "output_conversion.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_daac_rfl_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l3_rfl.nc")
        tmp_daac_rfl_unc_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l3_rfl_unc.nc")
        tmp_daac_obs_nc_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l3_obs.nc")
        tmp_daac_browse_png_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l3_rfl.png")
        tmp_daac_rfl_sidecar_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l3_rfl.json")

        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")

        # --chunksize 10 256 256 --compress --complevel 1 --max_workers
        cmd = ["python", 
               output_generator_exe, 
               tmp_daac_rfl_nc_path, 
               tmp_daac_rfl_unc_nc_path,
               tmp_daac_obs_nc_path,
               tmp_daac_browse_png_path,
               acq.rfl_img_path, 
               acq.rfluncert_img_path, 
               acq.state_img_path,
               acq.maskTf_img_path, 
               acq.loc_img_path, 
               acq.obs_img_path,
               "V0" + str(wm.config["prod_versions"]["l3rfl"]), 
               wm.config["extended_build_num"],
               "--log_file", tmp_log_path,
               "--chunksize 10 256 256",
               "--compress",
               "--complevel 1",
               "--max_workers 16",
               "--sidecar"]

        # Run this inside the emit-l3rfl conda environment will need to include emit-utils and other requirements
        main_pge = wm.pges["emit-sds-l3rfl"]
        main_pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy and rename output files back to /store
        log_path = acq.l3rfl_nc_path.replace(".nc", "_nc_pge.log")
        rfl_sidecar_path = acq.l3rfl_nc_path.replace(".nc", ".sidecar.json")
        wm.copy(tmp_daac_rfl_nc_path, acq.l3rfl_nc_path)
        wm.copy(tmp_daac_rfl_unc_nc_path, acq.l3rfluncert_nc_path)
        wm.copy(tmp_daac_obs_nc_path, acq.l3obs_nc_path)
        wm.copy(tmp_daac_browse_png_path, acq.l3rfl_png_path)
        wm.copy(tmp_daac_rfl_sidecar_path, rfl_sidecar_path)

        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.l3rfl_nc_path), tz=datetime.timezone.utc)
        dm = wm.database_manager
        product_dict_netcdf = {
            "netcdf_l3rfl_path": acq.l3rfl_nc_path,
            "netcdf_l3rfl_unc_path": acq.l3rfluncert_nc_path,
            "netcdf_l3obs_path": acq.l3obs_nc_path,
            "json_l3rflsidecar_path": rfl_sidecar_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.l3rfl.{wm.config['prod_versions']['l3rfl']}.rfl_netcdf": product_dict_netcdf})

        total_time = time.time() - pge_start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "rfl_img_path": acq.rfl_img_path,
                "rfluncert_img_path": acq.rfluncert_img_path,
                "state_img_path": acq.state_img_path,
                "maskTf_img_path": acq.maskTf_img_path,
                "loc_img_path": acq.loc_img_path,
                "obs_img_path": acq.obs_img_path
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": "TBD",
            "product_creation_time": nc_creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "product_version": wm.config["prod_versions"]["l3rfl"],
            "output": {
                "l3_rfl_netcdf_path": acq.l3rfl_nc_path,
                "l3_rfl_unc_netcdf_path": acq.l3rfluncert_nc_path,
                "l3_obs_netcdf_path": acq.l3obs_nc_path,
                "l3_rfl_png_path": acq.l3rfl_png_path,
                "l3_rfl_sidecar_path": rfl_sidecar_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L3ReflectanceDeliver(SlurmJobTask):
    """
    Stages NetCDF and UMM-G files and submits notification to DAAC interface
    :returns: Staged L3 Reflectance files
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
        return L3ReflectanceFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                         partition=self.partition)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")

        if self.override_output:
            return None

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family,
                                 product_version=wm.config["prod_versions"]["l3rfl"])

    def work(self):

        pge_start_time = time.time()
        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]

        # Get local SDS names
        ummg_path = acq.l3rfl_nc_path.replace(".nc", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_rfl_nc_name = f"{acq.l3rfl_granule_ur}.nc"
        daac_rfluncert_nc_name = f"{acq.l3rfluncert_granule_ur}.nc"
        daac_obs_nc_name = f"{acq.l3obs_granule_ur}.nc"
        daac_rflsidecar_name = f"{acq.l3rfl_granule_ur}.sidecar.json"
        daac_rflbrowse_name = f"{acq.l3rfl_granule_ur}.png"
        daac_ummg_name = f"{acq.l3rfl_granule_ur}.cmr.json"
        daac_rfl_nc_path = os.path.join(self.tmp_dir, daac_rfl_nc_name)
        daac_rfluncert_nc_path = os.path.join(self.tmp_dir, daac_rfluncert_nc_name)
        daac_obs_nc_path = os.path.join(self.tmp_dir, daac_obs_nc_name)
        daac_rflsidecar_path = os.path.join(self.tmp_dir, daac_rflsidecar_name)
        daac_rflbrowse_path = os.path.join(self.tmp_dir, daac_rflbrowse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        rfl_sidecar_path = acq.l3rfl_nc_path.replace(".nc", ".sidecar.json")
        wm.copy(acq.l3rfl_nc_path, daac_rfl_nc_path)
        wm.copy(acq.l3rfluncert_nc_path, daac_rfluncert_nc_path)
        wm.copy(acq.l3obs_nc_path, daac_obs_nc_path)
        wm.copy(rfl_sidecar_path, daac_rflsidecar_path)
        wm.copy(acq.l3rfl_png_path, daac_rflbrowse_path)

        # Get the software_build_version (extended build num when product was created)
        nc_ds = netCDF4.Dataset(acq.l3rfl_nc_path, 'r+')
        software_build_version = nc_ds.software_build_version

        # Use a cloud fraction that sums the nodata fraction (clouds screened on board) and the cloud fraction value
        # from the maskTf step.  These fractions are rounded separately.  Use min to ensure it doesn't go over 100.
        cloud_fraction = acq.metadata["products"]["mask"][wm.config["prod_versions"]["mask"]]["maskTf"]["cloud_fraction"]
        nodata_fraction = acq.metadata["products"]["mask"][wm.config["prod_versions"]["mask"]]["maskTf"]["nodata_fraction"]
        cloud_cover = min(cloud_fraction + nodata_fraction, 100)
        
        # Create the UMM-G file
        collection_version = f"0{wm.config['prod_versions']['l3rfl']}"
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.l3rfl_nc_path), tz=datetime.timezone.utc)
        l3rfl_pge = wm.pges["emit-sds-l3rfl"]
        ummg = daac_converter.initialize_ummg(acq.l3rfl_granule_ur, nc_creation_time, "EMITL3RFL",
                                              collection_version, acq.start_time,
                                              acq.stop_time, l3rfl_pge.repo_name, l3rfl_pge.version_tag,
                                              software_build_version=software_build_version,
                                              software_delivery_version=wm.config["extended_build_num"],
                                              doi=wm.config["dois"]["EMITL3RFL"], orbit=int(acq.orbit),
                                              orbit_segment=int(acq.scene), scene=int(acq.daac_scene),
                                              solar_zenith=acq.mean_solar_zenith,
                                              solar_azimuth=acq.mean_solar_azimuth,
                                              cloud_cover=cloud_cover)
        ummg = daac_converter.add_data_files_ummg(
            ummg,
            [daac_rfl_nc_path, daac_rfluncert_nc_path, daac_obs_nc_path, daac_rflsidecar_path, daac_rflbrowse_path],
            acq.daynight,
            ["NETCDF-4", "NETCDF-4", "NETCDF-4", "JSON", "PNG"])
        ummg = daac_converter.add_boundary_ummg(ummg, acq.gring)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to S3 for staging
        for path in (daac_rfl_nc_path, daac_rfluncert_nc_path, daac_obs_nc_path, daac_rflsidecar_path, daac_rflbrowse_path, daac_ummg_path):
            cmd_aws_s3 = ["ssh", "ngishpc1", "'" + wm.config["aws_cli_exe"], "s3", "cp", path, acq.aws_s3_uri_base,
                          "--profile", wm.config["aws_profile"] + "'"]
            pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.l3rfl_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l3rfl_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_rfl_nc_name: os.path.basename(acq.l3rfl_nc_path),
            daac_rfluncert_nc_name: os.path.basename(acq.l3rfluncert_nc_path),
            daac_obs_nc_name: os.path.basename(acq.l3obs_nc_path),
            daac_rflsidecar_name: os.path.basename(rfl_sidecar_path),
            daac_rflbrowse_name: os.path.basename(acq.l3rfl_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        provider = wm.config["daac_provider_forward"]
        queue_url = wm.config["daac_submission_url_forward"]
        if self.daac_ingest_queue == "backward":
            provider = wm.config["daac_provider_backward"]
            queue_url = wm.config["daac_submission_url_backward"]
        notification = {
            "collection": "EMITL3RFL",
            "provider": provider,
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.l3rfl_granule_ur,
                "dataVersion": collection_version,
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
                        "name": daac_obs_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_obs_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_obs_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_obs_nc_path, "sha512")
                    },
                    {
                        "name": daac_rflsidecar_name,
                        "uri": acq.aws_s3_uri_base + daac_rflsidecar_name,
                        "type": "data",
                        "size": os.path.getsize(daac_rflsidecar_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_rflsidecar_path, "sha512")
                    },
                    {
                        "name": daac_rflbrowse_name,
                        "uri": acq.aws_s3_uri_base + daac_rflbrowse_name,
                        "type": "browse",
                        "size": os.path.getsize(daac_rflbrowse_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_rflbrowse_path, "sha512")
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
        cmd_aws = ["ssh", "ngishpc1", "'" + wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", queue_url, "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"], ">", cnm_submission_output + "'"]
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
                "granule_ur": acq.l3rfl_granule_ur,
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
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.l3rfl.{wm.config['prod_versions']['l3rfl']}.rfl_ummg": product_dict_ummg})

        if "rfl_daac_submissions" in acq.metadata["products"]["l3rfl"][wm.config["prod_versions"]["l3rfl"]] and \
                acq.metadata["products"]["l3rfl"][wm.config["prod_versions"]["l3rfl"]]["rfl_daac_submissions"] is not None:
            acq.metadata["products"]["l3rfl"][wm.config["prod_versions"]["l3rfl"]]["rfl_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l3rfl"][wm.config["prod_versions"]["l3rfl"]]["rfl_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {f"products.l3rfl.{wm.config['prod_versions']['l3rfl']}.rfl_daac_submissions": acq.metadata["products"]["l3rfl"][wm.config["prod_versions"]["l3rfl"]]["rfl_daac_submissions"]})

        total_time = time.time() - pge_start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "l3rfl_netcdf_path": acq.l3rfl_nc_path,
                "l3rfluncert_netcdf_path": acq.l3rfluncert_nc_path,
                "l3obs_netcdf_path": acq.l3obs_nc_path,
                "l3rfl_sidecar_path": rfl_sidecar_path,
                "l3rfl_png_path": acq.l3rfl_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "product_version": wm.config["prod_versions"]["l3rfl"],
            "output": {
                "l3_rfl_ummg_path:": ummg_path,
                "l3_rfl_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
