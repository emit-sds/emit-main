"""
This code contains tasks for executing EMIT Level 2B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os
import sys
import time

import luigi
import spectral.io.envi as envi

from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.ghg_tasks import read_gdal_metadata
from emit_main.workflow.l2a_tasks import L2AReflectance
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils.file_checks import envi_header
from emit_utils import daac_converter

logger = logging.getLogger("emit-main")


class L2BMineral(SlurmJobTask):
    """
    Creates L2B mineral identification and band depth and uncertainties
    :returns: Mineral identification file and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 30000

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return (L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                               partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2b"]

        # Build PGE commands for run_tetracorder_pge.sh
        run_tetra_exe = os.path.join(pge.repo_dir, "run_tetracorder_pge.sh")
        env = os.environ.copy()
        env["SP_LOCAL"] = wm.config["specpr_path"]
        env["SP_BIN"] = "${SP_LOCAL}/bin"
        env["TETRA"] = wm.config["tetracorder_path"]
        env["TETRA_CMDS"] = wm.config["tetracorder_cmds_path"]
        env["PATH"] = "${PATH}:${SP_LOCAL}/bin:${TETRA}/bin:/usr/bin"

        # This has to be a bit truncated because of character limitations
        tmp_rfl_path = os.path.join(self.local_tmp_dir, 'r')
        tmp_rfl_path_hdr = envi_header(tmp_rfl_path)

        wm.symlink(acq.rfl_img_path, tmp_rfl_path)
        wm.symlink(acq.rfl_hdr_path, tmp_rfl_path_hdr)

        # This has to be a bit truncated because of character limitations
        tmp_tetra_output_path = os.path.join(self.local_tmp_dir, os.path.basename(acq.min_img_path).split('_')[0] + '_tetra')
        tmp_tetra_output_path_tar = tmp_tetra_output_path + '.tar'

        cmd_tetra_setup = [os.path.join(wm.config["tetracorder_cmds_path"], 'cmd-setup-tetrun'), tmp_tetra_output_path,
                           wm.config["tetracorder_library_cmdname"], "cube", tmp_rfl_path, "1", "-T", "-20", "80", "C",
                           "-P", ".5", "1.5", "bar"]
        pge.run(cmd_tetra_setup, tmp_dir=self.tmp_dir, env=env)

        current_pwd = os.getcwd()
        os.chdir(tmp_tetra_output_path)
        cmd_tetra = [os.path.join(tmp_tetra_output_path, "cmd.runtet"), "cube", tmp_rfl_path, 'band', '20', 'gif']
        pge.run(cmd_tetra, tmp_dir=self.tmp_dir, env=env)
        os.chdir(current_pwd)

        # Build aggregator cmd
        aggregator_exe = os.path.join(pge.repo_dir, "group_aggregator.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "l2b_aggregation_output")
        wm.makedirs(tmp_output_dir)
        tmp_min_path = os.path.join(tmp_output_dir, os.path.basename(acq.min_img_path))
        tmp_min_unc_path = os.path.join(tmp_output_dir, os.path.basename(acq.minuncert_img_path))
        tmp_quicklook_path = os.path.join(tmp_output_dir, os.path.splitext(os.path.basename(acq.min_img_path))[0] + '_quicklook.png')
        standard_library = os.path.join(
            wm.config['tetracorder_library_dir'], f's{wm.config["tetracorder_library_basename"]}_envi')
        research_library = os.path.join(
            wm.config['tetracorder_library_dir'], f'r{wm.config["tetracorder_library_basename"]}_envi')
        tetracorder_config_file = os.path.join(wm.config['tetracorder_cmds_path'], wm.config["tetracorder_config_filename"])
        min_group_mat_file = os.path.join(pge.repo_dir, 'data', wm.config["mineral_matrix_name"])

        input_files = {
            "reflectance_file": acq.rfl_img_path,
            "reflectance_uncertainty_file": acq.rfluncert_img_path,
            "tetracorder_library_basename": wm.config["tetracorder_library_basename"],
            "mineral_group_mat_file": min_group_mat_file,
            "tetracorder_config_filename": tetracorder_config_file
        }

        env = os.environ.copy()
        emit_utils_pge = wm.pges["emit-utils"]
        env["PYTHONPATH"] = f"$PYTHONPATH:{emit_utils_pge.repo_dir}"
        agg_cmd = ["python", aggregator_exe, tmp_tetra_output_path, min_group_mat_file, tmp_min_path, tmp_min_unc_path,
                   "--calculate_uncertainty",
                   "--reflectance_file", acq.rfl_img_path,
                   "--reflectance_uncertainty_file", acq.rfluncert_img_path,
                   "--reference_library", standard_library,
                   "--research_library", research_library,
                   "--expert_system_file", tetracorder_config_file,
                   ]
        pge.run(agg_cmd, cwd=pge.repo_dir, tmp_dir=self.tmp_dir, env=env)

        ql_cmd = ['python', os.path.join(pge.repo_dir, 'quicklook.py'), tmp_min_path, tmp_quicklook_path, '--unc_file', tmp_min_unc_path]
        pge.run(ql_cmd, cwd=pge.repo_dir, tmp_dir=self.tmp_dir, env=env)

        # tar l2b
        tar_cmd = ['tar', '-C', self.local_tmp_dir, '-cf', tmp_tetra_output_path_tar, os.path.basename(tmp_tetra_output_path)]
        pge.run(tar_cmd, cwd=pge.repo_dir, tmp_dir=self.tmp_dir, env=env)

        # Copy mask files to l2a dir
        wm.copy(tmp_tetra_output_path_tar, acq.tetra_dir_path)
        wm.copy(tmp_min_path, acq.min_img_path)
        wm.copy(envi_header(tmp_min_path), acq.min_hdr_path)
        wm.copy(tmp_min_unc_path, acq.minuncert_img_path)
        wm.copy(envi_header(tmp_min_unc_path), acq.minuncert_hdr_path)
        wm.copy(tmp_quicklook_path, acq.min_png_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2B JPL-D 104237, Rev A"
        for img_path, hdr_path in [(acq.min_img_path, acq.min_hdr_path),
                                   (acq.minuncert_img_path, acq.minuncert_hdr_path)]:
            hdr = envi.read_envi_header(hdr_path)
            hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit pge name"] = pge.repo_url
            hdr["emit pge version"] = pge.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd_tetra_setup) + ", " + " ".join(agg_cmd)
            hdr["emit software build version"] = wm.config["extended_build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(img_path), tz=datetime.timezone.utc)
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit data product version"] = wm.config["prod_versions"]["l2b"]
            hdr["emit acquisition daynight"] = acq.daynight
            envi.write_envi_header(hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict = {
            "img_path": acq.min_img_path,
            "hdr_path": acq.min_hdr_path,
            "png_path": acq.min_png_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.l2b.{wm.config['prod_versions']['l2b']}.min": product_dict})

        product_dict_minuncert = {
            "img_path": acq.minuncert_img_path,
            "hdr_path": acq.minuncert_hdr_path,
            "created": creation_time,
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.l2b.{wm.config['prod_versions']['l2b']}.minuncert": product_dict_minuncert})

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd_tetra_setup) + ", " + " ".join(agg_cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_min_img_path": acq.min_img_path,
                "l2b_min_hdr_path:": acq.min_hdr_path,
                "l2b_min_png_path:": acq.min_png_path,
                "l2b_minuncert_img_path": acq.minuncert_img_path,
                "l2b_minuncert_hdr_path:": acq.minuncert_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2BFormat(SlurmJobTask):
    """
    Converts L2B (mineral identification, band depth, and uncertainty) to netcdf files
    :returns: L2B netcdf output for delivery
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

        pge = wm.pges["emit-sds-l2b"]

        output_generator_exe = os.path.join(pge.repo_dir, "group_output_conversion.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_daac_nc_min_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2b_min.nc")
        tmp_daac_nc_minuncert_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2b_minuncert.nc")
        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")

        env = os.environ.copy()
        emit_utils_pge = wm.pges["emit-utils"]
        env["PYTHONPATH"] = f"$PYTHONPATH:{emit_utils_pge.repo_dir}"
        cmd = ["python", output_generator_exe, tmp_daac_nc_min_path, tmp_daac_nc_minuncert_path,
               acq.min_img_path, acq.minuncert_img_path, acq.loc_img_path, acq.glt_img_path,
               "V0" + str(wm.config["prod_versions"]["l2b"]), wm.config["extended_build_num"],
               "--log_file", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy and rename output files back to /store
        log_path = acq.min_nc_path.replace(".nc", "_nc_pge.log")
        wm.copy(tmp_daac_nc_min_path, acq.min_nc_path)
        wm.copy(tmp_daac_nc_minuncert_path, acq.minuncert_nc_path)
        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.min_nc_path), tz=datetime.timezone.utc)
        dm = wm.database_manager
        product_dict_netcdf = {
            "netcdf_min_path": acq.min_nc_path,
            "netcdf_minuncert_path": acq.minuncert_nc_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.l2b.{wm.config['prod_versions']['l2b']}.min_netcdf": product_dict_netcdf})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "min_img_path": acq.min_img_path,
                "minuncert_img_path": acq.minuncert_img_path,
                "loc_img_path": acq.loc_img_path,
                "glt_img_path": acq.glt_img_path
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": "TBD",
            "product_creation_time": nc_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_min_netcdf_path": acq.min_nc_path,
                "l2b_minuncert_netcdf_path": acq.minuncert_nc_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2BDeliver(SlurmJobTask):
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
        return L2BFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
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
        # nc_path = acq.min_img_path.replace(".img", ".nc")
        ummg_path = acq.min_nc_path.replace(".nc", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_min_nc_name = f"{acq.min_granule_ur}.nc"
        daac_minuncert_nc_name = f"{acq.minuncert_granule_ur}.nc"
        daac_ummg_name = f"{acq.min_granule_ur}.cmr.json"
        daac_browse_name = f"{acq.min_granule_ur}.png"
        daac_min_nc_path = os.path.join(self.tmp_dir, daac_min_nc_name)
        daac_minuncert_nc_path = os.path.join(self.tmp_dir, daac_minuncert_nc_name)
        daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(acq.min_nc_path, daac_min_nc_path)
        wm.copy(acq.minuncert_nc_path, daac_minuncert_nc_path)
        wm.copy(acq.min_png_path, daac_browse_path)

        # Get the software_build_version (extended build num when product was created)
        hdr = envi.read_envi_header(acq.min_hdr_path)
        software_build_version = hdr["emit software build version"]

        # Use a cloud fraction that sums the nodata fraction (clouds screened on board) and the cloud fraction value
        # from the maskTf step.  These fractions are rounded separately.  Use min to ensure it doesn't go over 100.
        cloud_fraction = acq["products"]["mask"][wm.config["prod_versions"]["mask"]]["maskTf"]["cloud_fraction"]
        nodata_fraction = acq["products"]["mask"][wm.config["prod_versions"]["mask"]]["maskTf"]["nodata_fraction"]
        cloud_cover = min(cloud_fraction + nodata_fraction, 100)

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.min_nc_path), tz=datetime.timezone.utc)
        l2b_pge = wm.pges["emit-sds-l2b"]
        ummg = daac_converter.initialize_ummg(acq.min_granule_ur, nc_creation_time, "EMITL2BMIN",
                                              acq.collection_version, acq.start_time,
                                              acq.stop_time, l2b_pge.repo_name, l2b_pge.version_tag,
                                              software_build_version=software_build_version,
                                              software_delivery_version=wm.config["extended_build_num"],
                                              doi=wm.config["dois"]["EMITL2BMIN"], orbit=int(acq.orbit),
                                              orbit_segment=int(acq.scene), scene=int(acq.daac_scene),
                                              solar_zenith=acq.mean_solar_zenith,
                                              solar_azimuth=acq.mean_solar_azimuth,
                                              cloud_cover=cloud_cover)
        ummg = daac_converter.add_data_files_ummg(
            ummg,
            [daac_min_nc_path, daac_minuncert_nc_path, daac_browse_path],
            acq.daynight,
            ["NETCDF-4", "NETCDF-4", "PNG"])
        # ummg = daac_converter.add_related_url(ummg, l2b_pge.repo_url, "DOWNLOAD SOFTWARE")
        ummg = daac_converter.add_boundary_ummg(ummg, acq.gring)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to S3 for staging
        for path in (daac_min_nc_path, daac_minuncert_nc_path, daac_browse_path, daac_ummg_path):
            cmd_aws_s3 = ["ssh", "ngishpc1", "'" + wm.config["aws_cli_exe"], "s3", "cp", path, acq.aws_s3_uri_base,
                          "--profile", wm.config["aws_profile"] + "'"]
            pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.min_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l2b_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_min_nc_name: os.path.basename(acq.min_nc_path),
            daac_minuncert_nc_name: os.path.basename(acq.minuncert_nc_path),
            daac_browse_name: os.path.basename(acq.min_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        provider = wm.config["daac_provider_forward"]
        queue_url = wm.config["daac_submission_url_forward"]
        if self.daac_ingest_queue == "backward":
            provider = wm.config["daac_provider_backward"]
            queue_url = wm.config["daac_submission_url_backward"]
        notification = {
            "collection": "EMITL2BMIN",
            "provider": provider,
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.min_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_min_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_min_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_min_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_min_nc_path, "sha512")
                    },
                    {
                        "name": daac_minuncert_nc_name,
                        "uri": acq.aws_s3_uri_base + daac_minuncert_nc_name,
                        "type": "data",
                        "size": os.path.getsize(daac_minuncert_nc_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_minuncert_nc_path, "sha512")
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
                "granule_ur": acq.min_granule_ur,
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
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.l2b.{wm.config['prod_versions']['l2b']}.min_ummg": product_dict_ummg})

        if "min_daac_submissions" in acq.metadata["products"]["l2b"][wm.config["prod_versions"]["l2b"]] and \
                acq.metadata["products"]["l2b"][wm.config["prod_versions"]["l2b"]]["min_daac_submissions"] is not None:
            acq.metadata["products"]["l2b"][wm.config["prod_versions"]["l2b"]]["min_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l2b"][wm.config["prod_versions"]["l2b"]]["min_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {f"products.l2b.{wm.config['prod_versions']['l2b']}.min_daac_submissions": acq.metadata["products"]["l2b"][wm.config["prod_versions"]["l2b"]]["min_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "min_netcdf_path": acq.min_nc_path,
                "minuncert_netcdf_path": acq.minuncert_nc_path,
                "min_png_path": acq.min_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_min_ummg_path:": ummg_path,
                "l2b_min_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2BFrCov(SlurmJobTask):
    """
    Creates L2B fractional cover estimates
    :returns: Fractional cover file and uncertainties
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
        return (L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                               partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["SpectralUnmixing"]

        # Build PGE commands for run_tetracorder_pge.sh
        unmix_exe = os.path.join(pge.repo_dir, "unmix.jl")
        endmember_key = "level_1"
        tmp_log_path = os.path.join(self.local_tmp_dir,
                                    os.path.basename(acq.frcov_img_path).replace(".img", "_pge.log"))
        output_base = os.path.join(self.local_tmp_dir, "unmixing_output")

        # Set up environment variables
        env = os.environ.copy()
        env["PATH"] = "/store/shared/julia-1.12.4/bin:${PATH}"
        env["JULIA_DEPOT_PATH"] = "/store/shared/.julia_1124"
        env["JULIA_PROJECT"] = pge.repo_dir

        # Build command
        cmd_unmix = ['julia', '-p', str(self.n_cores), unmix_exe, acq.rfl_img_path, wm.config["unmixing_library"],
                     endmember_key, output_base, "--normalization", "brightness", "--mode", "sma-best",
                     "--n_mc", "20", "--reflectance_uncertainty_file", acq.rfluncert_img_path,
                     "--spectral_starting_column", "8", "--num_endmembers", "30", "--log_file", tmp_log_path]

        pge.run(cmd_unmix, tmp_dir=self.tmp_dir, env=env, use_conda_run=False)

        wm.copy(f'{output_base}_fractional_cover', acq.frcov_img_path)
        wm.copy(f'{output_base}_fractional_cover.hdr', acq.frcov_hdr_path)
        wm.copy(f'{output_base}_fractional_cover_uncertainty', acq.frcovuncert_img_path)
        wm.copy(f'{output_base}_fractional_cover_uncertainty.hdr', acq.frcovuncert_hdr_path)
        wm.copy(tmp_log_path, acq.frcov_img_path.replace(".img", "_pge.log"))

        input_files = {
            "reflectance_file": acq.rfl_img_path,
            "reflectance_uncertainty_file": acq.rfluncert_img_path,
            "endmember_path": wm.config["unmixing_library"],
        }

        # Update hdr files
        for header_to_update in [acq.frcov_hdr_path, acq.frcovuncert_hdr_path]:
            input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
            doc_version = "EMIT SDS L3 JPL-D 104238, Rev A"  # \todo check
            hdr = envi.read_envi_header(header_to_update)
            hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit pge name"] = pge.repo_url
            hdr["emit pge version"] = pge.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd_unmix)
            hdr["emit software build version"] = wm.config["extended_build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(acq.frcov_img_path), tz=datetime.timezone.utc)
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit data product version"] = wm.config["prod_versions"]["frcov"]
            hdr["emit acquisition daynight"] = acq.daynight
            envi.write_envi_header(header_to_update, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict_frcov = {
            "img_path": acq.frcov_img_path,
            "hdr_path": acq.frcov_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{self.config['prod_versions']['frcov']}.frcov": product_dict_frcov})

        product_dict_frcov_uncert = {
            "img_path": acq.frcovuncert_img_path,
            "hdr_path": acq.frcovuncert_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{self.config['prod_versions']['frcov']}.frcovuncert": product_dict_frcov_uncert})

        total_time = time.time() - start_time
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd_unmix),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "pge_runtime_seconds": total_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_frcov_img_path": acq.frcov_img_path,
                "l2b_frcov_hdr_path:": acq.frcov_hdr_path,
                "l2b_frcovuncert_img_path": acq.frcovuncert_img_path,
                "l2b_frcovuncert_hdr_path:": acq.frcovuncert_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2BFrCovFormat(SlurmJobTask):
    """
    Converts L2B Fractional cover products mask to COGs and generates QA/QC mask
    :returns: L2B Fractional cover COG output for delivery
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

        start_time = time.time()
        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-frcov"]
        dm = wm.database_manager

        mask_generator_exe = os.path.join(pge.repo_dir, "create_frcov_masks.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_frcovqc_tif_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_frcovqc.tif")
        tmp_log_path = os.path.join(self.local_tmp_dir, "mask_and_format_pge.log")

        input_files = {
            "rfl_file": acq.rfl_img_path,
            "maskTf_file": acq.maskTf_img_path,
            "glt_file": acq.glt_img_path,
            "frcov_file": acq.frcov_img_path,
            "frcovuncert_file": acq.frcovuncert_img_path,
        }

        cmd = ["python",
               mask_generator_exe,
               "create-masks",
               acq.rfl_img_path, 
               acq.maskTf_img_path, 
               acq.glt_img_path,
               tmp_frcovqc_tif_path,
               '--urban_data', "/store/shared/landcover/complete_landcover.vrt",
               '--coastal_data', "/store/shared/landcover/GSHHS_f_L1.shp"]
               
        # Run this inside the emit-main conda environment to include emit-utils and other requirements
        main_pge = wm.pges["emit-sds-frcov"]
        main_pge.run(cmd, tmp_dir=self.tmp_dir)

        format_exe = os.path.join(pge.repo_dir, "format_outputs.py")

        tmp_frcov_base = os.path.join(tmp_output_dir, self.acquisition_id)

        format_cmd = ["python",
               format_exe,
               acq.frcov_img_path, 
               acq.frcovuncert_img_path, 
               tmp_frcovqc_tif_path,
               acq.glt_img_path,
               "--software_version", wm.config["extended_build_num"],
               "--product_version", acq.collection_version,
               tmp_frcov_base]

        main_pge.run(format_cmd, tmp_dir=self.tmp_dir)

        wm.copy(tmp_frcovqc_tif_path, acq.frcovqc_tif_path)
        
        wm.copy(tmp_frcov_base + '_frcov_pv.tif', acq.frcovpv_tif_path)
        wm.copy(tmp_frcov_base + '_frcovunc_pv.tif', acq.frcovpvunc_tif_path)

        wm.copy(tmp_frcov_base + '_frcov_npv.tif', acq.frcovnpv_tif_path)
        wm.copy(tmp_frcov_base + '_frcovunc_npv.tif', acq.frcovnpvunc_tif_path)

        wm.copy(tmp_frcov_base + '_frcov_bare.tif', acq.frcovbare_tif_path)
        wm.copy(tmp_frcov_base + '_frcovunc_bare.tif', acq.frcovbareunc_tif_path)
        
        wm.copy(tmp_frcov_base + '_frcov.png', acq.frcov_png_path)

        # Update db
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.qc": {
                "tif_path" : acq.frcovqc_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.pv": {
                "tif_path" : acq.frcovpv_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.pvunc": {
                "tif_path" : acq.frcovpvunc_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.npv": {
                "tif_path" : acq.frcovnpv_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.npvunc": {
                "tif_path" : acq.frcovnpvunc_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.bare": {
                "tif_path" : acq.frcovbareunc_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.bareunc": {
                "tif_path" : acq.frcovbareunc_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.browse": {
                "png_path" : acq.frcov_png_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})

        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.frcovpv_tif_path), tz=datetime.timezone.utc)
        
        doc_version = "EMIT SDS GHG JPL-D 107866, v0.2"
        
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
                "frcovqc_tif_path": acq.frcovqc_tif_path,
                "frcovpv_tif_path:": acq.frcovpv_tif_path,
                "frcovpvunc_tif_path": acq.frcovpvunc_tif_path,
                "frcovnpv_tif_path:": acq.frcovnpv_tif_path,
                "frcovnpvunc_tif_path": acq.frcovnpvunc_tif_path,
                "frcovbare_tif_path:": acq.frcovbare_tif_path,
                "frcovbareunc_tif_path": acq.frcovbareunc_tif_path,
                "frcov_png_path": acq.frcov_png_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2BFrCovDeliver(SlurmJobTask):
    """
    Stages FRCOV and UMM-G files and submits notification to DAAC interface
    :returns: Staged L2B files
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
        return L2BFrCovFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
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
        for name, value in vars(acq).items():
            print(f"{name}: {value}")

        # Get local SDS names
        ummg_path = acq.frcov_png_path.replace(".png", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_frcovqc_tif_name = f"{acq.frcovqc_granule_ur}.tif"
        daac_pv_tif_name = f"{acq.frcovpv_granule_ur}.tif"
        daac_pvunc_tif_name = f"{acq.frcovpvunc_granule_ur}.tif"
        daac_npv_tif_name = f"{acq.frcovnpv_granule_ur}.tif"
        daac_npvunc_tif_name = f"{acq.frcovnpvunc_granule_ur}.tif"
        daac_bare_tif_name = f"{acq.frcovbare_granule_ur}.tif"
        daac_bareunc_tif_name = f"{acq.frcovbareunc_granule_ur}.tif"

        daac_ummg_name = f"{acq.frcov_granule_ur}.cmr.json"
        daac_browse_name = f"{acq.frcov_granule_ur}.png"
        
        daac_frcovqc_tif_path = os.path.join(self.tmp_dir, daac_frcovqc_tif_name)
        daac_pv_tif_path = os.path.join(self.tmp_dir, daac_pv_tif_name)
        daac_pvunc_tif_path = os.path.join(self.tmp_dir, daac_pvunc_tif_name)
        daac_npv_tif_path = os.path.join(self.tmp_dir, daac_npv_tif_name)
        daac_npvunc_tif_path = os.path.join(self.tmp_dir, daac_npvunc_tif_name)
        daac_bare_tif_path = os.path.join(self.tmp_dir, daac_bare_tif_name)
        daac_bareunc_tif_path = os.path.join(self.tmp_dir, daac_bareunc_tif_name)
        daac_pv_tif_path = os.path.join(self.tmp_dir, daac_pv_tif_name)

        daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(acq.frcovqc_tif_path, daac_frcovqc_tif_path)
        wm.copy(acq.frcov_png_path, daac_browse_path)
        wm.copy(acq.frcovpv_tif_path, daac_pv_tif_path)
        wm.copy(acq.frcovpvunc_tif_path, daac_pvunc_tif_path)
        wm.copy(acq.frcovnpv_tif_path, daac_npv_tif_path)
        wm.copy(acq.frcovnpvunc_tif_path, daac_npvunc_tif_path)
        wm.copy(acq.frcovbare_tif_path, daac_bare_tif_path)
        wm.copy(acq.frcovbareunc_tif_path, daac_bareunc_tif_path)        

        # Get the software_build_version (extended build num when product was created)
        software_build_version = read_gdal_metadata(acq.frcovqc_tif_path, 'software_build_version')

        if not software_build_version:
            print('Could not read software build version from COG metadata')
            sys.exit()

        daac_paths = [daac_frcovqc_tif_path, daac_pv_tif_path, daac_pvunc_tif_path, 
                      daac_npv_tif_path, daac_npvunc_tif_path, daac_bare_tif_path, 
                      daac_bareunc_tif_path, daac_browse_path]

        # Use a cloud fraction that sums the nodata fraction (clouds screened on board) and the cloud fraction value
        # from the maskTf step.  These fractions are rounded separately.  Use min to ensure it doesn't go over 100.
        cloud_fraction = acq["products"]["mask"][wm.config["prod_versions"]["mask"]]["maskTf"]["cloud_fraction"]
        nodata_fraction = acq["products"]["mask"][wm.config["prod_versions"]["mask"]]["maskTf"]["nodata_fraction"]
        cloud_cover = min(cloud_fraction + nodata_fraction, 100)
        
        # Create the UMM-G file
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.frcovqc_tif_path), tz=datetime.timezone.utc)
        frcov_pge = wm.pges["emit-sds-frcov"]
        ummg = daac_converter.initialize_ummg(acq.frcov_granule_ur, creation_time, "EMITL2BFRCOV",
                                              acq.collection_version, acq.start_time,
                                              acq.stop_time, frcov_pge.repo_name, frcov_pge.version_tag,
                                              software_build_version=software_build_version,
                                              software_delivery_version=wm.config["extended_build_num"],
                                              doi=wm.config["dois"]["EMITL2BFRCOV"], orbit=int(acq.orbit),
                                              orbit_segment=int(acq.scene), scene=int(acq.daac_scene),
                                              solar_zenith=acq.mean_solar_zenith,
                                              solar_azimuth=acq.mean_solar_azimuth,
                                              cloud_cover=cloud_cover)
        ummg = daac_converter.add_data_files_ummg(
            ummg, daac_paths,
            acq.daynight,
            ["GeoTIFF", "GeoTIFF", "GeoTIFF",
             "GeoTIFF", "GeoTIFF", "GeoTIFF",
             "GeoTIFF", "PNG"])
        # ummg = daac_converter.add_related_url(ummg, frcov_pge.repo_url, "DOWNLOAD SOFTWARE")
        ummg = daac_converter.add_boundary_ummg(ummg, acq.gring)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        daac_paths.append(daac_ummg_path)

        # Copy files to S3 for staging
        for path in daac_paths:
            cmd_aws_s3 = ["ssh", "ngishpc1", "'" + wm.config["aws_cli_exe"], "s3", "cp", path, acq.aws_s3_uri_base,
                          "--profile", wm.config["aws_profile"] + "'"]
            pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.frcov_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.frcov_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_frcovqc_tif_name: os.path.basename(acq.frcovqc_tif_path),
            daac_pv_tif_name: os.path.basename(acq.frcovpv_tif_path),
            daac_pvunc_tif_name: os.path.basename(acq.frcovpvunc_tif_path),
            daac_npv_tif_name: os.path.basename(acq.frcovnpv_tif_path),
            daac_npvunc_tif_name: os.path.basename(acq.frcovnpvunc_tif_path),
            daac_bare_tif_name: os.path.basename(acq.frcovbare_tif_path),
            daac_bareunc_tif_name: os.path.basename(acq.frcovbareunc_tif_path),
            daac_browse_name: os.path.basename(acq.frcov_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        provider = wm.config["daac_provider_forward"]
        queue_url = wm.config["daac_submission_url_forward"]
        if self.daac_ingest_queue == "backward":
            provider = wm.config["daac_provider_backward"]
            queue_url = wm.config["daac_submission_url_backward"]
        notification = {
            "collection": "EMITL2BFRCOV",
            "provider": provider,
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.frcov_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_frcovqc_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_frcovqc_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_frcovqc_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_frcovqc_tif_path, "sha512")
                    },
                    {
                        "name": daac_pv_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_pv_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_pv_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_pv_tif_path, "sha512")    
                    },
                    {
                        "name": daac_pvunc_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_pvunc_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_pvunc_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_pvunc_tif_path, "sha512")                        
                    },
                    {
                        "name": daac_npv_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_npv_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_npv_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_npv_tif_path, "sha512")                        
                    },
                    {
                        "name": daac_npvunc_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_npvunc_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_npvunc_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_npvunc_tif_path, "sha512")
                    },
                    {
                        "name": daac_bare_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_bare_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_bare_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_bare_tif_path, "sha512")
                    },
                    {
                        "name": daac_bareunc_tif_name,
                        "uri": acq.aws_s3_uri_base + daac_bareunc_tif_name,
                        "type": "data",
                        "size": os.path.getsize(daac_bareunc_tif_name),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_bareunc_tif_path, "sha512")
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
                "granule_ur": acq.frcov_granule_ur,
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
        dm.update_acquisition_metadata(acq.acquisition_id, {f"products.frcov.{wm.config['prod_versions']['frcov']}.frcov_ummg": product_dict_ummg})

        if "frcov_daac_submissions" in acq.metadata["products"]["frcov"][wm.config["prod_versions"]["frcov"]] and \
                acq.metadata["products"]["frcov"][wm.config["prod_versions"]["frcov"]]["frcov_daac_submissions"] is not None:
            acq.metadata["products"]["frcov"][wm.config["prod_versions"]["frcov"]]["frcov_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["frcov"][wm.config["prod_versions"]["frcov"]]["frcov_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {f"products.frcov.{wm.config['prod_versions']['frcov']}.frcov_daac_submissions": acq.metadata["products"]["frcov"][wm.config["prod_versions"]["frcov"]]["frcov_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "frcovqc_tif_path": acq.frcovqc_tif_path,
                "pv_tif_path": acq.frcovpv_tif_path,
                "pvunc_tif_path": acq.frcovpvunc_tif_path,
                "npv_tif_path": acq.frcovnpv_tif_path,
                "npvunc_tif_path": acq.frcovnpvunc_tif_path,
                "bare_tif_path": acq.frcovbare_tif_path,
                "bareunc_tif_path": acq.frcovbareunc_tif_path,
                "frcov_png_path": acq.frcov_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_frcov_ummg_path:": ummg_path,
                "l2b_frcov_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
