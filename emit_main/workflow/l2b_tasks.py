"""
This code contains tasks for executing EMIT Level 2B PGEs and helper utilities.

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
from emit_main.workflow.l1b_tasks import L1BGeolocate
from emit_main.workflow.l2a_tasks import L2AMask, L2AReflectance
from emit_main.workflow.slurm import SlurmJobTask
from emit_utils.file_checks import envi_header
from emit_utils import daac_converter

logger = logging.getLogger("emit-main")


class L2BAbundance(SlurmJobTask):
    """
    Creates L2B mineral spectral abundance estimate file
    :returns: Mineral abundance file and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return (L2AReflectance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                               partition=self.partition),
                L2AMask(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                        partition=self.partition))

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

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
        env["PATH"] = "${PATH}:${SP_LOCAL}/bin:${TETRA}/bin:/usr/bin"

        # This has to be a bit truncated because of character limitations
        tmp_rfl_path = os.path.join(self.local_tmp_dir, 'r')
        tmp_rfl_path_hdr = envi_header(tmp_rfl_path)

        wm.symlink(acq.rfl_img_path, tmp_rfl_path)
        wm.symlink(acq.rfl_hdr_path, tmp_rfl_path_hdr)

        # This has to be a bit truncated because of character limitations
        tmp_tetra_output_path = os.path.join(self.local_tmp_dir, os.path.basename(acq.abun_img_path).split('_')[0] + '_tetra')

        cmd_tetra_setup = [wm.config["tetracorder_setup_cmd_path"], tmp_tetra_output_path,
                           wm.config["tetracorder_library_cmdname"], "cube", tmp_rfl_path, "1", "-T", "-20", "80", "C",
                           "-P", ".5", "1.5", "bar"]
        pge.run(cmd_tetra_setup, tmp_dir=self.tmp_dir, env=env)

        current_pwd = os.getcwd()
        os.chdir(tmp_tetra_output_path)
        cmd_tetra = [os.path.join(tmp_tetra_output_path, "cmd.runtet"), "cube", tmp_rfl_path, 'band', '20', 'gif']
        pge.run(cmd_tetra, tmp_dir=self.tmp_dir, env=env)
        os.chdir(current_pwd)

        # Build aggregator cmd
        aggregator_exe = os.path.join(pge.repo_dir, "aggregator.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "l2b_aggregation_output")
        wm.makedirs(tmp_output_dir)
        tmp_abun_path = os.path.join(tmp_output_dir, os.path.basename(acq.abun_img_path))
        standard_library = os.path.join(
            wm.config['tetracorder_library_dir'], f's{wm.config["tetracorder_library_basename"]}_envi')
        research_library = os.path.join(
            wm.config['tetracorder_library_dir'], f'r{wm.config["tetracorder_library_basename"]}_envi')
        input_files = {
            "reflectance_file": acq.rfl_img_path,
            "reflectance_uncertainty_file": acq.rfluncert_img_path,
            "tetracorder_library_basename": wm.config["tetracorder_library_basename"]
        }
        cmd = ["python", aggregator_exe, tmp_tetra_output_path, tmp_abun_path,
               "--calculate_uncertainty", "1",
               "--reflectance_file", acq.rfl_img_path,
               "--reflectance_uncertainty_file", acq.rfluncert_img_path,
               "--reference_library", standard_library,
               "--research_library", research_library,
               ]
        pge.run(cmd, cwd=pge.repo_dir, tmp_dir=self.tmp_dir)

        # Copy mask files to l2a dir
        wm.copytree(tmp_tetra_output_path, acq.tetra_dir_path)
        wm.copy(tmp_abun_path, acq.abun_img_path)
        wm.copy(envi_header(tmp_abun_path), acq.abun_hdr_path)
        wm.copy(tmp_abun_path + '_uncert', acq.abununcert_img_path)
        wm.copy(envi_header(tmp_abun_path + '_uncert'), acq.abununcert_hdr_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L2B JPL-D 104237, Rev A"
        hdr = envi.read_envi_header(acq.abun_hdr_path)
        hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["extended_build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.abun_img_path), tz=datetime.timezone.utc)
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit data product version"] = wm.config["processing_version"]
        hdr["emit acquisition daynight"] = acq.daynight
        envi.write_envi_header(acq.abun_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
        product_dict = {
            "img_path": acq.abun_img_path,
            "hdr_path": acq.abun_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2b.abun": product_dict})

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
                "l2b_abun_img_path": acq.abun_img_path,
                "l2b_abun_hdr_path:": acq.abun_hdr_path,
                "l2b_abununcert_img_path": acq.abununcert_img_path,
                "l2b_abununcert_hdr_path:": acq.abununcert_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L2BFormat(SlurmJobTask):
    """
    Converts L2B (spectral abundance and uncertainty) to netcdf files
    :returns: L2B netcdf output for delivery
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):
        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L2BAbundance(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                            partition=self.partition)

    def output(self):
        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

    def work(self):
        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition

        pge = wm.pges["emit-sds-l2b"]

        output_generator_exe = os.path.join(pge.repo_dir, "output_conversion.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)
        tmp_daac_nc_abun_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2b_abun.nc")
        tmp_daac_nc_abununcert_path = os.path.join(tmp_output_dir, f"{self.acquisition_id}_l2b_abununcert.nc")
        tmp_log_path = os.path.join(self.local_tmp_dir, "output_conversion_pge.log")

        cmd = ["python", output_generator_exe, tmp_daac_nc_abun_path, tmp_daac_nc_abununcert_path,
               acq.abun_img_path, acq.abununcert_img_path, acq.loc_img_path, acq.glt_img_path, 
               "V0" + str(wm.config["processing_version"]), "--log_file", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy and rename output files back to /store
        log_path = acq.abun_nc_path.replace(".nc", "_nc_pge.log")
        wm.copy(tmp_daac_nc_abun_path, acq.abun_nc_path)
        wm.copy(tmp_daac_nc_abununcert_path, acq.abununcert_nc_path)
        wm.copy(tmp_log_path, log_path)

        # PGE writes metadata to db
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.abun_nc_path), tz=datetime.timezone.utc)
        dm = wm.database_manager
        product_dict_netcdf = {
            "netcdf_abun_path": acq.abun_nc_path,
            "netcdf_abununcert_path": acq.abununcert_nc_path,
            "created": nc_creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2b.abun_netcdf": product_dict_netcdf})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "abun_img_path": acq.abun_img_path,
                "abununert_img_path": acq.abununcert_img_path,
                "loc_img_path": acq.loc_img_path,
                "glt_img_path": acq.glt_img_path
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": "TBD",
            "product_creation_time": nc_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_abun_netcdf_path": acq.abun_nc_path,
                "l2b_abununcert_netcdf_path": acq.abununcert_nc_path
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

    task_namespace = "emit"
    n_cores = 1
    memory = 30000

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        return L2BFormat(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
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
        nc_path = acq.abun_img_path.replace(".img", ".nc")
        ummg_path = nc_path.replace(".nc", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_nc_name = f"{acq.abun_granule_ur}.nc"
        daac_ummg_name = f"{acq.abun_granule_ur}.cmr.json"
        daac_browse_name = f"{acq.abun_granule_ur}.png"
        daac_nc_path = os.path.join(self.tmp_dir, daac_nc_name)
        daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(nc_path, daac_nc_path)
        wm.copy(acq.rdn_png_path, daac_browse_path)

        # Create the UMM-G file
        nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(nc_path), tz=datetime.timezone.utc)
        daynight = "Day" if acq.submode == "science" else "Night"
        ummg = daac_converter.initialize_ummg(acq.abun_granule_ur, nc_creation_time, "EMITL2BMIN")
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

        for path in (daac_nc_path, daac_browse_path, daac_ummg_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.abun_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l2b_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_nc_name: os.path.basename(nc_path),
            daac_browse_name: os.path.basename(acq.rdn_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        notification = {
            "collection": "EMITL2BMIN",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.abun_granule_ur,
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
                        "name": daac_browse_name,
                        "uri": acq.daac_uri_base + daac_browse_name,
                        "type": "browse",
                        "size": os.path.getsize(daac_browse_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_browse_path, "sha512")
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
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2b.abun_ummg": product_dict_ummg})

        if "abun_daac_submissions" in acq.metadata["products"]["l2b"] and \
                acq.metadata["products"]["l2b"]["abun_daac_submissions"] is not None:
            acq.metadata["products"]["l2b"]["abun_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l2b"]["abun_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {"products.l2b.abun_daac_submissions": acq.metadata["products"]["l2b"]["abun_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "netcdf_path": nc_path,
                "rdn_png_path": acq.rdn_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l2b_abun_ummg_path:": ummg_path,
                "l2b_abun_cnm_submission_path": cnm_submission_path
            }
        }
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
