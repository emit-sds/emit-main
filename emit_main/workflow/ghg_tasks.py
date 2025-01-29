"""
This code contains tasks for executing EMIT Methane Detection.

Authors:  Philip G. Brodrick, philip.brodrick@jpl.nasa.gov
          Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os
import sys
import time

import luigi

from emit_main.workflow.output_targets import AcquisitionTarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask

from emit_utils.file_checks import envi_header

logger = logging.getLogger("emit-main")


class CH4(SlurmJobTask):
    """
    Performs methane mapping on the EMIT SDS
    :returns: Matched filter form methane estimation
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

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(f"{self.task_family} run: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-ghg"]
        emit_utils_pge = wm.pges["emit-utils"]
        dm = wm.database_manager

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.local_tmp_dir, "ch4")
        wm.makedirs(tmp_output_dir)
        env = os.environ.copy()
        env["RAY_worker_register_timeout_seconds"] = "600"
        env["PYTHONPATH"] = f"$PYTHONPATH:{pge.repo_dir}:{emit_utils_pge.repo_dir}"
        sys.path.append(pge.repo_dir)

        from files import Filenames # This might now work without path mod

        # Definte exe's
        process_exe = os.path.join(pge.repo_dir, "ghg_process.py")

        # Define local output files
        ch4_log_file = os.path.join(tmp_output_dir, "ch4_log.txt")

        ch4_base = os.path.join(self.tmp_dir, acq.acquisition_id + '_ch4')

        noise_file = os.path.join(pge.repo_dir, "instrument_noise_parameters","emit_noise.txt")

        input_files = {
            "radiance_file": acq.rdn_img_path,
            "obs_file": acq.obs_img_path,
            "loc_file": acq.loc_img_path,
            "glt_file": acq.glt_img_path,
            "bandmask_file": acq.bandmask_img_path,
            "mask_file": acq.mask_img_path,
            "state_subs_file": acq.statesubs_img_path,
        }

        # Create command
        cmd = ["python", process_exe,
               acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path, acq.glt_img_path,
               acq.bandmask_img_path, acq.mask_img_path, ch4_base,
               '--state_subs', acq.statesubs_img_path,
               "--noise_file",noise_file,'--lut_file',
               wm.config["ch4_lut_file"], "--logfile", ch4_log_file]
        
        # Run CH4
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)
        ch4_of = Filenames(ch4_base)

        # MF - CH4
        wm.copy(ch4_of.mf_file, acq.ch4_img_path)
        wm.copy(envi_header(ch4_of.mf_file), acq.ch4_hdr_path)
        wm.copy(ch4_of.mf_ort_cog, acq.ortch4_tif_path)
        wm.copy(ch4_of.mf_ort_ql, acq.ortch4_png_path)

        # Sensitivity - CH4
        wm.copy(ch4_of.mf_sens_file, acq.sensch4_img_path)
        wm.copy(envi_header(ch4_of.mf_sens_file), acq.sensch4_hdr_path)
        wm.copy(ch4_of.sens_ort_cog, acq.ortsensch4_tif_path)

        # Uncertainty - CH4
        wm.copy(ch4_of.mf_uncert_file, acq.uncertch4_img_path)
        wm.copy(envi_header(ch4_of.mf_uncert_file), acq.uncertch4_hdr_path)
        wm.copy(ch4_of.uncert_ort_cog, acq.ortuncertch4_tif_path)

        # Update db
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.ch4": {
                "img_path" : acq.ch4_img_path,
                "hdr_path" : acq.ch4_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.sensch4": {
                "img_path" : acq.sensch4_img_path,
                "hdr_path" : acq.sensch4_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.uncertch4": {
                "img_path" : acq.uncertch4_img_path,
                "hdr_path" : acq.uncertch4_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.ortch4": {
                "tif_path" : acq.ortch4_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.ortch4ql": {
                "png_path" : acq.ortch4_png_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.ortsensch4": {
                "tif_path" : acq.ortsensch4_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.ch4.ortuncertch4": {
                "tif_path" : acq.ortuncertch4_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})

        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.ortuncertch4_tif_path), tz=datetime.timezone.utc)

        doc_version = "EMIT SDS L2B JPL-D........."  #TODO: Placeholder

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
                "ch4_img_path": acq.ch4_img_path,
                "ch4_hdr_path:": acq.ch4_hdr_path,
                "sensch4_img_path": acq.sensch4_img_path,
                "sensch4_hdr_path": acq.sensch4_hdr_path,
                "uncertch4_img_path": acq.uncertch4_img_path,
                "uncertch4_hdr_path": acq.uncertch4_hdr_path,
                "ortch4_tif_path": acq.ortch4_tif_path,
                "ortch4_png_path": acq.ortch4_png_path,
                "ortsensch4_tif_path": acq.ortsensch4_tif_path,
                "ortuncertch4_tif_path": acq.ortuncertch4_tif_path,
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
        
class CO2(SlurmJobTask):
    """
    Performs carbon dioxide mapping on the EMIT SDS
    :returns: Matched filter form carbon dioxide estimation
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

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        start_time = time.time()
        logger.debug(f"{self.task_family} run: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-ghg"]
        emit_utils_pge = wm.pges["emit-utils"]
        dm = wm.database_manager

        # PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.local_tmp_dir, "co2")
        wm.makedirs(tmp_output_dir)
        env = os.environ.copy()
        env["RAY_worker_register_timeout_seconds"] = "600"
        env["PYTHONPATH"] = f"$PYTHONPATH:{pge.repo_dir}:{emit_utils_pge.repo_dir}"
        sys.path.append(pge.repo_dir)

        from files import Filenames # This might now work without path mod

        # Definte exe's
        process_exe = os.path.join(pge.repo_dir, "ghg_process.py")

        # Define local output files
        co2_log_file = os.path.join(tmp_output_dir, "co2_log.txt")

        co2_base = os.path.join(self.tmp_dir, acq.acquisition_id + '_co2')

        noise_file = os.path.join(pge.repo_dir, "instrument_noise_parameters","emit_noise.txt")

        input_files = {
            "radiance_file": acq.rdn_img_path,
            "obs_file": acq.obs_img_path,
            "loc_file": acq.loc_img_path,
            "glt_file": acq.glt_img_path,
            "bandmask_file": acq.bandmask_img_path,
            "mask_file": acq.mask_img_path,
            "state_subs_file": acq.statesubs_img_path,
        }

        # Create command
        cmd = ["python", process_exe,
               acq.rdn_img_path, acq.obs_img_path, acq.loc_img_path, acq.glt_img_path,
               acq.bandmask_img_path, acq.mask_img_path, co2_base,
               '--state_subs', acq.statesubs_img_path,
               "--noise_file",noise_file, '--lut_file', wm.config["co2_lut_file"],
               "--logfile", co2_log_file, "--co2"]

        # Run CO2
        start_time = time.time()
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)
        co2_of = Filenames(co2_base)

        # MF - CO2
        wm.copy(co2_of.mf_file, acq.co2_img_path)
        wm.copy(envi_header(co2_of.mf_file), acq.co2_hdr_path)
        wm.copy(co2_of.mf_ort_cog, acq.ortco2_tif_path)
        wm.copy(co2_of.mf_ort_ql, acq.ortco2_png_path)

        # Sensitivity - CO2
        wm.copy(co2_of.mf_sens_file, acq.sensco2_img_path)
        wm.copy(envi_header(co2_of.mf_sens_file), acq.sensco2_hdr_path)
        wm.copy(co2_of.sens_ort_cog, acq.ortsensco2_tif_path)

        # Uncertainty - CO2
        wm.copy(co2_of.mf_uncert_file, acq.uncertco2_img_path)
        wm.copy(envi_header(co2_of.mf_uncert_file), acq.uncertco2_hdr_path)
        wm.copy(co2_of.uncert_ort_cog, acq.ortuncertco2_tif_path)

        # Update db
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.co2": {
                "img_path" : acq.co2_img_path,
                "hdr_path" : acq.co2_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.sensco2": {
                "img_path" : acq.sensco2_img_path,
                "hdr_path" : acq.sensco2_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.uncertco2": {
                "img_path" : acq.uncertco2_img_path,
                "hdr_path" : acq.uncertco2_hdr_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.ortco2": {
                "tif_path" : acq.ortco2_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.ortco2ql": {
                "png_path" : acq.ortco2_png_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.ortsensco2": {
                "tif_path" : acq.ortsensco2_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.ghg.co2.ortuncertco2": {
                "tif_path" : acq.ortuncertco2_tif_path,
                "created" : datetime.datetime.now(tz=datetime.timezone.utc)}})

        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(acq.ortuncertco2_tif_path), tz=datetime.timezone.utc)

        doc_version = "EMIT SDS L2B JPL-D........."  #TODO: Placeholder

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
                "co2_img_path": acq.co2_img_path,
                "co2_hdr_path:": acq.co2_hdr_path,
                "sensco2_img_path": acq.sensco2_img_path,
                "sensco2_hdr_path": acq.sensco2_hdr_path,
                "uncertco2_img_path": acq.uncertco2_img_path,
                "uncertco2_hdr_path": acq.uncertco2_hdr_path,
                "ortco2_tif_path": acq.ortco2_tif_path,
                "ortco2_png_path": acq.ortco2_png_path,
                "ortsensco2_tif_path": acq.ortsensco2_tif_path,
                "ortuncertco2_tif_path": acq.ortuncertco2_tif_path,
            }
        }
        
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


# class CH4Deliver(SlurmJobTask):
#     """
#     Stages GH4 and UMM-G files and submits notification to DAAC interface
#     :returns: Staged L2A files
#     """

#     config_path = luigi.Parameter()
#     acquisition_id = luigi.Parameter()
#     level = luigi.Parameter()
#     partition = luigi.Parameter()
#     daac_ingest_queue = luigi.Parameter(default="forward")
#     override_output = luigi.BoolParameter(default=False)

#     memory = 18000

#     task_namespace = "emit"

#     def requires(self):

#         logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
#         return GHG(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
#                          partition=self.partition)

#     def output(self):

#         logger.debug(f"{self.task_family} output: {self.acquisition_id}")

#         if self.override_output:
#             return None

#         acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
#         return AcquisitionTarget(acquisition=acq, task_family=self.task_family)

#     def work(self):

#         logger.debug(f"{self.task_family} work: {self.acquisition_id}")

#         wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
#         acq = wm.acquisition
#         pge = wm.pges["emit-main"]

#         # Get local SDS names
#         ummg_path = acq.ortch4_tif_path.replace(".tif", ".cmr.json")

#         # Create local/tmp daac names and paths
#         daac_ortch4_tif_name = f"{acq.ch4_granule_ur}.tif"
#         daac_ortsensch4_tif_name = f"{acq.ch4uncert_granule_ur}.tif"
#         daac_ortuncertch4_tif_name = f"{acq.ch4sens_granule_ur}.tif"

#         daac_ummg_name = f"{acq.ch4_granule_ur}.cmr.json"
#         daac_browse_name = f"{acq.ch4_granule_ur}.png"
#         daac_ortch4_tif_path = os.path.join(self.tmp_dir, daac_ortch4_tif_name)
#         daac_ortsensch4_tif_path = os.path.join(self.tmp_dir, daac_ortsensch4_tif_name)
#         daac_ortuncertch4_tif_path = os.path.join(self.tmp_dir, daac_ortuncertch4_tif_name)

#         daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
#         daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

#         # Copy files to tmp dir and rename
#         wm.copy(acq.abun_nc_path, daac_abun_nc_path)
#         wm.copy(acq.abununcert_nc_path, daac_abununcert_nc_path)
#         wm.copy(acq.abun_png_path, daac_browse_path)

#         # Get the software_build_version (extended build num when product was created)
#         hdr = envi.read_envi_header(acq.abun_hdr_path)
#         software_build_version = hdr["emit software build version"]

#         # Create the UMM-G file
#         nc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.abun_nc_path), tz=datetime.timezone.utc)
#         daynight = "Day" if acq.submode == "science" else "Night"
#         l2b_pge = wm.pges["emit-sds-l2b"]
#         ummg = daac_converter.initialize_ummg(acq.abun_granule_ur, nc_creation_time, "EMITL2BMIN",
#                                               acq.collection_version, acq.start_time,
#                                               acq.stop_time, l2b_pge.repo_name, l2b_pge.version_tag,
#                                               software_build_version=software_build_version,
#                                               software_delivery_version=wm.config["extended_build_num"],
#                                               doi=wm.config["dois"]["EMITL2BMIN"], orbit=int(acq.orbit),
#                                               orbit_segment=int(acq.scene), scene=int(acq.daac_scene),
#                                               solar_zenith=acq.mean_solar_zenith,
#                                               solar_azimuth=acq.mean_solar_azimuth,
#                                               cloud_fraction=acq.cloud_fraction)
#         ummg = daac_converter.add_data_files_ummg(
#             ummg,
#             [daac_abun_nc_path, daac_abununcert_nc_path, daac_browse_path],
#             daynight,
#             ["NETCDF-4", "NETCDF-4", "PNG"])
#         # ummg = daac_converter.add_related_url(ummg, l2b_pge.repo_url, "DOWNLOAD SOFTWARE")
#         ummg = daac_converter.add_boundary_ummg(ummg, acq.gring)
#         daac_converter.dump_json(ummg, ummg_path)
#         wm.change_group_ownership(ummg_path)

#         # Copy ummg file to tmp dir and rename
#         wm.copy(ummg_path, daac_ummg_path)

#         # Copy files to S3 for staging
#         for path in (daac_abun_nc_path, daac_abununcert_nc_path, daac_browse_path, daac_ummg_path):
#             cmd_aws_s3 = ["ssh", "ngishpc1", "'" + wm.config["aws_cli_exe"], "s3", "cp", path, acq.aws_s3_uri_base,
#                           "--profile", wm.config["aws_profile"] + "'"]
#             pge.run(cmd_aws_s3, tmp_dir=self.tmp_dir)

#         # Build notification dictionary
#         utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
#         cnm_submission_id = f"{acq.abun_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
#         cnm_submission_path = os.path.join(acq.l2b_data_dir, cnm_submission_id + "_cnm.json")
#         target_src_map = {
#             daac_abun_nc_name: os.path.basename(acq.abun_nc_path),
#             daac_abununcert_nc_name: os.path.basename(acq.abununcert_nc_path),
#             daac_browse_name: os.path.basename(acq.abun_png_path),
#             daac_ummg_name: os.path.basename(ummg_path)
#         }
#         provider = wm.config["daac_provider_forward"]
#         queue_url = wm.config["daac_submission_url_forward"]
#         if self.daac_ingest_queue == "backward":
#             provider = wm.config["daac_provider_backward"]
#             queue_url = wm.config["daac_submission_url_backward"]
#         notification = {
#             "collection": "EMITL2BMIN",
#             "provider": provider,
#             "identifier": cnm_submission_id,
#             "version": wm.config["cnm_version"],
#             "product": {
#                 "name": acq.abun_granule_ur,
#                 "dataVersion": acq.collection_version,
#                 "files": [
#                     {
#                         "name": daac_abun_nc_name,
#                         "uri": acq.aws_s3_uri_base + daac_abun_nc_name,
#                         "type": "data",
#                         "size": os.path.getsize(daac_abun_nc_name),
#                         "checksumType": "sha512",
#                         "checksum": daac_converter.calc_checksum(daac_abun_nc_path, "sha512")
#                     },
#                     {
#                         "name": daac_abununcert_nc_name,
#                         "uri": acq.aws_s3_uri_base + daac_abununcert_nc_name,
#                         "type": "data",
#                         "size": os.path.getsize(daac_abununcert_nc_name),
#                         "checksumType": "sha512",
#                         "checksum": daac_converter.calc_checksum(daac_abununcert_nc_path, "sha512")
#                     },
#                     {
#                         "name": daac_browse_name,
#                         "uri": acq.aws_s3_uri_base + daac_browse_name,
#                         "type": "browse",
#                         "size": os.path.getsize(daac_browse_path),
#                         "checksumType": "sha512",
#                         "checksum": daac_converter.calc_checksum(daac_browse_path, "sha512")
#                     },
#                     {
#                         "name": daac_ummg_name,
#                         "uri": acq.aws_s3_uri_base + daac_ummg_name,
#                         "type": "metadata",
#                         "size": os.path.getsize(daac_ummg_path),
#                         "checksumType": "sha512",
#                         "checksum": daac_converter.calc_checksum(daac_ummg_path, "sha512")
#                     }
#                 ]
#             }
#         }

#         # Write notification submission to file
#         with open(cnm_submission_path, "w") as f:
#             f.write(json.dumps(notification, indent=4))
#         wm.change_group_ownership(cnm_submission_path)

#         # Submit notification via AWS SQS
#         cnm_submission_output = cnm_submission_path.replace(".json", ".out")
#         cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", queue_url, "--message-body",
#                    f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"], ">", cnm_submission_output]
#         pge.run(cmd_aws, tmp_dir=self.tmp_dir)
#         wm.change_group_ownership(cnm_submission_output)
#         cnm_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(cnm_submission_path),
#                                                             tz=datetime.timezone.utc)

#         # Record delivery details in DB for reconciliation report
#         dm = wm.database_manager
#         for file in notification["product"]["files"]:
#             delivery_report = {
#                 "timestamp": utc_now,
#                 "extended_build_num": wm.config["extended_build_num"],
#                 "collection": notification["collection"],
#                 "collection_version": notification["product"]["dataVersion"],
#                 "granule_ur": acq.abun_granule_ur,
#                 "sds_filename": target_src_map[file["name"]],
#                 "daac_filename": file["name"],
#                 "uri": file["uri"],
#                 "type": file["type"],
#                 "size": file["size"],
#                 "checksum": file["checksum"],
#                 "checksum_type": file["checksumType"],
#                 "submission_id": cnm_submission_id,
#                 "submission_queue": queue_url,
#                 "submission_status": "submitted"
#             }
#             dm.insert_granule_report(delivery_report)

#         # Update db with log entry
#         product_dict_ummg = {
#             "ummg_json_path": ummg_path,
#             "created": datetime.datetime.fromtimestamp(os.path.getmtime(ummg_path), tz=datetime.timezone.utc)
#         }
#         dm.update_acquisition_metadata(acq.acquisition_id, {"products.l2b.abun_ummg": product_dict_ummg})

#         if "abun_daac_submissions" in acq.metadata["products"]["l2b"] and \
#                 acq.metadata["products"]["l2b"]["abun_daac_submissions"] is not None:
#             acq.metadata["products"]["l2b"]["abun_daac_submissions"].append(cnm_submission_path)
#         else:
#             acq.metadata["products"]["l2b"]["abun_daac_submissions"] = [cnm_submission_path]
#         dm.update_acquisition_metadata(
#             acq.acquisition_id,
#             {"products.l2b.abun_daac_submissions": acq.metadata["products"]["l2b"]["abun_daac_submissions"]})

#         log_entry = {
#             "task": self.task_family,
#             "pge_name": pge.repo_url,
#             "pge_version": pge.version_tag,
#             "pge_input_files": {
#                 "abun_netcdf_path": acq.abun_nc_path,
#                 "abununcert_netcdf_path": acq.abununcert_nc_path,
#                 "abun_png_path": acq.abun_png_path
#             },
#             "pge_run_command": " ".join(cmd_aws),
#             "documentation_version": "TBD",
#             "product_creation_time": cnm_creation_time,
#             "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
#             "completion_status": "SUCCESS",
#             "output": {
#                 "l2b_abun_ummg_path:": ummg_path,
#                 "l2b_abun_cnm_submission_path": cnm_submission_path
#             }
#         }
#         dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
