"""
This code contains tasks pertaining DAAC delivery

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import luigi
import os

from emit_main.workflow.output_targets import DAACSceneNumbersTarget
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_utils.file_checks import get_gring_boundary_points, get_band_mean

logger = logging.getLogger("emit-main")


class AssignDAACSceneNumbers(SlurmJobTask):
    """
    Assigns DAAC scene numbers to all scenes in the orbit
    """

    config_path = luigi.Parameter()
    orbit_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    override_output = luigi.BoolParameter(default=False)

    memory = 18000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.orbit_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.orbit_id}")

        if self.override_output:
            return None

        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        orbit = wm.orbit
        dm = wm.database_manager

        # Get acquisitions in orbit
        acquisitions = dm.find_acquisitions_by_orbit_id(orbit.orbit_id, "science", min_valid_lines=0)
        acquisitions += dm.find_acquisitions_by_orbit_id(orbit.orbit_id, "dark", min_valid_lines=0)
        return DAACSceneNumbersTarget(acquisitions)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        orbit = wm.orbit
        pge = wm.pges["emit-main"]
        dm = wm.database_manager

        if not orbit.has_complete_raw() and not self.override_output:
            raise RuntimeError(f"Orbit {orbit.orbit_id} does not have complete set of raw acquisitions. Use "
                               f"--override_output if you want to continue to assign daac scene numbers.")

        # Get acquisitions in orbit
        acquisitions = dm.find_acquisitions_by_orbit_id(orbit.orbit_id, "science", min_valid_lines=0)
        acquisitions += dm.find_acquisitions_by_orbit_id(orbit.orbit_id, "dark", min_valid_lines=0)

        # Throw error if some acquisitions have daac scene numbers but others don't
        count = 0
        acq_ids = []
        for acq in acquisitions:
            if "daac_scene" in acq:
                count += 1
            acq_ids.append(acq["acquisition_id"])

        if not self.override_output and 0 < count < len(acquisitions):
            raise RuntimeError(f"While assigning scene numbers for DAAC, found some with scene numbers already. "
                               f"Aborting...")

        # Assign the scene numbers
        acq_ids = list(set(acq_ids))
        acq_ids.sort()
        daac_scene = 1
        for acq_id in acq_ids:
            dm.update_acquisition_metadata(acq_id, {"daac_scene": str(daac_scene).zfill(3)})

            log_entry = {
                "task": self.task_family,
                "pge_name": pge.repo_url,
                "pge_version": pge.version_tag,
                "pge_input_files": {
                    "orbit_id": orbit.orbit_id
                },
                "pge_run_command": "N/A - DB updates only",
                "documentation_version": "N/A",
                "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
                "completion_status": "SUCCESS",
                "output": {
                    "daac_scene_number": str(daac_scene).zfill(3)
                }
            }

            dm.insert_acquisition_log_entry(acq_id, log_entry)

            # Increment scene number
            daac_scene += 1

        # Update orbit metadata and processing log too
        num_scenes = len(acq_ids)
        dm.update_orbit_metadata(orbit.orbit_id, {"num_scenes": num_scenes})
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "orbit_id": orbit.orbit_id
            },
            "pge_run_command": "N/A - DB updates only",
            "documentation_version": "N/A",
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "number_of_scenes": num_scenes
            }
        }

        dm.insert_orbit_log_entry(self.orbit_id, log_entry)


class GetAdditionalMetadata(SlurmJobTask):
    """
    Looks up additional attributes (like gring, solar zenith, etc) and saves to DB for easy access
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
        return None

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]
        dm = wm.database_manager

        # Get additional attributes and add to DB
        glt_gring = get_gring_boundary_points(acq.glt_hdr_path)
        mean_solar_azimuth = get_band_mean(acq.obs_img_path, 3)
        mean_solar_zenith = get_band_mean(acq.obs_img_path, 4)
        meta = {
            "gring": glt_gring,
            "mean_solar_azimuth": mean_solar_azimuth,
            "mean_solar_zenith": mean_solar_zenith
        }
        dm.update_acquisition_metadata(acq.acquisition_id, meta)

        del meta["last_modified"]
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "l1b_glt_hdr_path": acq.glt_hdr_path,
                "l1b_obs_img_path": acq.obs_img_path
            },
            "pge_run_command": "N/A - DB updates only",
            "documentation_version": "N/A",
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": meta
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class ReconciliationReport(SlurmJobTask):
    """
    Create and submit a reconciliation report based on start and end dates
    """

    config_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    start_time = luigi.Parameter()
    stop_time = luigi.Parameter()

    memory = 18000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: None")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: None")
        return None

    def work(self):

        logger.debug(f"{self.task_family} work: {self.start_time} to {self.stop_time}")

        wm = WorkflowManager(config_path=self.config_path)
        pge = wm.pges["emit-main"]
        dm = wm.database_manager

        start = datetime.datetime.strptime(self.start_time, "%Y%m%dT%H%M%S")
        stop = datetime.datetime.strptime(self.stop_time, "%Y%m%dT%H%M%S")

        files = dm.find_files_for_reconciliation_report(start, stop)
        if len(files) == 0:
            raise RuntimeError(f"No files were found between {start} and {stop} for the "
                               f"reconciliation report. Exiting...")

        # Generate the report
        # utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        # report_name = f"EMIT_RECON_{self.start_time}_{self.stop_time}_{utc_now.strftime('%Y%m%dT%H%M%S')}.rpt"
        report_match = f"ER_{self.start_time}_{self.stop_time}*rpt"
        matching_reports = glob.glob(os.path.join(wm.reconciliation_dir, report_match))
        if len(matching_reports) == 0:
            report_name = f"ER_{self.start_time}_{self.stop_time}_v01.rpt"
        else:
            matching_reports.sort()
            # Get version
            ver = int(matching_reports[-1].split("_")[-1][1:3]) + 1
            report_name = f"ER_{self.start_time}_{self.stop_time}_v{str(ver).zfill(2)}.rpt"

        tmp_report_path = os.path.join(self.tmp_dir, report_name)
        with open(tmp_report_path, "w") as rf:
            # collection,collection_version,granuleId,fileName,fileSize,ingestTime,hash
            for f in files:
                line = ",".join([f["collection"], f["collection_version"], f["granule_ur"], f["daac_filename"],
                                 str(f["size"]), f["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"), f["checksum"]])
                rf.write(line + "\n")
        wm.change_group_ownership(tmp_report_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={wm.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{wm.daac_recon_staging_dir}/"
        # First set up permissions if needed
        env = "dev"
        if wm.config['environment'].startswith("test"):
            env = "test"
        if wm.config['environment'].startswith("ops"):
            env = "ops"
        group = f"emit-{env}"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{wm.daac_recon_staging_dir}'", "];", "then", "mkdir", f"{wm.daac_recon_staging_dir};",
                           "chgrp", group, f"{wm.daac_recon_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)
        # Rsync the files
        cmd_rsync = ["rsync", "-av", partial_dir_arg, log_file_arg, tmp_report_path, target]
        pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Create a submission file
        submission_dict = {
            "report": {
                "uri": os.path.join(wm.daac_recon_uri_base, report_name)
            }
        }
        tmp_submission_path = os.path.join(self.tmp_dir, report_name.replace(".rpt", "_submission.json"))
        with open(tmp_submission_path, "w") as f:
            f.write(json.dumps(submission_dict))

        # Submit notification via AWS SQS
        cmd_aws = [wm.config["aws_cli_exe"], "sns", "publish", "--topic-arn", wm.config["daac_reconciliation_arn"],
                   "--message", f"file://{tmp_submission_path}", "--profile", wm.config["aws_profile"]]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)

        # Copy reconciliation report and submission to reconciliation dir
        wm.copy(tmp_report_path, os.path.join(wm.reconciliation_dir, os.path.basename(tmp_report_path)))
        wm.copy(tmp_submission_path, os.path.join(wm.reconciliation_dir, os.path.basename(tmp_submission_path)))

        # Update granules with reconciliation report submission
        for f in files:
            dm.update_reconciliation_submission_status(f["daac_filename"], f["submission_id"], report_name, "submitted")
