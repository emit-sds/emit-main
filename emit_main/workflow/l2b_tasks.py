"""
This code contains tasks for executing EMIT Level 2B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os
import shutil

import luigi
import spectral.io.envi as envi

from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l2a_tasks import L2AMask
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


class L2BAbundance(SlurmJobTask):
    """
    Creates L2B mineral spectral abundance estimate file
    :returns: Mineral abundance file and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L2AMask(config_path=self.config_path, acquisition_id=self.acquisition_id)

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l2b"]

        # Build PGE commands for run_tetracorder_pge.sh
        run_tetra_exe = os.path.join(pge.repo_dir, "run_tetracorder_pge.sh")
        cmd_tetra = [run_tetra_exe, self.tmp_dir, acq.rfl_img_path]
        env = os.environ.copy()
        env["SP_LOCAL"] = "/shared/specpr"
        env["SP_BIN"] = "${SP_LOCAL}/bin"
        env["TETRA"] = "/shared/tetracorder5.26"
        env["PATH"] = "${PATH}:${SP_LOCAL}/bin:${TETRA}/bin:/usr/bin"
        pge.run(cmd_tetra, tmp_dir=self.tmp_dir, env=env)

        # Build aggregator cmd
        aggregator_exe = os.path.join(pge.repo_dir, "aggregator.py")
        tetra_out_dir = os.path.join(self.tmp_dir, "tetra_out")
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        os.makedirs(tmp_output_dir)
        tmp_abun_path = os.path.join(tmp_output_dir, os.path.basename(acq.abun_img_path))
        tmp_abun_hdr_path = tmp_abun_path + ".hdr"
        input_files = {
            "reflectance_file": acq.rfl_img_path,
            "reflectance_uncertainty_file": acq.uncert_img_path
        }
        cmd = ["python", aggregator_exe, tetra_out_dir, tmp_abun_path,
               "-calculate_uncertainty", "0",
               "-reflectance_file", acq.rfl_img_path,
               "-reflectance_uncertainty_file", acq.uncert_img_path]
        pge.run(cmd, cwd=pge.repo_dir, tmp_dir=self.tmp_dir)

        # Copy mask files to l2a dir
        shutil.copy2(tmp_abun_path, acq.abun_img_path)
        shutil.copy2(tmp_abun_hdr_path, acq.abun_hdr_path)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        hdr = envi.read_envi_header(acq.abun_hdr_path)
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.build_num
        hdr["emit documentation version"] = "TBD"
        # TODO: Get creation time separately for each file type?
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.abun_img_path))
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit data product version"] = wm.processing_version
        envi.write_envi_header(acq.abun_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager
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
                "l2b_abun_path": acq.abun_img_path,
                "l2b_abun_hdr_path:": acq.abun_hdr_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)
