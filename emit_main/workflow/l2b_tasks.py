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
        env["PATH"] = "${PATH}:${SP_LOCAL}/bin"
        env["SP_BIN"] = "${SP_LOCAL}/bin"
        env["TETRA"] = "/shared/tetracorder5.26"
        env["PATH"] = "${PATH}:${TETRA}/bin"
        pge.run(cmd_tetra, tmp_dir=self.tmp_dir, env=env)
