"""
This code contains test tasks for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os

import luigi

from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask


class ExampleTask(SlurmJobTask):

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        return None

    def output(self):

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        return luigi.LocalTarget(acq.rdn_hdr_path)

    def work(self):

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        os.system(" ".join(["touch", acq.rdn_hdr_path]))
