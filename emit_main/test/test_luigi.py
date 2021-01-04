"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os

import luigi

from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask


class ExampleTask(SlurmJobTask):

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

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
        if os.path.exists(acq.rdn_hdr_path):
            os.system(" ".join(["rm", "-f", acq.rdn_hdr_path]))
        os.system(" ".join(["touch", acq.rdn_hdr_path]))


def test_luigi_build(config_path):

    print("\nRunning test_luigi_build with config: %s" % config_path)

    acquisition_id = "emit20200101t000000"

    wm = WorkflowManager(config_path=config_path, acquisition_id=acquisition_id)

    luigi_logging_conf = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "workflow", "luigi",
                                                           "logging.conf"))
    success = luigi.build(
        [ExampleTask(config_path=config_path, acquisition_id=acquisition_id)],
        workers=wm.luigi_workers,
        local_scheduler=wm.luigi_local_scheduler,
        logging_conf_file=luigi_logging_conf)

    assert success
