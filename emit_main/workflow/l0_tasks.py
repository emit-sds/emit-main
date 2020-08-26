"""
This code contains tasks for executing EMIT Level 0 PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import luigi

from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")

# TODO: Full implementation TBD
class L0StripEthernet(SlurmJobTask):
    """
    Strips HOSC ethernet headers from raw data in ingest folder
    :returns Ordered APID specific packet stream
    """

    config_path = luigi.Parameter()
    apid = luigi.Parameter()
    start_time = luigi.DateSecondParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_time = luigi.DateSecondParameter(default=datetime.date.today())

    task_namespace = "emit"

    def requires(self):

        return None

    def output(self):

        wm = WorkflowManager(self.config_path)
        return luigi.LocalTarget("ccsds_path")

    def work(self):

        pass
