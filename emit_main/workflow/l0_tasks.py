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
class L0StripHOSC(SlurmJobTask):
    """
    Strips HOSC ethernet headers from raw data in apid-specific ingest folder
    :returns Ordered APID specific packet stream
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):
        logger.debug(self.task_family + " requires")
        return None

    def output(self):
        logger.debug(self.task_family + " output")
        return luigi.LocalTarget("ccsds_path")

    def work(self):
        logger.debug(self.task_family + " work")

        wm = WorkflowManager(self.config_path)
        pge_sds_runner = wm.pges["emit-sds-l0"]
        pge_ios_processer = wm.pges["emit-l0edp"]
