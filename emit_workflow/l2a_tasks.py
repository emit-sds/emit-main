"""
This code contains tasks for executing EMIT Level 2A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import luigi

from acquisition import Acquisition
from database_manager import DatabaseManager
from envi_target import ENVITarget
from file_manager import FileManager
from l1b_tasks import L1BCalibrate, L1BGeolocate
from pge import PGE
from slurm import SlurmJobTask

logger = logging.getLogger("emit-workflow")


# TODO: Full implementation TBD
class L2AReflectance(SlurmJobTask):
    """
    Performs atmospheric correction on radiance
    :returns: Surface reflectance and uncertainties
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return (L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id),
                L1BGeolocate(config_path=self.config_path, acquisition_id=self.acquisition_id))

    def output(self):

        logger.debug(self.task_family + " output")
        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        return (ENVITarget(fm.rfl_img_path),
                ENVITarget(fm.uncert_img_path),
                ENVITarget(fm.mask_img_path))

    def work(self):

        logger.debug(self.task_family + " run")

        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        pge = fm.pges["isofit"]
        logger.debug("isofit version is %s" % pge.version)
#        cmd = ["python", fm.emitrdn_exe]
#        pge.run(cmd)



