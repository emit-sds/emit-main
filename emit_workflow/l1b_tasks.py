"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import luigi
import sys

from envi_target import ENVITarget
from file_manager import FileManager
from l1a_tasks import L1AReassembleRaw
from slurm import SlurmJobTask

logger = logging.getLogger("emit-workflow")


# TODO: Full implementation TBD
class L1BCalibrate(SlurmJobTask):
    """
    Performs calibration of raw data to produce radiance
    :returns: Spectrally calibrated radiance
    """
    task_namespace = "emit"
    acquisition_id = luigi.Parameter()
    config_path = luigi.Parameter()

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1AReassembleRaw(acquisition_id=self.acquisition_id, config_path=self.config_path)

    def output(self):

        logger.debug(self.task_family + " output")
        fm = FileManager(acquisition_id=self.acquisition_id, config_path=self.config_path)
        return ENVITarget(fm.paths["rdn_img"])

    def work(self):

        fm = FileManager(acquisition_id=self.acquisition_id, config_path=self.config_path)
        fm.touch_path(fm.paths["rdn_img"])
        fm.touch_path(fm.paths["rdn_hdr"])
        logger.debug(self.task_family + " run")


# TODO: Full implementation TBD
class L1BGeolocate(SlurmJobTask):
    """
    Performs geolocation using BAD telemetry and counter-OS time pair file
    :returns: Geolocation files including GLT, OBS, LOC, corrected attitude and ephemeris
    """

    task_namespace = "emit"

    def requires(self):

        return L1BCalibrate()

    def output(self):

        return luigi.LocalTarget("raw_file")

    def work(self):

        pass

