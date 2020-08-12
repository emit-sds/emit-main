"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import luigi
import sys

from emit_workflow.acquisition import Acquisition
from emit_workflow.database_manager import DatabaseManager
from emit_workflow.envi_target import ENVITarget
from emit_workflow.file_manager import FileManager
from emit_workflow.l1a_tasks import L1AReassembleRaw
from emit_workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-workflow")


# TODO: Full implementation TBD
class L1BCalibrate(SlurmJobTask):
    """
    Performs calibration of raw data to produce radiance
    :returns: Spectrally calibrated radiance
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1AReassembleRaw(self.config_path, acquisition_id=self.acquisition_id)

    def output(self):

        logger.debug(self.task_family + " output")
        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(fm.paths["rdn_img"])

    def work(self):

        logger.debug(self.task_family + " run")

        # Placeholder: PGE creates files on filesystem
        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        fm.touch_path(fm.paths["rdn_img"])
        fm.touch_path(fm.paths["rdn_hdr"])

        # Placeholder: PGE writes metadata to db
        metadata = {
            "lines": 5500,
            "bands": 324,
            "samples": 1280,
            "start_time": datetime.datetime.utcnow(),
            "end_time": datetime.datetime.utcnow() + datetime.timedelta(minutes=11)
        }
        acquisition = Acquisition(self.acquisition_id, metadata)

        dm = DatabaseManager(self.config_path)
        acquisitions = dm.db.acquisitions
        query = {"_id": self.acquisition_id}

        acquisitions.delete_one(query)

        acquisition_id = acquisitions.insert_one(acquisition.__dict__).inserted_id

        #acquisitions.update(query, acquisition.__dict__, upsert=True)


# TODO: Full implementation TBD
class L1BGeolocate(SlurmJobTask):
    """
    Performs geolocation using BAD telemetry and counter-OS time pair file
    :returns: Geolocation files including GLT, OBS, LOC, corrected attitude and ephemeris
    """

    config_path = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        return L1BCalibrate()

    def output(self):

        return luigi.LocalTarget("raw_file")

    def work(self):

        pass

