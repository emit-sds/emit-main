"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import luigi

from emit_main.acquisition import Acquisition
from emit_main.database_manager import DatabaseManager
from emit_main.envi_target import ENVITarget
from emit_main.workflow_manager import FileManager
from emit_main.l1a_tasks import L1AReassembleRaw
from emit_main.slurm import SlurmJobTask

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
        return L1AReassembleRaw(config_path=self.config_path, acquisition_id=self.acquisition_id)

    def output(self):

        logger.debug(self.task_family + " output")
        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(fm.rdn_img_path)

    def work(self):

        logger.debug(self.task_family + " run")

        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        pge = fm.pges["emit-sds-l1b"]
#        cmd = ["python", fm.emitrdn_exe]
#        pge.run(cmd)

        cmd = ["touch", fm.rdn_img_path]
        pge.run(cmd)
        cmd = ["touch", fm.rdn_hdr_path]
        pge.run(cmd)

        # # Placeholder: PGE writes metadata to db
        # metadata = {
        #     "lines": 5500,
        #     "bands": 324,
        #     "samples": 1280,
        #     "start_time": datetime.datetime.utcnow(),
        #     "end_time": datetime.datetime.utcnow() + datetime.timedelta(minutes=11),
        #     "orbit": "00001",
        #     "scene": "001"
        # }
        # acquisition = Acquisition(self.acquisition_id, metadata)
        #
        # dm = DatabaseManager(self.config_path)
        # acquisitions = dm.db.acquisitions
        # query = {"_id": self.acquisition_id}
        #
        # acquisitions.delete_one(query)
        #
        # acquisition_id = acquisitions.insert_one(acquisition.__dict__).inserted_id
        #
        # #acquisitions.update(query, acquisition.__dict__, upsert=True)


# TODO: Full implementation TBD
class L1BGeolocate(SlurmJobTask):
    """
    Performs geolocation using BAD telemetry and counter-OS time pair file
    :returns: Geolocation files including GLT, OBS, LOC, corrected attitude and ephemeris
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1BCalibrate(config_path=self.config_path, acquisition_id=self.acquisition_id)

    def output(self):

        logger.debug(self.task_family + " output")
        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        return (ENVITarget(fm.loc_img_path),
                ENVITarget(fm.obs_img_path),
                ENVITarget(fm.glt_img_path))

    def work(self):

        logger.debug(self.task_family + " run")

        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        pge = fm.pges["emit-sds-l1b"]
        cmd = ["touch", fm.loc_img_path]
        pge.run(cmd)
        cmd = ["touch", fm.loc_hdr_path]
        pge.run(cmd)
        cmd = ["touch", fm.obs_img_path]
        pge.run(cmd)
        cmd = ["touch", fm.obs_hdr_path]
        pge.run(cmd)
        cmd = ["touch", fm.glt_img_path]
        pge.run(cmd)
        cmd = ["touch", fm.glt_hdr_path]
        pge.run(cmd)

