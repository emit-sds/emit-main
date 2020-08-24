"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import luigi

from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1a_tasks import L1AReassembleRaw
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


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
        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(wm.rdn_img_path)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        pge = wm.pges["emit-sds-l1b"]
#        cmd = ["python", wm.emitrdn_exe]
#        pge.run(cmd)

        cmd = ["touch", wm.rdn_img_path]
        pge.run(cmd)
        cmd = ["touch", wm.rdn_hdr_path]
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
        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        return (ENVITarget(wm.loc_img_path),
                ENVITarget(wm.obs_img_path),
                ENVITarget(wm.glt_img_path))

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        pge = wm.pges["emit-sds-l1b"]
        cmd = ["touch", wm.loc_img_path]
        pge.run(cmd)
        cmd = ["touch", wm.loc_hdr_path]
        pge.run(cmd)
        cmd = ["touch", wm.obs_img_path]
        pge.run(cmd)
        cmd = ["touch", wm.obs_hdr_path]
        pge.run(cmd)
        cmd = ["touch", wm.glt_img_path]
        pge.run(cmd)
        cmd = ["touch", wm.glt_hdr_path]
        pge.run(cmd)

