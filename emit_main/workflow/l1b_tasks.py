"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import luigi

from emit_main.workflow.acquisition import Acquisition
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
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acq.rdn_img_path)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]
        cmd = ["python", pge.emitrdn_exe]
#        pge.run(cmd)

        cmd = ["touch", acq.rdn_img_path]
        pge.run(cmd)
        cmd = ["touch", acq.rdn_hdr_path]
        pge.run(cmd)


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
        acq = Acquisition(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return (ENVITarget(acq.loc_img_path),
                ENVITarget(acq.obs_img_path),
                ENVITarget(acq.glt_img_path))

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1b"]

        cmd = ["touch", acq.loc_img_path]
        pge.run(cmd)
        cmd = ["touch", acq.loc_hdr_path]
        pge.run(cmd)
        cmd = ["touch", acq.obs_img_path]
        pge.run(cmd)
        cmd = ["touch", acq.obs_hdr_path]
        pge.run(cmd)
        cmd = ["touch", acq.glt_img_path]
        pge.run(cmd)
        cmd = ["touch", acq.glt_hdr_path]
        pge.run(cmd)

