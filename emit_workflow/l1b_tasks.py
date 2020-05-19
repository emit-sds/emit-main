"""
This code contains tasks for executing EMIT Level 1B PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import luigi
import os

from emit_workflow.envi_target import ENVITarget
from emit_workflow.file_manager import FileManager
from emit_workflow.l1a_tasks import L1AReassembleRaw

# TODO: Full implementation TBD
class L1BCalibrate(luigi.Task):
    """
    Performs calibration of raw data to produce radiance
    :returns: Spectrally calibrated radiance
    """
    task_namespace = "emit"
    acquisition = luigi.Parameter()
    config_path = luigi.Parameter()

    def requires(self):

        return L1AReassembleRaw(self.acquisition)

    def output(self):

        fm = FileManager(self.acquisition, self.config_path)
        return ENVITarget(fm.l1b["rdn_img"])

    def run(self):

        fm = FileManager(self.acquisition, self.config_path)
        os.system(" ".join(["touch", fm.l1b["rdn_img"]]))
        os.system(" ".join(["touch", fm.l1b["rdn_hdr"]]))

# TODO: Full implementation TBD
class L1BGeolocate(luigi.Task):
    """
    Performs geolocation using BAD telemetry and counter-OS time pair file
    :returns: Geolocation files including GLT, OBS, LOC, corrected attitude and ephemeris
    """

    task_namespace = "emit"

    def requires(self):

        return L1BCalibrate()

    def output(self):

        return luigi.LocalTarget("raw_file")

    def run(self):

        pass

