"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import luigi

from envi_target import ENVITarget
from file_manager import FileManager
from l0_tasks import L0StripEthernet
from slurm import SlurmJobTask

logger = logging.getLogger("emit-workflow")


# TODO: Full implementation TBD
class L1ADepacketize(SlurmJobTask):
    """
    Depacketizes CCSDS packet streams
    :returns: Reconstituted science frames, engineering data, or BAD telemetry depending on APID
    """

    apid = luigi.Parameter()
    start_time = luigi.DateSecondParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_time = luigi.DateSecondParameter(default=datetime.date.today())

    task_namespace = "emit"

    def requires(self):

        return L0StripEthernet(apid=self.apid, start_time=self.start_time, end_time=self.end_time)

    def output(self):

        return luigi.LocalTarget("depacketized_directory_by_apid")

    def work(self):

        pass


# TODO: Full implementation TBD
class L1APrepFrames(SlurmJobTask):
    """
    Orders compressed frames and checks for a complete set for a given DCID
    :returns: Folder containing a complete set of compressed frames
    """

    apid = luigi.Parameter()
    start_time = luigi.DateSecondParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_time = luigi.DateSecondParameter(default=datetime.date.today())

    task_namespace = "emit"

    def requires(self):

        return L0StripEthernet(apid=self.apid, start_time=self.start_time, end_time=self.end_time)

    def output(self):

        return luigi.LocalTarget("depacketized_directory_by_apid")

    def work(self):

        pass


# TODO: Full implementation TBD
class L1AReassembleRaw(SlurmJobTask):
    """
    Decompresses science frames and assembles them into time-ordered acquisitions
    :returns: Uncompressed raw acquisitions in binary cube format (ENVI compatible)
    """
    task_namespace = "emit"
    acquisition_id = luigi.Parameter()
    config_path = luigi.Parameter()

    def requires(self):

        # This task must be triggered once a complete set of frames
        return None

    def output(self):

        fm = FileManager(acquisition_id=self.acquisition_id, config_path=self.config_path)
        return ENVITarget(fm.paths["raw_img"])

    def work(self):

        fm = FileManager(acquisition_id=self.acquisition_id, config_path=self.config_path)
        fm.touch_path(fm.paths["raw_img"])
        fm.touch_path(fm.paths["raw_hdr"])


# TODO: Full implementation TBD
class L1APEP(SlurmJobTask):
    """
    Performs performance evaluation of raw data
    :returns: Perfomance evaluation report
    """

    task_namespace = "emit"

    def requires(self):

        return L1AReassembleRaw()

    def output(self):

        return luigi.LocalTarget("pep_path")

    def work(self):

        pass
