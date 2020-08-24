"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import luigi

from emit_main.acquisition import Acquisition
from emit_main.database_manager import DatabaseManager
from emit_main.envi_target import ENVITarget
from emit_main.workflow_manager import FileManager
from emit_main.l0_tasks import L0StripEthernet
from emit_main.slurm import SlurmJobTask

logger = logging.getLogger("emit-workflow")


# TODO: Full implementation TBD
class L1ADepacketize(SlurmJobTask):
    """
    Depacketizes CCSDS packet streams
    :returns: Reconstituted science frames, engineering data, or BAD telemetry depending on APID
    """

    config_path = luigi.Parameter()
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

    config_path = luigi.Parameter()
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

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        # This task must be triggered once a complete set of frames
        return None

    def output(self):

        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(fm.raw_img_path)

    def work(self):

        fm = FileManager(self.config_path, acquisition_id=self.acquisition_id)
        fm.touch_path(fm.raw_img_path)
        fm.touch_path(fm.raw_hdr_path)

        # Placeholder: PGE writes metadata to db
        metadata = {
            "lines": 5500,
            "bands": 324,
            "samples": 1280,
            "start_time": datetime.datetime.utcnow(),
            "end_time": datetime.datetime.utcnow() + datetime.timedelta(minutes=11),
            "orbit": "00001",
            "scene": "001"
        }
        acquisition = Acquisition(self.acquisition_id, metadata)

        dm = DatabaseManager(self.config_path)
        acquisitions = dm.db.acquisitions
        query = {"_id": self.acquisition_id}

        acquisitions.delete_one(query)

        acquisition_id = acquisitions.insert_one(acquisition.__dict__).inserted_id
        #
        # #acquisitions.update(query, acquisition.__dict__, upsert=True)


# TODO: Full implementation TBD
class L1APEP(SlurmJobTask):
    """
    Performs performance evaluation of raw data
    :returns: Perfomance evaluation report
    """

    config_path = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        return L1AReassembleRaw()

    def output(self):

        return luigi.LocalTarget("pep_path")

    def work(self):

        pass
