"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import luigi
import os

from emit_workflow.envi_target import ENVITarget
from emit_workflow.file_manager import FileManager
from emit_workflow.l0_tasks import L0StripEthernet

# TODO: Full implementation TBD
class L1ADepacketize(luigi.Task):
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

    def run(self):

        pass

# TODO: Full implementation TBD
class L1APrepFrames(luigi.Task):
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

    def run(self):

        pass

# TODO: Full implementation TBD
class L1AReassembleRaw(luigi.Task):
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

        fm = FileManager(self.acquisition_id, self.config_path)
        #return luigi.LocalTarget(fm.l1a["raw_img"])
        return ENVITarget(fm.l1a["raw_img"])

    def run(self):

        fm = FileManager(self.acquisition_id, self.config_path)
        os.system(" ".join(["touch", fm.l1a["raw_img"]]))
        os.system(" ".join(["touch", fm.l1a["raw_hdr"]]))

# TODO: Full implementation TBD
class L1APEP(luigi.Task):
    """
    Performs performance evaluation of raw data
    :returns: Perfomance evaluation report
    """

    task_namespace = "emit"

    def requires(self):

        return L1AReassembleRaw()

    def output(self):

        return luigi.LocalTarget("pep_path")

    def run(self):

        pass
