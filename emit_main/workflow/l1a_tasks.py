"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import logging
import luigi
import os
import shutil

from emit_main.workflow.acquisition import Acquisition
from emit_main.database.database_manager import DatabaseManager
from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l0_tasks import L0StripEthernet
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


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

        logger.debug(self.task_family + " requires")
        # This task must be triggered once a complete set of frames
        return None

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        if acq is None:
            return False
        else:
            return ENVITarget(acq.raw_img_path)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1a"]

        # Placeholder: PGE writes to tmp folder
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        os.makedirs(tmp_output_dir)
        tmp_raw_img_path = os.path.join(tmp_output_dir, os.path.basename(acq.raw_img_path))
        tmp_raw_hdr_path = os.path.join(tmp_output_dir, os.path.basename(acq.raw_hdr_path))
        wm.touch_path(tmp_raw_img_path)
        wm.touch_path(tmp_raw_hdr_path)

        # Placeholder: copy tmp folder back to l1a dir and rename?
        raw_dir = os.path.join(acq.l1a_data_dir, os.path.basename(acq.raw_img_path.replace(".img", ".dir")))
        if os.path.exists(raw_dir):
            shutil.rmtree(raw_dir)
        shutil.copytree(self.tmp_dir, raw_dir)

        # Placeholder: move output files to l1a dir
        for file in glob.glob(os.path.join(raw_dir, "output", "*")):
            shutil.move(file, acq.l1a_data_dir)

#        wm.copy_tmp_dir_and_outputs()

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
        acquisition = Acquisition(self.config_path, self.acquisition_id, metadata)
        acquisition.save()

        # dm = DatabaseManager(self.config_path)
        # acquisitions = dm.db.acquisitions
        # query = {"_id": self.acquisition_id}
        #
        # acquisitions.delete_one(query)
        #
        # acquisition_id = acquisitions.insert_one(acquisition.metadata).inserted_id


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
