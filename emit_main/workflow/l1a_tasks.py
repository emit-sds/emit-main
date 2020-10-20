"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import logging
import os
import shutil

import luigi
import spectral.io.envi as envi

from emit_main.workflow.stream_target import StreamTarget
from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l0_tasks import L0StripHOSC
from emit_main.workflow.slurm import SlurmJobTask

logger = logging.getLogger("emit-main")


# TODO: Full implementation TBD
class L1ADepacketize(SlurmJobTask):
    """
    Depacketizes CCSDS packet streams
    :returns: Reconstituted science frames, engineering data, or BAD telemetry depending on APID
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    start_time = luigi.DateSecondParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_time = luigi.DateSecondParameter(default=datetime.date.today())

    task_namespace = "emit"

    def requires(self):

        return L0StripHOSC(apid=self.apid, start_time=self.start_time, end_time=self.end_time)

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
    stream_path = luigi.Parameter()
    start_time = luigi.DateSecondParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_time = luigi.DateSecondParameter(default=datetime.date.today())

    task_namespace = "emit"

    def requires(self):

        return L0StripHOSC(apid=self.apid, start_time=self.start_time, end_time=self.end_time)

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
#    frames_dir = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        # This task must be triggered once a complete set of frames

        #FIXME: Acquisition insertion should be happening in previous step.  This is temporary for testing.
        wm = WorkflowManager(self.config_path, acquisition_id=self.acquisition_id)
        acq_meta = {
            "acquisition_id": "emit20200101t000000",
            "build_num": wm.build_num,
            "processing_version": wm.processing_version,
            "start_time": datetime.datetime(2020, 1, 1, 0, 0, 0),
            "end_time": datetime.datetime(2020, 1, 1, 0, 11, 26),
            "orbit": "00001",
            "scene": "001"
        }
        wm.database_manager.insert_acquisition(acq_meta)
        return None

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        if acq is None:
            return None
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
#        wm.touch_path(tmp_raw_img_path)
#        wm.touch_path(tmp_raw_hdr_path)
        test_data_raw_img_path = os.path.join(wm.data_dir, "test_data", os.path.basename(acq.raw_img_path))
        test_data_raw_hdr_path = os.path.join(wm.data_dir, "test_data", os.path.basename(acq.raw_hdr_path))
        shutil.copy(test_data_raw_img_path, tmp_raw_img_path)
        shutil.copy(test_data_raw_hdr_path, tmp_raw_hdr_path)

        # Placeholder: copy tmp folder back to l1a dir and rename?
        proc_dir = os.path.join(acq.l1a_data_dir, os.path.basename(acq.raw_img_path.replace(".img", "_proc")))
        if os.path.exists(proc_dir):
            shutil.rmtree(proc_dir)
        shutil.copytree(self.tmp_dir, proc_dir)

        # Placeholder: move output files to l1a dir
        for file in glob.glob(os.path.join(proc_dir, "output", "*")):
            shutil.move(file, acq.l1a_data_dir)

        # Placeholder: update hdr files
        hdr = envi.read_envi_header(acq.raw_hdr_path)
        hdr["description"] = "EMIT L1A raw instrument data (units: DN)"
        hdr["emit acquisition start time"] = datetime.datetime(2020, 1, 1, 0, 0, 0).strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit acquisition stop time"] = datetime.datetime(2020, 1, 1, 0, 11, 26).strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit pge name"] = pge.repo_name
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = [
            "file1_key=file1_value",
            "file2_key=file2_value"
        ]
        hdr["emit pge run command"] = "python l1a_run.py args"
        hdr["emit software build version"] = wm.build_num
        hdr["emit documentation version"] = "v1"
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.raw_img_path))
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S")
        hdr["emit data product version"] = "v" + wm.processing_version

        envi.write_envi_header(acq.raw_hdr_path, hdr)

        # Placeholder: PGE writes metadata to db
        metadata = {
            "lines": hdr["lines"],
            "bands": hdr["bands"],
            "samples": hdr["samples"],
            "day_night_flag": "day"
        }

#        acq.save_metadata(metadata)
        dm = wm.database_manager
        dm.update_acquisition_metadata(self.acquisition_id, metadata)

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_name,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "file1_key": "file1_value",
                "file2_key": "file2_value",
            },
            "pge_run_command": "python l1a_run.py args",
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_raw_path": acq.raw_img_path,
                "l1a_raw_hdr_path:": acq.raw_hdr_path
            }
        }

#        acq.save_processing_log_entry(log_entry)
        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


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


# TODO: Full implementation TBD
class L1AReformatEDP(SlurmJobTask):
    """
    Creates reformatted engineering data product from CCSDS packet stream
    :returns: Reformatted engineering data product
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L0StripHOSC(config_path=self.config_path, stream_path=self.stream_path)

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " work")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream
        pge = wm.pges["emit-sds-l1a"]

        # Build command and run
        sds_l1a_eng_exe = os.path.join(pge.repo_dir, "run_l1a_eng.sh")
        ios_l1_edp_exe = os.path.join(wm.pges["emit-ios"].repo_dir, "emit", "bin", "emit_l1_edp.py")
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        tmp_log_dir = os.path.join(self.tmp_dir, "logs")

        tmp_log = os.path.join(tmp_output_dir, stream.hosc_name + ".log")

        cmd = [sds_l1a_eng_exe, stream.ccsds_path, self.tmp_dir, ios_l1_edp_exe]
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, env=env)

        # Copy scratch files back to store
        for file in glob.glob(os.path.join(tmp_output_dir, "*")):
            shutil.copy(file, stream.l1a_dir)
        for file in glob.glob(os.path.join(tmp_log_dir, "*")):
            shutil.copy(file, stream.l1a_dir)
        # Get edp output filename
        edp_name = os.path.basename(glob.glob(os.path.join(tmp_output_dir, "*.csv"))[0])
        edp_path = os.path.join(stream.l1a_dir, edp_name)

        query = {
            "hosc_name": stream.hosc_name,
            "build_num": wm.build_num,
        }
        metadata = {
            "edp_name": edp_name
        }
        dm = wm.database_manager
        dm.update_stream_metadata(query, metadata)

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_name,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ccsds_path": stream.ccsds_path,
            },
            "pge_run_command": " ".join(cmd),
            "product_creation_time": datetime.datetime.fromtimestamp(os.path.getmtime(edp_path)),
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "SUCCESS",
            "output": {
                "edp_path": edp_path
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)