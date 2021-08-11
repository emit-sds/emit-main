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


class L1ADepacketizeScienceFrames(SlurmJobTask):
    """
    Depacketizes CCSDS packet stream for science data (APID 1675) and writes out frames
    :returns: Reconstituted science frames, engineering data, or BAD telemetry depending on APID
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return None

    def output(self):

        logger.debug(self.task_family + " output")
        return None

    def work(self):

        logger.debug(self.task_family + " work")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        dm = wm.database_manager
        stream = wm.stream
        pge = wm.pges["emit-sds-l1a"]

        # Build command and run
        sds_l1a_science_packet_exe = os.path.join(pge.repo_dir, "depacketize_science_frames.py")
        tmp_output_dir = os.path.join(self.tmp_dir, "output")
        tmp_log_path = os.path.join(self.tmp_dir, "depacketize_science_frames.log")
        tmp_report_path = tmp_log_path.replace(".log", "_report.txt")
        cmd = ["python", sds_l1a_science_packet_exe, stream.ccsds_path,
               "--out_dir", tmp_output_dir,
               "--level", self.level,
               "--log_path", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Based on DCIDs, copy frames to appropriate acquisition l1a frames directory.
        # Also, attach stream files to acquisition object in DB and add frames as well
        frames = [os.path.basename(file) for file in glob.glob(os.path.join(tmp_output_dir, "*"))]
        frames.sort()
        dcids = set([frame.split("_")[0] for frame in frames])
        logger.debug(f"Found frames {frames} and dcids {dcids}")
        acquisition_frames_map = {}
        output_frame_paths = []
        for dcid in dcids:
            acq = dm.find_acquisition_by_dcid(dcid)
            if acq is None:
                raise RuntimeError(f"Unable to find acquisition in DB with dcid {dcid}")
            wm = WorkflowManager(config_path=self.config_path, acquisition_id=acq["acquisition_id"],
                                 stream_path=self.stream_path)
            acq = wm.acquisition
            stream = wm.stream

            # Copy the frames
            acq_frame_paths = []
            for path in glob.glob(os.path.join(tmp_output_dir, dcid + "*")):
                # TODO: Keep DCID here for future reference and rename on later step?
                # Replace dcid with acquisition id on copy
                fname_tokens = os.path.basename(path).split("_")
                fname_tokens[0] = acq.acquisition_id
                acquisition_frame_path = os.path.join(acq.frames_dir, "_".join(fname_tokens))
                shutil.copy2(path, acquisition_frame_path)
                acq_frame_paths.append(acquisition_frame_path)

            # Create a symlink from the stream l1a dir to the acquisition l1a frames dir
            acq_frame_symlink = os.path.join(stream.frames_dir, os.path.basename(acq.frames_dir))
            if not os.path.exists(acq_frame_symlink):
                os.symlink(acq.frames_dir, acq_frame_symlink)

            # Add frame paths to acquisition metadata
            if "frames" in acq.metadata["products"]["l1a"] and acq.metadata["products"]["l1a"]["frames"] is not None:
                for path in acq_frame_paths:
                    if path not in acq.metadata["products"]["l1a"]["frames"]:
                        acq.metadata["products"]["l1a"]["frames"] += [path]
            else:
                acq.metadata["products"]["l1a"]["frames"] = acq_frame_paths
            acq.metadata["products"]["l1a"]["frames"].sort()
            dm.update_acquisition_metadata(
                acq.acquisition_id,
                {"products.l1a.frames": acq.metadata["products"]["l1a"]["frames"]})

            # Add stream file to acquisition metadata in DB so there is a link back to CCSDS packet stream for each
            # acquisition. There may be multiple stream files that contribute frames to a given acquisition
            if "associated_ccsds" in acq.metadata and acq.metadata["associated_ccsds"] is not None:
                if stream.ccsds_path not in acq.metadata["associated_ccsds"]:
                    acq.metadata["associated_ccsds"] += stream.ccsds_path
            else:
                acq.metadata["associated_ccsds"] = [stream.ccsds_path]
            dm.update_acquisition_metadata(acq.acquisition_id, {"associated_ccsds": acq.metadata["associated_ccsds"]})

            # Append frames to include in stream metadata
            acq_frame_paths.sort()
            acquisition_frames_map.update({acq.acquisition_id: acq_frame_paths})

            # Keep track of all output paths for log entry
            output_frame_paths += acq_frame_paths

        dm.update_stream_metadata(stream.ccsds_name, {"products.l1a": acquisition_frames_map})

        # Copy log file and report file into the stream's l1a directory
        sdp_log_name = stream.ccsds_name.replace("l0_ccsds", "l1a_frames").replace(".bin", "_pge.log")
        sdp_log_path = os.path.join(stream.l1a_dir, sdp_log_name)
        sdp_report_path = sdp_log_path.replace("_pge.log", "_report.txt")
        shutil.copy2(tmp_log_path, sdp_log_path)
        shutil.copy2(tmp_report_path, sdp_report_path)

        doc_version = "EMIT IOS SDS ICD JPL-D 104239, Initial"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ccsds_path": stream.ccsds_path,
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "frame_paths": output_frame_paths
            }
        }
        dm.insert_stream_log_entry(stream.ccsds_name, log_entry)


class L1AReassembleRaw(SlurmJobTask):
    """
    Decompresses science frames and assembles them into time-ordered acquisitions
    :returns: Uncompressed raw acquisitions in binary cube format (ENVI compatible)
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    ignore_missing = luigi.BoolParameter(default=False)
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):
        # This task requires a complete set of frames
        logger.debug(self.task_family + " requires")

        return None

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        # Check for missing frames before proceeding. Override with --ignore_missing arg
        if self.ignore_missing is False and acq.has_complete_set_of_frames() is False:
            raise RuntimeError(f"Unable to run {self.task_family} on {self.acquisition_id} due to missing frames in "
                               f"{acq.frames_dir}")

        pge = wm.pges["emit-sds-l1a"]
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        os.makedirs(tmp_output_dir)

        reassemble_raw_pge = os.path.join(pge.repo_dir, "reassemble_raw_cube.py")
        flex_pge = wm.pges["EMIT_FLEX_codec"]
        flex_codec_exe = os.path.join(flex_pge.repo_dir, "flexcodec")
        constants_path = wm.config["decompression_constants_path"]
        init_data_path = wm.config["decompression_init_data_path"]
        tmp_log_path = os.path.join(self.local_tmp_dir, "reassemble_raw_pge.log")
        tmp_report_path = tmp_log_path.replace(".log", "_report.txt")
        input_files = {
            "frames_dir": acq.frames_dir,
            "flexcodec_exe_path": flex_codec_exe,
            "constants_path": constants_path,
            "init_data_path": init_data_path
        }
        cmd = ["python", reassemble_raw_pge, acq.frames_dir,
               "--flexcodec_exe", flex_codec_exe,
               "--constants_path", constants_path,
               "--init_data_path", init_data_path,
               "--out_dir", tmp_output_dir,
               "--level", self.level,
               "--log_path", tmp_log_path]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        # Copy raw file and log back to l1a data dir
        tmp_raw_path = os.path.join(tmp_output_dir, acq.acquisition_id + "_raw.img")
        tmp_raw_hdr_path = tmp_raw_path.replace(".img", ".hdr")
        # TODO: Just use "raw" and not "dark"?
        # Rename to "raw" or "dark" depending on submode flag, but leave log and report files as "raw"
        reassembled_img_path = acq.dark_img_path if acq.submode == "dark" else acq.raw_img_path
        reassembled_hdr_path = acq.dark_hdr_path if acq.submode == "dark" else acq.raw_hdr_path
        shutil.copy2(tmp_raw_path, reassembled_img_path)
        shutil.copy2(tmp_raw_hdr_path, reassembled_hdr_path)
        shutil.copy2(tmp_log_path, acq.raw_img_path.replace(".img", "_pge.log"))
        report_path = acq.raw_img_path.replace(".img", "_report.txt")
        shutil.copy2(tmp_report_path, report_path)

        # Create rawqa report file based on CCSDS depacketization report(s) and reassembly report
        rawqa_file = open(acq.rawqa_txt_path, "w")

        # Get depacketization report from associated CCSDS files
        for path in acq.associated_ccsds:
            depacket_report_path = path.replace("l0", "l1a").replace("ccsds", "frames").replace(".bin", "_report.txt")
            if os.path.exists(depacket_report_path):
                with open(depacket_report_path, "r") as f:
                    rawqa_file.write("SOURCE FILE\n")
                    rawqa_file.write("===========\n")
                    rawqa_file.write(f"{depacket_report_path}\n\n")
                    rawqa_file.write(f.read() + "\n\n")
            else:
                logger.warning(f"Unable to find depacketization report located at {depacket_report_path}")

        # Get reassembly report
        if os.path.exists(report_path):
            with open(tmp_report_path, "r") as f:
                rawqa_file.write("SOURCE FILE\n")
                rawqa_file.write("===========\n")
                rawqa_file.write(f"{report_path}\n\n")
                rawqa_file.write(f.read())
        else:
            logger.warning(f"Unable to find reassembly report located at {report_path}")

        rawqa_file.close()

        # Copy decompressed frames to /store
        tmp_decomp_frame_paths = glob.glob(os.path.join(tmp_output_dir, "*.decomp"))
        for path in tmp_decomp_frame_paths:
            shutil.copy2(path, acq.decomp_dir)

        # Update hdr files
        input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
        doc_version = "EMIT SDS L1A JPL-D 104186, Initial"
        hdr = envi.read_envi_header(reassembled_hdr_path)
        hdr["emit acquisition start time"] = acq.start_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit acquisition stop time"] = acq.stop_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit pge name"] = pge.repo_url
        hdr["emit pge version"] = pge.version_tag
        hdr["emit pge input files"] = input_files_arr
        hdr["emit pge run command"] = " ".join(cmd)
        hdr["emit software build version"] = wm.config["build_num"]
        hdr["emit documentation version"] = doc_version
        creation_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(reassembled_img_path), tz=datetime.timezone.utc)
        hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        hdr["emit data product version"] = wm.config["processing_version"]
        envi.write_envi_header(reassembled_hdr_path, hdr)

        # PGE writes metadata to db
        dm = wm.database_manager

        # Update raw product dictionary
        product_dict_raw = {
            "img_path": reassembled_img_path,
            "hdr_path": reassembled_hdr_path,
            "created": creation_time,
            "dimensions": {
                "lines": hdr["lines"],
                "samples": hdr["samples"],
                "bands": hdr["bands"]
            }
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1a.raw": product_dict_raw})

        # Update rawqa product dictionary
        product_dict_rawqa = {
            "txt_path": acq.rawqa_txt_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.rawqa_txt_path), tz=datetime.timezone.utc)
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1a.rawqa": product_dict_rawqa})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_raw_img_path": reassembled_img_path,
                "l1a_raw_hdr_path:": reassembled_hdr_path,
                "l1a_rawqa_txt_path": acq.rawqa_txt_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1AFrameReport(SlurmJobTask):
    """
    Runs the ngis_check_frame.py script.
    :returns: A frame report, frame header and line header CSVs for all frames in an acquisition
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L1AReassembleRaw(config_path=self.config_path, acquisition_id=self.acquisition_id, level=self.level,
                                partition=self.partition)

    def output(self):

        logger.debug(self.task_family + " output")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return ENVITarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(self.task_family + " run")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["NGIS_Check_Line_Frame"]

        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        os.makedirs(tmp_output_dir)

        # Create symlinks of decompressed frames
        input_decomp_frame_paths = glob.glob(os.path.join(acq.decomp_dir, "*.decomp"))
        for decomp_frame_path in input_decomp_frame_paths:
            decomp_frame_symlink = os.path.join(tmp_output_dir, os.path.basename(decomp_frame_path))
            os.symlink(decomp_frame_path, decomp_frame_symlink)

        ngis_check_list_exe = os.path.join(pge.repo_dir, "python", "ngis_check_list.py")
        cmd = ["python", ngis_check_list_exe, tmp_output_dir]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        output_files = glob.glob(os.path.join(tmp_output_dir, "*.csv"))
        output_files += glob.glob(os.path.join(tmp_output_dir, "*.txt"))
        for file in output_files:
            shutil.copy2(file, acq.decomp_dir)

        # PGE writes metadata to db
        dm = wm.database_manager

        all_frames_report = glob.glob(os.path.join(acq.decomp_dir, "*allframesreport.txt"))[0]
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(all_frames_report), tz=datetime.timezone.utc)

        # Update frames_report product dictionary
        product_dict = {
            "txt_path": all_frames_report,
            "created": creation_time
        }
        dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1a.frames_report": product_dict})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {"decomp_dir": acq.decomp_dir},
            "pge_run_command": " ".join(cmd),
            "documentation_version": "N/A",
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_decomp_dir": acq.decomp_dir
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1AReformatEDP(SlurmJobTask):
    """
    Creates reformatted engineering data product from CCSDS packet stream
    :returns: Reformatted engineering data product
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"

    def requires(self):

        logger.debug(self.task_family + " requires")
        return L0StripHOSC(config_path=self.config_path, stream_path=self.stream_path, level=self.level,
                           partition=self.partition)

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
        # TODO: Convert these to ancillary file paths?
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Get tmp edp and log names
        tmp_edp_path = glob.glob(os.path.join(tmp_output_dir, "*.csv"))[0]
        tmp_report_path = glob.glob(os.path.join(tmp_output_dir, "*_report.txt"))[0]

        # Construct EDP filename and report name based on ccsds name
        edp_name = stream.ccsds_name.replace("l0_ccsds", "l1a_eng").replace(".bin", ".csv")
        edp_path = os.path.join(stream.l1a_dir, edp_name)
        report_path = edp_path.replace(".csv", "_report.txt")

        # Copy scratch EDP file and report back to store
        shutil.copy2(tmp_edp_path, edp_path)
        shutil.copy2(tmp_report_path, report_path)

        # Copy and rename log file
        l1a_pge_log_path = edp_path.replace(".csv", "_pge.log")
        for file in glob.glob(os.path.join(tmp_log_dir, "*")):
            shutil.copy2(file, l1a_pge_log_path)

        metadata = {
            "edp_name": edp_name,

        }
        dm = wm.database_manager
        dm.update_stream_metadata(stream.hosc_name, metadata)

        product_dict = {
            "edp_path": edp_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(edp_path), tz=datetime.timezone.utc)
        }

        dm.update_stream_metadata(stream.hosc_name, {"products.l1a": product_dict})

        doc_version = "EMIT IOS SDS ICD JPL-D 104239, Initial"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "ccsds_path": stream.ccsds_path,
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": datetime.datetime.fromtimestamp(
                os.path.getmtime(edp_path), tz=datetime.timezone.utc),
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "edp_path": edp_path
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)
