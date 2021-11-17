"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import logging
import os

import luigi
import spectral.io.envi as envi

from emit_main.workflow.data_collection_target import DataCollectionTarget
from emit_main.workflow.envi_target import ENVITarget
from emit_main.workflow.l0_tasks import L0StripHOSC
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.stream_target import StreamTarget
from emit_main.workflow.workflow_manager import WorkflowManager

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
    miss_pkt_thresh = luigi.FloatParameter()
    test_mode = luigi.BoolParameter(default=False)
    override_output = luigi.BoolParameter(default=False)

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.stream_path}")
        # TODO: Add dependency on previous stream file (if one is found in DB)
        return L0StripHOSC(config_path=self.config_path, stream_path=self.stream_path, level=self.level,
                           partition=self.partition, miss_pkt_thresh=self.miss_pkt_thresh)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.stream_path}")

        if self.override_output:
            return None

        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        dm = wm.database_manager
        stream = wm.stream
        pge = wm.pges["emit-sds-l1a"]

        # Build command and run
        sds_l1a_science_packet_exe = os.path.join(pge.repo_dir, "depacketize_science_frames.py")
        tmp_frames_dir = os.path.join(self.local_tmp_dir, "frames")
        tmp_log_path = os.path.join(self.local_tmp_dir, "depacketize_science_frames.log")
        tmp_report_path = tmp_log_path.replace(".log", "_report.txt")
        input_files = {"ccsds_path": stream.ccsds_path}
        cmd = ["python", sds_l1a_science_packet_exe, stream.ccsds_path,
               "--work_dir", self.local_tmp_dir,
               "--level", self.level,
               "--log_path", tmp_log_path]

        # Get previous stream path if exists
        # TODO: What should the search window be here for finding previous stream files?
        prev_streams = dm.find_streams_by_date_range("1675", "stop_time",
                                                     stream.start_time - datetime.timedelta(seconds=1),
                                                     stream.start_time + datetime.timedelta(minutes=1))
        prev_stream_path = None
        if prev_streams is not None and len(prev_streams) > 0:
            try:
                prev_stream_path = prev_streams[0]["products"]["l0"]["ccsds_path"]
            except KeyError:
                logger.warning(f"Could not find a previous stream path for {stream.ccsds_path} in DB.")
                pass

        if prev_stream_path is not None:
            wm_tmp = WorkflowManager(config_path=self.config_path, stream_path=prev_stream_path)
            prev_stream_report = wm_tmp.stream.frames_dir + "_report.txt"
            bytes_read = None
            if os.path.exists(prev_stream_report):
                with open(prev_stream_report, "r") as f:
                    for line in f.readlines():
                        if "Bytes read since last index" in line:
                            bytes_read = int(line.rstrip("\n").split(" ")[-1])
                            break

            # Append optional args and run
            cmd.extend(["--prev_stream_path", prev_stream_path])
            input_files["prev_stream_path"] = prev_stream_path
            if bytes_read is not None:
                cmd.extend(["--prev_bytes_to_read", str(bytes_read)])

        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Based on DCIDs, copy frames to appropriate acquisition l1a frames directory.
        # Also, attach stream files to data collection object in DB and add frames as well
        frames = [os.path.basename(file) for file in glob.glob(os.path.join(tmp_frames_dir, "*"))]
        frames.sort()

        dcids = set([frame.split("_")[0] for frame in frames])
        logger.debug(f"Found frames {frames} and dcids {dcids}")

        # For each DCID, copy the frames to its dcid-specific frames folder.  Keep track of all output paths.
        dcid_frames_map = {}
        output_frame_paths = []
        for dcid in dcids:
            # Get the earliest start_time and latest start_time from this group of frames
            tmp_dcid_frame_paths = glob.glob(os.path.join(tmp_frames_dir, dcid + "*"))
            start_time_strs = [os.path.basename(p).split("_")[1] for p in tmp_dcid_frame_paths]
            start_time_strs.sort()
            start_time = datetime.datetime.strptime(start_time_strs[0], "%Y%m%dt%H%M%S")
            stop_time = datetime.datetime.strptime(start_time_strs[-1], "%Y%m%dt%H%M%S")

            dc_lookup = dm.find_data_collection_by_id(dcid)
            if dc_lookup is None:
                # Insert DCID in database if it doesn't exist
                dc_meta = {
                    "dcid": dcid,
                    "build_num": wm.config["build_num"],
                    "processing_version": wm.config["processing_version"],
                    "start_time": start_time,
                    "stop_time": stop_time
                }
                dm.insert_data_collection(dc_meta)
                logger.debug(f"Inserted data collection in DB with {dc_meta}")
            elif dc_lookup is not None and "start_time" not in dc_lookup:
                # If only the planning product has been inserted, then add start_time
                dc_meta = {
                    "start_time": start_time,
                    "stop_time": stop_time
                }
                dm.update_data_collection_metadata(dcid, dc_meta)
                logger.debug(f"Updated data collection in DB with {dc_meta}")

            # Now get workflow manager again containing data collection
            wm = WorkflowManager(config_path=self.config_path, dcid=dcid)
            dc = wm.data_collection

            # Check start and stop times to see if this group of frames has an earlier or later one
            if dc.start_time < start_time:
                start_time = dc.start_time
            if dc.stop_time > stop_time:
                stop_time = dc.stop_time

            # Copy the frames
            dcid_frame_paths = []
            for path in tmp_dcid_frame_paths:
                dcid_frame_name = os.path.basename(path)
                dcid_frame_path = os.path.join(dc.frames_dir, dcid_frame_name)
                if not os.path.exists(dcid_frame_path):
                    wm.copy(path, dcid_frame_path)
                dcid_frame_paths.append(dcid_frame_path)

            # Create a symlink from the stream l1a dir to the dcid frames dir
            dcid_frame_symlink = os.path.join(stream.frames_dir, os.path.basename(dc.frames_dir))
            wm.symlink(dc.frames_dir, dcid_frame_symlink)

            # Create a symlink from the dcid/by_date dir structure to the dcid/by_dcid dir structure
            by_dcid_date_dir = os.path.join(dc.by_date_dir, start_time.strftime("%Y%m%d"))
            date_to_dcid_symlink = os.path.join(by_dcid_date_dir, f'{start_time.strftime("%Y%m%dt%H%M%S")}_{dcid}')
            wm.symlink(dc.dcid_dir, date_to_dcid_symlink)

            # Add frame paths to acquisition metadata
            if "frames" in dc.metadata["products"]["l1a"] and dc.metadata["products"]["l1a"]["frames"] is not None:
                for path in dcid_frame_paths:
                    if path not in dc.metadata["products"]["l1a"]["frames"]:
                        dc.metadata["products"]["l1a"]["frames"] += [path]
            else:
                dc.metadata["products"]["l1a"]["frames"] = dcid_frame_paths
            dc.metadata["products"]["l1a"]["frames"].sort()

            dm.update_data_collection_metadata(
                dcid,
                {
                    "start_time": start_time,
                    "stop_time": stop_time,
                    "products.l1a.frames": dc.metadata["products"]["l1a"]["frames"]
                })

            # Add stream file to data collection metadata in DB so there is a link back to CCSDS packet stream for each
            # acquisition. There may be multiple stream files that contribute frames to a given acquisition
            if "associated_ccsds" in dc.metadata and dc.metadata["associated_ccsds"] is not None:
                if stream.ccsds_path not in dc.metadata["associated_ccsds"]:
                    dc.metadata["associated_ccsds"].append(stream.ccsds_path)
            else:
                dc.metadata["associated_ccsds"] = [stream.ccsds_path]
            dm.update_data_collection_metadata(dcid, {"associated_ccsds": dc.metadata["associated_ccsds"]})

            # Append frames to include in stream metadata
            dcid_frame_paths.sort()
            dcid_frames_map.update(
                {
                    dcid: {
                        "dcid_frame_paths": dcid_frame_paths,
                        "created": datetime.datetime.now(tz=datetime.timezone.utc)
                    }
                }
            )

            # Keep track of all output paths for log entry
            output_frame_paths += dcid_frame_paths

        dm.update_stream_metadata(stream.ccsds_name, {"products.l1a.data_collections": dcid_frames_map})

        # Copy log file and report file into the stream's l1a directory
        sdp_log_name = stream.ccsds_name.replace("l0_ccsds", "l1a_frames").replace(".bin", "_pge.log")
        sdp_log_path = os.path.join(stream.l1a_dir, sdp_log_name)
        sdp_report_path = sdp_log_path.replace("_pge.log", "_report.txt")
        wm.copy(tmp_log_path, sdp_log_path)
        wm.copy(tmp_report_path, sdp_report_path)

        doc_version = "EMIT IOS SDS ICD JPL-D 104239, Initial"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_frame_paths": output_frame_paths
            }
        }
        dm.insert_stream_log_entry(stream.ccsds_name, log_entry)


class L1AReassembleRaw(SlurmJobTask):
    """
    Decompresses science frames and assembles them into time-ordered acquisitions
    :returns: Uncompressed raw acquisitions in binary cube format (ENVI compatible)
    """

    config_path = luigi.Parameter()
    dcid = luigi.Parameter()
    ignore_missing_frames = luigi.BoolParameter(default=False)
    level = luigi.Parameter()
    partition = luigi.Parameter()
    acq_chunksize = luigi.IntParameter(default=1280)
    test_mode = luigi.BoolParameter(default=False)

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):
        # This task requires a complete set of frames
        logger.debug(f"{self.task_family} requires: {self.dcid}")

        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.dcid}")
        wm = WorkflowManager(config_path=self.config_path, dcid=self.dcid)
        return DataCollectionTarget(data_collection=wm.data_collection, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} run: {self.dcid}")

        wm = WorkflowManager(config_path=self.config_path, dcid=self.dcid)
        dc = wm.data_collection
        dm = wm.database_manager

        # Check for missing frames before proceeding. Override with --ignore_missing_frames arg
        if self.ignore_missing_frames is False and dc.has_complete_set_of_frames() is False:
            raise RuntimeError(f"Unable to run {self.task_family} on {self.dcid} due to missing frames in "
                               f"{dc.frames_dir}")

        # If not in test mode raise runtime error if we are missing orbit, scene, or submode.  These are needed
        # for creating the output acquisition filenames
        if not self.test_mode:
            if "orbit" not in dc.metadata or "scene" not in dc.metadata or "submode" not in dc.metadata:
                raise RuntimeError(f"Attempting to create acquisitions without orbit, scene, or submode! "
                                   f"It appears that there was no planning product for DCID {self.dcid}")

        pge = wm.pges["emit-sds-l1a"]
        reassemble_raw_pge = os.path.join(pge.repo_dir, "reassemble_raw_cube.py")
        flex_pge = wm.pges["EMIT_FLEX_codec"]
        flex_codec_exe = os.path.join(flex_pge.repo_dir, "flexcodec")
        constants_path = wm.config["decompression_constants_path"]
        init_data_path = wm.config["decompression_init_data_path"]
        tmp_log_path = os.path.join(self.local_tmp_dir, "reassemble_raw_pge.log")
        input_files = {
            "frames_dir": dc.frames_dir,
            "flexcodec_exe_path": flex_codec_exe,
            "constants_path": constants_path,
            "init_data_path": init_data_path
        }
        cmd = ["python", reassemble_raw_pge, dc.frames_dir,
               "--flexcodec_exe", flex_codec_exe,
               "--constants_path", constants_path,
               "--init_data_path", init_data_path,
               "--work_dir", self.local_tmp_dir,
               "--level", self.level,
               "--log_path", tmp_log_path,
               "--chunksize", str(self.acq_chunksize)]
        if self.test_mode:
            cmd.append("--test_mode")
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy decompressed frames to /store
        tmp_image_dir = os.path.join(self.local_tmp_dir, "image")
        tmp_decomp_frame_paths = glob.glob(os.path.join(tmp_image_dir, "*.decomp"))
        for path in tmp_decomp_frame_paths:
            wm.copy(path, os.path.join(dc.decomp_dir, os.path.basename(path)))

        # Find unique acquisitions in tmp output folder
        acquisition_files = [os.path.basename(file) for file in glob.glob(os.path.join(tmp_image_dir, "emit*"))]
        acquisition_files.sort()
        acq_ids = list(set([a.split("_")[0] for a in acquisition_files]))

        # Update DB with associated acquisitions
        if "associated_acquisitions" in dc.metadata and dc.metadata["associated_acquisitions"] is not None:
            for a in acq_ids:
                if a not in dc.metadata["associated_acquisitions"]:
                    dc.metadata["associated_acquisitions"].append(a)
        else:
            dc.metadata["associated_acquisitions"] = acq_ids
        dc.metadata["associated_acquisitions"].sort()
        dc_meta = {"associated_acquisitions": dc.metadata["associated_acquisitions"]}
        dm.update_data_collection_metadata(self.dcid, dc_meta)

        # Loop through acquisition ids and process
        output_paths = {
            "l1a_raw_img_paths": [],
            "l1a_raw_hdr_paths": [],
            "l1a_raw_line_timestamps": [],
            "l1a_rawqa_txt_paths": []
        }
        acq_product_map = {}
        for acq_id in acq_ids:
            orbit = dc.metadata["orbit"] if "orbit" in dc.metadata else "00000"
            scene = dc.metadata["scene"] if "scene" in dc.metadata else"000"
            submode = dc.metadata["submode"] if "submode" in dc.metadata else "science"

            # Get start/stop times from reassembly report
            tmp_report_path = os.path.join(tmp_image_dir, f"{acq_id}_report.txt")
            start_time = None
            stop_time = None
            start_index = None
            stop_index = None
            with open(tmp_report_path, "r") as f:
                for line in f.readlines():
                    if "Start time" in line:
                        start_time = datetime.datetime.strptime(
                            line.rstrip("\n").split(": ")[1], "%Y-%m-%d %H:%M:%S.%f")
                    if "Stop time" in line:
                        stop_time = datetime.datetime.strptime(
                            line.rstrip("\n").split(": ")[1], "%Y-%m-%d %H:%M:%S.%f")
                    if "First frame number in acquisition" in line:
                        start_index = int(line.rstrip("\n").split(": ")[1])
                    if "Last frame number in acquisition" in line:
                        stop_index = int(line.rstrip("\n").split(": ")[1])

            # TODO: Check valid date?
            if start_time is None or stop_time is None:
                raise RuntimeError("Could not find start or stop time for acquisition!")

            # Get list of paths to frames for this acquisition
            acq_frame_nums = [str(i).zfill(5) for i in list(range(start_index, stop_index + 1))]
            frame_paths = glob.glob(os.path.join(dc.frames_dir, "*"))
            decomp_frame_paths = glob.glob(os.path.join(dc.decomp_dir, "*.decomp"))
            acq_frame_paths = [p for p in frame_paths if os.path.basename(p).split("_")[2] in acq_frame_nums]
            acq_frame_paths.sort()
            acq_decomp_frame_paths = [p for p in decomp_frame_paths if os.path.basename(p).split("_")[2] in
                                      acq_frame_nums]
            acq_decomp_frame_paths.sort()

            # Define acquisition metadata
            acq_meta = {
                "acquisition_id": acq_id,
                "build_num": wm.config["build_num"],
                "processing_version": wm.config["processing_version"],
                "start_time": start_time,
                "stop_time": stop_time,
                "orbit": orbit,
                "scene": scene,
                "submode": submode.lower(),
                "associated_dcid": self.dcid
            }

            # Insert acquisition into DB
            if not dm.find_acquisition_by_id(acq_id):
                dm.insert_acquisition(acq_meta)
                logger.debug(f"Inserted acquisition in DB with {acq_meta}")

            wm = WorkflowManager(config_path=self.config_path, acquisition_id=acq_id)
            acq = wm.acquisition

            # Copy raw file, report file, timestamps file, and log back to l1a data dir
            tmp_raw_path = os.path.join(tmp_image_dir, acq.acquisition_id + "_raw.img")
            tmp_raw_hdr_path = tmp_raw_path.replace(".img", ".hdr")
            wm.copy(tmp_raw_path, acq.raw_img_path)
            wm.copy(tmp_raw_hdr_path, acq.raw_hdr_path)
            wm.copy(tmp_log_path, acq.raw_img_path.replace(".img", "_pge.log"))
            report_path = acq.raw_img_path.replace(".img", "_report.txt")
            wm.copy(tmp_report_path, report_path)
            tmp_line_timestamps_path = tmp_raw_path.replace("_raw.img", "_line_timestamps.txt")
            line_timestamps_path = acq.raw_img_path.replace(".img", "_line_timestamps.txt")
            wm.copy(tmp_line_timestamps_path, line_timestamps_path)

            # Create symlinks from the acquisition to the specific frames that are associated with it.
            for p in acq_frame_paths:
                frame_path_symlink = os.path.join(acq.frames_dir, os.path.basename(p))
                wm.symlink(p, frame_path_symlink)
            for p in acq_decomp_frame_paths:
                decomp_frame_path_symlink = os.path.join(acq.decomp_dir, os.path.basename(p))
                wm.symlink(p, decomp_frame_path_symlink)

            # Create rawqa report file based on CCSDS depacketization report(s) and reassembly report
            rawqa_file = open(acq.rawqa_txt_path, "w")

            # Get depacketization report from associated CCSDS files
            for path in dc.associated_ccsds:
                depacket_report_path = path.replace(
                    "l0", "l1a").replace("ccsds", "frames").replace(".bin", "_report.txt")
                if os.path.exists(depacket_report_path):
                    with open(depacket_report_path, "r") as f:
                        rawqa_file.write("===========\n")
                        rawqa_file.write("SOURCE FILE\n")
                        rawqa_file.write("===========\n")
                        rawqa_file.write(f"{depacket_report_path}\n\n")
                        rawqa_file.write(f.read() + "\n\n")
                else:
                    logger.warning(f"Unable to find depacketization report located at {depacket_report_path}")

            # Get reassembly report
            if os.path.exists(report_path):
                with open(tmp_report_path, "r") as f:
                    rawqa_file.write("===========\n")
                    rawqa_file.write("SOURCE FILE\n")
                    rawqa_file.write("===========\n")
                    rawqa_file.write(f"{report_path}\n\n")
                    rawqa_file.write(f.read())
            else:
                logger.warning(f"Unable to find reassembly report located at {report_path}")

            rawqa_file.close()
            wm.change_group_ownership(acq.rawqa_txt_path)

            # Update hdr files
            input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
            doc_version = "EMIT SDS L1A JPL-D 104186, Initial"
            hdr = envi.read_envi_header(acq.raw_hdr_path)
            hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit pge name"] = pge.repo_url
            hdr["emit pge version"] = pge.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd)
            hdr["emit software build version"] = wm.config["build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(acq.raw_img_path), tz=datetime.timezone.utc)
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit data product version"] = wm.config["processing_version"]
            envi.write_envi_header(acq.raw_hdr_path, hdr)

            # Update products with frames and decompressed frames:
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1a.frames": acq_frame_paths})
            dm.update_acquisition_metadata(acq.acquisition_id,
                                           {"products.l1a.decompressed_frames": acq_decomp_frame_paths})

            # Update raw product dictionary
            product_dict_raw = {
                "img_path": acq.raw_img_path,
                "hdr_path": acq.raw_hdr_path,
                "created": creation_time,
                "dimensions": {
                    "lines": hdr["lines"],
                    "samples": hdr["samples"],
                    "bands": hdr["bands"]
                }
            }
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1a.raw": product_dict_raw})

            # Update line timestamps product dictionary
            product_dict_line_timestamps = {
                "txt_path": line_timestamps_path,
                "created": datetime.datetime.fromtimestamp(os.path.getmtime(line_timestamps_path),
                                                           tz=datetime.timezone.utc)
            }
            dm.update_acquisition_metadata(acq.acquisition_id,
                                           {"products.l1a.raw_line_timestamps": product_dict_line_timestamps})

            # Update rawqa product dictionary
            product_dict_rawqa = {
                "txt_path": acq.rawqa_txt_path,
                "created": datetime.datetime.fromtimestamp(os.path.getmtime(acq.rawqa_txt_path),
                                                           tz=datetime.timezone.utc)
            }
            dm.update_acquisition_metadata(acq.acquisition_id, {"products.l1a.rawqa": product_dict_rawqa})

            # Keep track of output paths and acquisition product map for dc update
            output_paths["l1a_raw_img_paths"].append(acq.raw_img_path)
            output_paths["l1a_raw_hdr_paths"].append(acq.raw_hdr_path)
            output_paths["l1a_raw_line_timestamps"].append(line_timestamps_path)
            output_paths["l1a_rawqa_txt_paths"].append(acq.rawqa_txt_path)
            acq_product_map.update({
                acq_id: {
                    "raw": product_dict_raw,
                    "raw_line_timestamps": product_dict_line_timestamps,
                    "rawqa": product_dict_rawqa
                }
            })

        # Add log entry to DB
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
            "output": output_paths
        }

        dm.insert_data_collection_log_entry(self.dcid, log_entry)

        dm.update_data_collection_metadata(self.dcid, {"products.l1a.acquisitions": acq_product_map})


class L1AFrameReport(SlurmJobTask):
    """
    Runs the ngis_check_frame.py script.
    :returns: A frame report, frame header and line header CSVs for all frames in a data collection
    """

    config_path = luigi.Parameter()
    dcid = luigi.Parameter()
    ignore_missing_frames = luigi.BoolParameter(default=False)
    level = luigi.Parameter()
    partition = luigi.Parameter()
    acq_chunksize = luigi.IntParameter(default=1280)
    test_mode = luigi.BoolParameter(default=False)

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.dcid}")
        return L1AReassembleRaw(config_path=self.config_path, dcid=self.dcid, level=self.level,
                                partition=self.partition, ignore_missing_frames=self.ignore_missing_frames,
                                acq_chunksize=self.acq_chunksize, test_mode=self.test_mode)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.dcid}")
        wm = WorkflowManager(config_path=self.config_path, dcid=self.dcid)
        return DataCollectionTarget(data_collection=wm.data_collection, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} run: {self.dcid}")

        wm = WorkflowManager(config_path=self.config_path, dcid=self.dcid)
        dc = wm.data_collection
        pge = wm.pges["NGIS_Check_Line_Frame"]

        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        wm.makedirs(tmp_output_dir)

        # Copy decompressed frames to local tmp
        input_decomp_frame_paths = glob.glob(os.path.join(dc.decomp_dir, "*.decomp"))
        for decomp_frame_path in input_decomp_frame_paths:
            tmp_decomp_frame_path = os.path.join(tmp_output_dir, os.path.basename(decomp_frame_path))
            wm.copy(decomp_frame_path, tmp_decomp_frame_path)

        ngis_check_list_exe = os.path.join(pge.repo_dir, "python", "ngis_check_list.py")
        cmd = ["python", ngis_check_list_exe, tmp_output_dir]
        pge.run(cmd, tmp_dir=self.tmp_dir)

        output_files = glob.glob(os.path.join(tmp_output_dir, "*.csv"))
        output_files += glob.glob(os.path.join(tmp_output_dir, "*.txt"))
        for file in output_files:
            if "allframesparsed.csv" in file or "allframesreport.txt" in file:
                wm.copy(file, os.path.join(dc.decomp_dir, dc.dcid + "_" + os.path.basename(file)))
            else:
                wm.copy(file, os.path.join(dc.decomp_dir, os.path.basename(file)))

        # PGE writes metadata to db
        dm = wm.database_manager

        all_frames_report = glob.glob(os.path.join(dc.decomp_dir, "*allframesreport.txt"))[0]
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(all_frames_report), tz=datetime.timezone.utc)

        # Update frames_report product dictionary
        product_dict = {
            "txt_path": all_frames_report,
            "created": creation_time
        }
        dm.update_data_collection_metadata(dc.dcid, {"products.l1a.frames_report": product_dict})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {"decomp_dir": dc.decomp_dir},
            "pge_run_command": " ".join(cmd),
            "documentation_version": "N/A",
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_all_frames_report_path": all_frames_report
            }
        }

        dm.insert_data_collection_log_entry(dc.dcid, log_entry)


class L1AReformatEDP(SlurmJobTask):
    """
    Creates reformatted engineering data product from CCSDS packet stream
    :returns: Reformatted engineering data product
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    miss_pkt_thresh = luigi.FloatParameter()

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.stream_path}")
        return L0StripHOSC(config_path=self.config_path, stream_path=self.stream_path, level=self.level,
                           partition=self.partition, miss_pkt_thresh=self.miss_pkt_thresh)

    def output(self):

        logger.debug(f"{self.task_family} output: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        stream = wm.stream
        pge = wm.pges["emit-sds-l1a"]

        # Build command and run
        sds_l1a_eng_exe = os.path.join(pge.repo_dir, "run_l1a_eng.sh")
        ios_l1_edp_exe = os.path.join(wm.pges["emit-ios"].repo_dir, "emit", "bin", "emit_l1_edp.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        tmp_log_dir = os.path.join(self.local_tmp_dir, "logs")

        tmp_log = os.path.join(tmp_output_dir, stream.hosc_name + ".log")

        cmd = [sds_l1a_eng_exe, stream.ccsds_path, self.local_tmp_dir, ios_l1_edp_exe]
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
        wm.copy(tmp_edp_path, edp_path)
        wm.copy(tmp_report_path, report_path)

        # Copy and rename log file
        l1a_pge_log_path = edp_path.replace(".csv", "_pge.log")
        for file in glob.glob(os.path.join(tmp_log_dir, "*")):
            wm.copy(file, l1a_pge_log_path)

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
                "l1a_edp_path": edp_path
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)


class L1AReformatBAD(SlurmJobTask):
    """
    Creates reformatted NetCDF BAD file with attitude and ephemeris from input BAD STO file
    :returns: Reformatted BAD data product
    """

    config_path = luigi.Parameter()
    orbit_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    memory = 30000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.stream_path}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, stream_path=self.stream_path)
        return StreamTarget(stream=wm.stream, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.stream_path}")
        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        orbit = wm.orbit
        pge = wm.pges["emit-sds-l1a"]

        # Find all BAD STO files in an orbit
        # TODO: Need to "ingest" sto files and copy them somewhere

        # Build command and run
        sds_l1a_eng_exe = os.path.join(pge.repo_dir, "run_l1a_eng.sh")
        ios_l1_edp_exe = os.path.join(wm.pges["emit-ios"].repo_dir, "emit", "bin", "emit_l1_edp.py")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        tmp_log_dir = os.path.join(self.local_tmp_dir, "logs")

        tmp_log = os.path.join(tmp_output_dir, stream.hosc_name + ".log")

        cmd = [sds_l1a_eng_exe, stream.ccsds_path, self.local_tmp_dir, ios_l1_edp_exe]
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
        wm.copy(tmp_edp_path, edp_path)
        wm.copy(tmp_report_path, report_path)

        # Copy and rename log file
        l1a_pge_log_path = edp_path.replace(".csv", "_pge.log")
        for file in glob.glob(os.path.join(tmp_log_dir, "*")):
            wm.copy(file, l1a_pge_log_path)

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
                "l1a_edp_path": edp_path
            }
        }
        dm.insert_stream_log_entry(stream.hosc_name, log_entry)