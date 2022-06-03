"""
This code contains tasks for executing EMIT Level 1A PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import json
import logging
import os

import luigi
import spectral.io.envi as envi


from emit_main.workflow.output_targets import StreamTarget, DataCollectionTarget, OrbitTarget, AcquisitionTarget
from emit_main.workflow.l0_tasks import L0StripHOSC
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_utils import daac_converter

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

    memory = 90000
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
        prev_streams = dm.find_streams_touching_date_range("1675", "stop_time",
                                                           stream.start_time - datetime.timedelta(seconds=1),
                                                           stream.start_time + datetime.timedelta(minutes=1),
                                                           sort=-1)

        prev_stream_path = None
        if prev_streams is not None and len(prev_streams) > 0:
            # First iterate through and find the first previous stream file that starts before the current stream file
            index = None
            for i in range(len(prev_streams)):
                if prev_streams[i]["start_time"] < stream.start_time:
                    index = i
                    break

            # If we found one, then try to get the previous stream path
            if index is not None:
                try:
                    prev_stream_path = prev_streams[index]["products"]["l0"]["ccsds_path"]
                except KeyError:
                    wm.print(__name__, f"Could not find a previous stream path for {stream.ccsds_path} in DB.")
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
            else:
                raise RuntimeError(f"While processing {stream.ccsds_name}, found previous stream file at "
                                   f"{prev_stream_path}, but no report at {prev_stream_report}. Unable to proceed.")

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
                    "stop_time": stop_time,
                    "frames_status": ""
                }
                dm.insert_data_collection(dc_meta)
                logger.debug(f"Inserted data collection in DB with {dc_meta}")
            elif dc_lookup is not None and "start_time" not in dc_lookup:
                # If only the planning product has been inserted, then add some timing info
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
            wm.makedirs(by_dcid_date_dir)
            date_to_dcid_symlink = os.path.join(by_dcid_date_dir, f'{start_time.strftime("%Y%m%dt%H%M%S")}_{dcid}')
            wm.symlink(dc.dcid_dir, date_to_dcid_symlink)

            # Add frame paths to data collection metadata
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
                    "frames_last_modified": datetime.datetime.now(tz=datetime.timezone.utc),
                    "products.l1a.frames": dc.metadata["products"]["l1a"]["frames"]
                })

            # Check if data collection has complete set of frames
            if dc.has_complete_set_of_frames():
                dm.update_data_collection_metadata(dcid, {"frames_status": "complete"})
            else:
                dm.update_data_collection_metadata(dcid, {"frames_status": "incomplete"})

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

    memory = 90000
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

        pge_reassemble = wm.pges["emit-sds-l1a"]
        reassemble_raw_pge = os.path.join(pge_reassemble.repo_dir, "reassemble_raw_cube.py")
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
        pge_reassemble.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Now run the compute line stats script
        pge_line_stats = wm.pges["NGIS_Check_Line_Frame"]
        compute_line_stats_exe = os.path.join(pge_line_stats.repo_dir, "python", "compute_line_stats.py")
        tmp_image_dir = os.path.join(self.local_tmp_dir, "image")
        tmp_decomp_no_header_paths = glob.glob(os.path.join(tmp_image_dir, "*.decomp_no_header"))
        tmp_decomp_no_header_paths = [p for p in tmp_decomp_no_header_paths if os.path.getsize(p) > 0]
        tmp_decomp_no_header_paths.sort()
        tmp_no_header_list = os.path.join(self.local_tmp_dir, "no_header_list.txt")
        with open(tmp_no_header_list, "w") as f:
            f.write("\n".join(tmp_decomp_no_header_paths))
        tmp_line_stats_path = os.path.join(self.local_tmp_dir, "line_stats.txt")
        cmd = ["python", compute_line_stats_exe, tmp_no_header_list, ">", tmp_line_stats_path]
        pge_line_stats.run(cmd, tmp_dir=self.tmp_dir)

        # Copy decompressed frames to /store
        tmp_decomp_frame_paths = glob.glob(os.path.join(tmp_image_dir, "*.decomp"))
        for path in tmp_decomp_frame_paths:
            wm.copy(path, os.path.join(dc.decomp_dir, os.path.basename(path)))

        # If in test_mode, also copy the decompressed frames with no header to /store
        if self.test_mode:
            for path in tmp_decomp_no_header_paths:
                wm.copy(path, os.path.join(dc.decomp_dir, os.path.basename(path)))

        # Copy line stats log to /store
        line_stats_path = os.path.join(dc.decomp_dir, f"{dc.dcid}_{os.path.basename(tmp_line_stats_path)}")
        wm.copy(tmp_line_stats_path, line_stats_path)

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
            "l1a_line_stats_path": line_stats_path,
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
            num_valid_lines = None
            instrument_mode = None
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
                    if "Number of lines with valid data" in line:
                        num_valid_lines = int(line.rstrip("\n").split(": ")[1])
                    if "Instrument mode:" in line:
                        instrument_mode = line.rstrip("\n").split(": ")[1]

            # TODO: Check valid date?
            if start_time is None or stop_time is None:
                raise RuntimeError("Could not find start or stop time for acquisition!")

            # Get list of paths to frames for this acquisition
            acq_frame_nums = [str(i).zfill(5) for i in list(range(start_index, stop_index + 1))]
            frame_paths = glob.glob(os.path.join(dc.frames_dir, "*[!txt]"))
            decomp_frame_paths = glob.glob(os.path.join(dc.decomp_dir, "*.decomp"))
            acq_frame_paths = [p for p in frame_paths if os.path.basename(p).split("_")[2] in acq_frame_nums]
            acq_frame_paths.sort()
            acq_decomp_frame_paths = [p for p in decomp_frame_paths if os.path.basename(p).split("_")[2] in
                                      acq_frame_nums]
            acq_decomp_frame_paths.sort()

            # Define acquisition metadata
            daynight = "Day" if submode.lower() == "science" else "Night"
            acq_meta = {
                "acquisition_id": acq_id,
                "build_num": wm.config["build_num"],
                "processing_version": wm.config["processing_version"],
                "start_time": start_time,
                "stop_time": stop_time,
                "orbit": orbit,
                "scene": scene,
                "submode": submode.lower(),
                "daynight": daynight,
                "instrument_mode": instrument_mode,
                "num_valid_lines": num_valid_lines,
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
                    wm.print(__name__, f"Unable to find depacketization report located at {depacket_report_path}")

            # Get reassembly report
            if os.path.exists(report_path):
                with open(tmp_report_path, "r") as f:
                    rawqa_file.write("===========\n")
                    rawqa_file.write("SOURCE FILE\n")
                    rawqa_file.write("===========\n")
                    rawqa_file.write(f"{report_path}\n\n")
                    rawqa_file.write(f.read())
            else:
                wm.print(__name__, f"Unable to find reassembly report located at {report_path}")

            rawqa_file.close()
            wm.change_group_ownership(acq.rawqa_txt_path)

            # Create raw waterfall
            raw_waterfall_exe = os.path.join(pge_reassemble.repo_dir, "util", "raw_waterfall.py")
            waterfall_cmd = ["python", raw_waterfall_exe, tmp_raw_path]
            pge_reassemble.run(waterfall_cmd, tmp_dir=self.tmp_dir, env=env)
            wm.copy(tmp_raw_path.replace(".img", "_waterfall.png"), acq.raw_img_path.replace(".img", "_waterfall.png"))

            # Update hdr files
            input_files_arr = ["{}={}".format(key, value) for key, value in input_files.items()]
            doc_version = "EMIT SDS L1A JPL-D 104186, Initial"
            hdr = envi.read_envi_header(acq.raw_hdr_path)
            hdr["emit acquisition start time"] = acq.start_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit acquisition stop time"] = acq.stop_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit pge name"] = pge_reassemble.repo_url
            hdr["emit pge version"] = pge_reassemble.version_tag
            hdr["emit pge input files"] = input_files_arr
            hdr["emit pge run command"] = " ".join(cmd)
            hdr["emit software build version"] = wm.config["extended_build_num"]
            hdr["emit documentation version"] = doc_version
            creation_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(acq.raw_img_path), tz=datetime.timezone.utc)
            hdr["emit data product creation time"] = creation_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            hdr["emit data product version"] = wm.config["processing_version"]
            hdr["emit acquisition daynight"] = acq.daynight
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

            # Add symlinks to acquisitions for easy access
            wm.symlink(acq.acquisition_id_dir, os.path.join(dc.acquisitions_dir, acq_id))

        # Update product dictionary with line stats and acquisitions
        product_dict_line_stats = {
            "txt_path": line_stats_path,
            "created": datetime.datetime.fromtimestamp(os.path.getmtime(line_stats_path),
                                                       tz=datetime.timezone.utc)
        }
        dm.update_data_collection_metadata(self.dcid, {"products.l1a.line_stats": product_dict_line_stats})
        dm.update_data_collection_metadata(self.dcid, {"products.l1a.acquisitions": acq_product_map})

        # Add log entry to DB
        log_entry = {
            "task": self.task_family,
            "pge_name": pge_reassemble.repo_url,
            "pge_version": pge_reassemble.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": output_paths
        }

        dm.insert_data_collection_log_entry(self.dcid, log_entry)


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

        # Create and run ngis_check_list command
        ngis_check_list_exe = os.path.join(pge.repo_dir, "python", "ngis_check_list.py")
        cmd_check_list = ["python", ngis_check_list_exe, tmp_output_dir]
        pge.run(cmd_check_list, cwd=tmp_output_dir, tmp_dir=self.tmp_dir)

        # Create and run parse_hdr command
        ngis_parse_hdr_exe = os.path.join(pge.repo_dir, "python", "ngis_parse_hdr_list.py")
        tmp_parse_hdr_dir = os.path.join(self.local_tmp_dir, "parse_hdr_output")
        wm.makedirs(tmp_parse_hdr_dir)
        cmd_parse_hdr = ["python", ngis_parse_hdr_exe, dc.frames_dir, tmp_parse_hdr_dir]
        pge.run(cmd_parse_hdr, tmp_dir=self.tmp_dir)

        # Copy parse hdr files back to frames directory
        tmp_hdr_text_paths = glob.glob(os.path.join(tmp_parse_hdr_dir, "*"))
        for path in tmp_hdr_text_paths:
            wm.copy(path, os.path.join(dc.frames_dir, os.path.basename(path)))

        # Copy check_list files back to decomp directory
        output_files = glob.glob(os.path.join(tmp_output_dir, "*.csv"))
        output_files += glob.glob(os.path.join(tmp_output_dir, "*.txt"))
        output_files += glob.glob(os.path.join(tmp_output_dir, "*.log"))
        for file in output_files:
            if "allframesparsed.csv" in file or "allframesreport.txt" in file or "ALL_LINEPARSED.csv" in file \
                    or "line_header_check.log" in file:
                wm.copy(file, os.path.join(dc.decomp_dir, dc.dcid + "_" + os.path.basename(file)))
            else:
                wm.copy(file, os.path.join(dc.decomp_dir, os.path.basename(file)))

        # PGE writes metadata to db
        dm = wm.database_manager

        all_frames_report = glob.glob(os.path.join(dc.decomp_dir, "*allframesreport.txt"))[0]
        afr_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(all_frames_report),
                                                            tz=datetime.timezone.utc)
        line_header_check_log = glob.glob(os.path.join(dc.decomp_dir, "*line_header_check.log"))[0]
        lhc_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(line_header_check_log),
                                                            tz=datetime.timezone.utc)

        # Update frames_report product dictionary
        afr_product_dict = {
            "txt_path": all_frames_report,
            "created": afr_creation_time
        }
        dm.update_data_collection_metadata(dc.dcid, {"products.l1a.frames_report": afr_product_dict})

        # Update line header check report product dictionary
        lhc_product_dict = {
            "txt_path": line_header_check_log,
            "created": lhc_creation_time
        }
        dm.update_data_collection_metadata(dc.dcid, {"products.l1a.line_header_check_log": lhc_product_dict})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {"decomp_dir": dc.decomp_dir},
            "pge_run_command": " ".join(cmd_check_list),
            "documentation_version": "N/A",
            "product_creation_time": afr_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_all_frames_report_path": all_frames_report,
                "l1a_line_header_check_log_path": line_header_check_log
            }
        }

        dm.insert_data_collection_log_entry(dc.dcid, log_entry)


class L1ADeliver(SlurmJobTask):
    """
    Stages Raw and UMM-G files and submits notification to DAAC interface
    :returns: Staged L1A files
    """

    config_path = luigi.Parameter()
    acquisition_id = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()

    task_namespace = "emit"
    n_cores = 1
    memory = 90000

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.acquisition_id}")
        # TODO: Add dependency for reassemble by looking up DCID
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.acquisition_id}")
        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        return AcquisitionTarget(acquisition=wm.acquisition, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.acquisition_id}")

        wm = WorkflowManager(config_path=self.config_path, acquisition_id=self.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-main"]

        # Get local SDS names
        ummg_path = acq.raw_img_path.replace(".img", ".cmr.json")

        # Create local/tmp daac names and paths
        daac_raw_name = f"{acq.raw_granule_ur}.img"
        daac_raw_hdr_name = f"{acq.raw_granule_ur}.hdr"
        daac_browse_name = f"{acq.raw_granule_ur}.png"
        daac_ummg_name = f"{acq.raw_granule_ur}.cmr.json"
        daac_raw_path = os.path.join(self.tmp_dir, daac_raw_name)
        daac_raw_hdr_path = os.path.join(self.tmp_dir, daac_raw_hdr_name)
        daac_browse_path = os.path.join(self.tmp_dir, daac_browse_name)
        daac_ummg_path = os.path.join(self.tmp_dir, daac_ummg_name)

        # Copy files to tmp dir and rename
        wm.copy(acq.raw_img_path, daac_raw_path)
        wm.copy(acq.raw_hdr_path, daac_raw_hdr_path)
        wm.copy(acq.rdn_png_path, daac_browse_path)

        # First create the UMM-G file
        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(acq.raw_img_path), tz=datetime.timezone.utc)
        l1a_pge = wm.pges["emit-sds-l1a"]
        ummg = daac_converter.initialize_ummg(acq.raw_granule_ur, creation_time, "EMITL1ARAW", acq.collection_version,
                                              wm.config["extended_build_num"], l1a_pge.repo_name, l1a_pge.version_tag, 
                                              cloud_fraction = acq.cloud_fraction)
        daynight = "Day" if acq.submode == "science" else "Night"
        ummg = daac_converter.add_data_files_ummg(
            ummg,
            [daac_raw_path, daac_raw_hdr_path, daac_browse_path],
            daynight,
            ["BINARY", "ASCII", "PNG"])
        ummg = daac_converter.add_related_url(ummg, l1a_pge.repo_url, "DOWNLOAD SOFTWARE")
        # TODO: replace w/ database read or read from L1B Geolocate PGE
        tmp_boundary_points_list = [[-118.53, 35.85], [-118.53, 35.659], [-118.397, 35.659], [-118.397, 35.85]]
        ummg = daac_converter.add_boundary_ummg(ummg, tmp_boundary_points_list)
        daac_converter.dump_json(ummg, ummg_path)
        wm.change_group_ownership(ummg_path)

        # Copy ummg file to tmp dir and rename
        wm.copy(ummg_path, daac_ummg_path)

        # Copy files to staging server
        partial_dir_arg = f"--partial-dir={acq.daac_partial_dir}"
        log_file_arg = f"--log-file={os.path.join(self.tmp_dir, 'rsync.log')}"
        target = f"{wm.config['daac_server_internal']}:{acq.daac_staging_dir}/"
        group = f"emit-{wm.config['environment']}" if wm.config["environment"] in ("test", "ops") else "emit-dev"
        # This command only makes the directory and changes ownership if the directory doesn't exist
        cmd_make_target = ["ssh", wm.config["daac_server_internal"], "\"if", "[", "!", "-d",
                           f"'{acq.daac_staging_dir}'", "];", "then", "mkdir", f"{acq.daac_staging_dir};", "chgrp",
                           group, f"{acq.daac_staging_dir};", "fi\""]
        pge.run(cmd_make_target, tmp_dir=self.tmp_dir)

        for path in (daac_raw_path, daac_raw_hdr_path, daac_browse_path, daac_ummg_path):
            cmd_rsync = ["rsync", "-azv", partial_dir_arg, log_file_arg, path, target]
            pge.run(cmd_rsync, tmp_dir=self.tmp_dir)

        # Build notification dictionary
        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        cnm_submission_id = f"{acq.raw_granule_ur}_{utc_now.strftime('%Y%m%dt%H%M%S')}"
        cnm_submission_path = os.path.join(acq.l1a_data_dir, cnm_submission_id + "_cnm.json")
        target_src_map = {
            daac_raw_name: os.path.basename(acq.raw_img_path),
            daac_raw_hdr_name: os.path.basename(acq.raw_hdr_path),
            daac_browse_name: os.path.basename(acq.rdn_png_path),
            daac_ummg_name: os.path.basename(ummg_path)
        }
        notification = {
            "collection": "EMITL1ARAW",
            "provider": wm.config["daac_provider"],
            "identifier": cnm_submission_id,
            "version": wm.config["cnm_version"],
            "product": {
                "name": acq.raw_granule_ur,
                "dataVersion": acq.collection_version,
                "files": [
                    {
                        "name": daac_raw_name,
                        "uri": acq.daac_uri_base + daac_raw_name,
                        "type": "data",
                        "size": os.path.getsize(daac_raw_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_raw_path, "sha512")
                    },
                    {
                        "name": daac_raw_hdr_name,
                        "uri": acq.daac_uri_base + daac_raw_hdr_name,
                        "type": "data",
                        "size": os.path.getsize(daac_raw_hdr_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_raw_hdr_path, "sha512")
                    },
                    {
                        "name": daac_browse_name,
                        "uri": acq.daac_uri_base + daac_browse_name,
                        "type": "browse",
                        "size": os.path.getsize(daac_browse_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_browse_path, "sha512")
                    },
                    {
                        "name": daac_ummg_name,
                        "uri": acq.daac_uri_base + daac_ummg_name,
                        "type": "metadata",
                        "size": os.path.getsize(daac_ummg_path),
                        "checksumType": "sha512",
                        "checksum": daac_converter.calc_checksum(daac_ummg_path, "sha512")
                    }
                ]
            }
        }

        # Write notification submission to file
        with open(cnm_submission_path, "w") as f:
            f.write(json.dumps(notification, indent=4))
        wm.change_group_ownership(cnm_submission_path)

        # Submit notification via AWS SQS
        cmd_aws = [wm.config["aws_cli_exe"], "sqs", "send-message", "--queue-url", wm.config["daac_submission_url"], "--message-body",
                   f"file://{cnm_submission_path}", "--profile", wm.config["aws_profile"]]
        pge.run(cmd_aws, tmp_dir=self.tmp_dir)
        cnm_creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(cnm_submission_path),
                                                            tz=datetime.timezone.utc)

        # Record delivery details in DB for reconciliation report
        dm = wm.database_manager
        for file in notification["product"]["files"]:
            delivery_report = {
                "timestamp": utc_now,
                "extended_build_num": wm.config["extended_build_num"],
                "collection": notification["collection"],
                "collection_version": notification["product"]["dataVersion"],
                "sds_filename": target_src_map[file["name"]],
                "daac_filename": file["name"],
                "uri": file["uri"],
                "type": file["type"],
                "size": file["size"],
                "checksum": file["checksum"],
                "checksum_type": file["checksumType"],
                "submission_id": cnm_submission_id,
                "submission_status": "submitted"
            }
            dm.insert_granule_report(delivery_report)

        # Update db with log entry
        if "raw_daac_submissions" in acq.metadata["products"]["l1a"] and \
                acq.metadata["products"]["l1a"]["raw_daac_submissions"] is not None:
            acq.metadata["products"]["l1a"]["raw_daac_submissions"].append(cnm_submission_path)
        else:
            acq.metadata["products"]["l1a"]["raw_daac_submissions"] = [cnm_submission_path]
        dm.update_acquisition_metadata(
            acq.acquisition_id,
            {"products.l1a.raw_daac_submissions": acq.metadata["products"]["l1a"]["raw_daac_submissions"]})

        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "raw_img_path": acq.raw_img_path,
                "raw_hdr_path": acq.raw_hdr_path,
                "rdn_png_path": acq.rdn_png_path
            },
            "pge_run_command": " ".join(cmd_aws),
            "documentation_version": "TBD",
            "product_creation_time": cnm_creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_raw_ummg_path": ummg_path,
                "l1a_raw_cnm_submission_path": cnm_submission_path
            }
        }

        dm.insert_acquisition_log_entry(self.acquisition_id, log_entry)


class L1AReformatEDP(SlurmJobTask):
    """
    Creates reformatted engineering data products from CCSDS packet stream
    :returns: Reformatted engineering data product
    """

    config_path = luigi.Parameter()
    stream_path = luigi.Parameter()
    level = luigi.Parameter()
    partition = luigi.Parameter()
    miss_pkt_thresh = luigi.FloatParameter()

    memory = 90000
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
        pge = wm.pges["emit-ios"]

        # Find corresponding 1676 stream file
        dm = wm.database_manager
        # TODO: What should this query time be?  Up to 2 hours?
        anc_streams = dm.find_streams_touching_date_range("1676", "start_time",
                                                          stream.start_time - datetime.timedelta(seconds=1),
                                                          stream.start_time + datetime.timedelta(minutes=4)
                                                          )
        anc_stream_path = None
        if anc_streams is not None and len(anc_streams) > 0:
            try:
                anc_stream_path = anc_streams[0]["products"]["l0"]["ccsds_path"]
            except KeyError:
                wm.print(__name__, f"Could not find a ancillary 1676 stream path for {stream.ccsds_path} in DB.")
                raise RuntimeError(f"Could not find a ancillary 1676 stream path for {stream.ccsds_path} in DB.")

        # Build command
        l1_edp_exe = os.path.join(pge.repo_dir, "emit", "bin", "emit_l1_edp.py")
        tmp_input_dir = os.path.join(self.local_tmp_dir, "input")
        tmp_output_dir = os.path.join(self.local_tmp_dir, "output")
        tmp_log_dir = os.path.join(self.local_tmp_dir, "logs")
        wm.makedirs(tmp_input_dir)
        wm.makedirs(tmp_output_dir)
        wm.makedirs(tmp_log_dir)

        # Copy input files to input dir
        input_files = {"1674_ccsds_path": stream.ccsds_path}
        if anc_stream_path is not None:
            input_files["1676_ccsds_path"] = anc_stream_path
        for path in input_files.values():
            wm.copy(path, tmp_input_dir)

        # python ${L1_EDP_EXE} --input-dir=${L1_INPUT} --output-dir=${L1_OUTPUT} --log-dir=${L1_LOGS}
        cmd = ["python", l1_edp_exe,
               f"--input-dir={tmp_input_dir}",
               f"--output-dir={tmp_output_dir}",
               f"--log-dir={tmp_log_dir}"]
        env = os.environ.copy()
        env["AIT_ROOT"] = pge.repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Get tmp edp product and report paths
        tmp_edp_path = glob.glob(os.path.join(tmp_output_dir, "*.csv"))[0]
        tmp_edp_prod_paths = glob.glob(os.path.join(tmp_output_dir, "0x*"))
        tmp_report_path = glob.glob(os.path.join(tmp_output_dir, "l1_pge_report.txt"))[0]

        # Construct EDP filename and report name based on ccsds name
        base_edp_name = stream.ccsds_name.replace("l0_ccsds", "l1a_eng").replace(".bin", ".ext")
        base_edp_path = os.path.join(stream.l1a_dir, base_edp_name)
        report_path = base_edp_path.replace(".ext", "_report.txt")

        # Copy tmp EDP files, report, and log back to store. Build product dictionary
        product_dict = {}
        outputs = {}
        for path in tmp_edp_prod_paths:
            edp_name = os.path.basename(path)
            prod_path = base_edp_path.replace(".ext", f"_{edp_name}")
            wm.copy(path, prod_path)
            subheader_id = edp_name.split("_")[0]
            product_dict[subheader_id] = {
                f"{subheader_id}_path": prod_path,
                "created": datetime.datetime.fromtimestamp(os.path.getmtime(prod_path), tz=datetime.timezone.utc)
            }
            outputs[f"l1a_edp_{subheader_id}_path"] = prod_path
        wm.copy(tmp_report_path, report_path)
        l1a_pge_log_path = base_edp_path.replace(".ext", "_pge.log")
        wm.copy(glob.glob(os.path.join(tmp_log_dir, "*"))[0], l1a_pge_log_path)
        # Update DB with product dictionary
        dm.update_stream_metadata(stream.hosc_name, {"products.l1a": product_dict})

        doc_version = "EMIT IOS SDS ICD JPL-D 104239, Initial"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": input_files,
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": product_dict["0x15"]["created"],
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": outputs
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
    ignore_missing_bad = luigi.BoolParameter(default=False)

    memory = 90000
    local_tmp_space = 125000

    task_namespace = "emit"

    def requires(self):

        logger.debug(f"{self.task_family} requires: {self.orbit_id}")
        return None

    def output(self):

        logger.debug(f"{self.task_family} output: {self.orbit_id}")
        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        return OrbitTarget(orbit=wm.orbit, task_family=self.task_family)

    def work(self):

        logger.debug(f"{self.task_family} work: {self.orbit_id}")
        wm = WorkflowManager(config_path=self.config_path, orbit_id=self.orbit_id)
        dm = wm.database_manager
        orbit = wm.orbit
        pge = wm.pges["emit-sds-l1a"]

        # TODO: Use --test_mode to remove dependency on database.  Is this even possible since orbit must be defined?

        # Check for missing BAD data before proceeding. Override with --ignore_missing_bad arg
        if self.ignore_missing_bad is False and orbit.has_complete_bad_data() is False:
            raise RuntimeError(f"Unable to run {self.task_family} on {self.orbit_id} due to missing BAD data in "
                               f"orbit.")

        # Sort the bad sto paths just to be safe
        orbit.associated_bad_sto.sort()
        if len(orbit.associated_bad_sto) == 0:
            raise RuntimeError(f"Unable to find any BAD STO files for orbit {self.orbit_id}!")

        # Copy sto files to an input directory and call PGE
        tmp_bad_sto_dir = os.path.join(self.local_tmp_dir, f"o{self.orbit_id}_bad_sto_files")
        wm.makedirs(tmp_bad_sto_dir)
        for p in orbit.associated_bad_sto:
            wm.copy(p, os.path.join(tmp_bad_sto_dir, os.path.basename(p)))

        # Build command and run
        reformat_bad_exe = os.path.join(pge.repo_dir, "reformat_bad.py")
        tmp_log_path = os.path.join(self.local_tmp_dir, "reformat_bad_pge.log")
        cmd = ["python", reformat_bad_exe, tmp_bad_sto_dir,
               "--work_dir", self.local_tmp_dir,
               "--start_time", (orbit.start_time - datetime.timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%S"),
               "--stop_time", (orbit.stop_time + datetime.timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%S"),
               "--level", self.level,
               "--log_path", tmp_log_path]
        env = os.environ.copy()
        env["AIT_ROOT"] = wm.pges["emit-ios"].repo_dir
        env["AIT_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "config.yaml")
        env["AIT_ISS_CONFIG"] = os.path.join(env["AIT_ROOT"], "config", "sim.yaml")
        pge.run(cmd, tmp_dir=self.tmp_dir, env=env)

        # Copy output files back
        tmp_output_path = glob.glob(os.path.join(self.local_tmp_dir, "output", "*"))[0]
        wm.copy(tmp_output_path, orbit.uncorr_att_eph_path)
        wm.copy(tmp_log_path, orbit.uncorr_att_eph_path.replace(".nc", "_pge.log"))

        # Symlink from orbits directory back to associated BAD files
        for p in orbit.associated_bad_sto:
            wm.symlink(p, os.path.join(orbit.raw_dir, os.path.basename(p)))

        creation_time = datetime.datetime.fromtimestamp(os.path.getmtime(orbit.uncorr_att_eph_path),
                                                        tz=datetime.timezone.utc)
        metadata = {
            "associated_bad_netcdf": orbit.uncorr_att_eph_path,
            "products.l1a": {
                "uncorr_att_eph_path": orbit.uncorr_att_eph_path,
                "created": creation_time
            },
        }
        dm.update_orbit_metadata(orbit.orbit_id, metadata)

        doc_version = "N/A"
        log_entry = {
            "task": self.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "bad_sto_paths": orbit.associated_bad_sto,
            },
            "pge_run_command": " ".join(cmd),
            "documentation_version": doc_version,
            "product_creation_time": creation_time,
            "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "completion_status": "SUCCESS",
            "output": {
                "l1a_ucorr_att_eph_path": orbit.uncorr_att_eph_path
            }
        }
        dm.insert_orbit_log_entry(orbit.orbit_id, log_entry)
