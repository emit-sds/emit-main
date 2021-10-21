"""
This code contains the DataCollection class that manages data collections and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import logging
import os
import pytz

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class DataCollection:

    def __init__(self, config_path, dcid):
        """
        :param config_path: The path to the config file
        :param dcid: The data collection identifier
        """

        self.config_path = config_path
        self.dcid = dcid

        # Get config properties
        self.config = Config(config_path).get_dictionary()

        dm = DatabaseManager(config_path)
        self.metadata = dm.find_data_collection_by_id(self.dcid)
        self.__dict__.update(self.metadata)

        # Add UTC tzinfo property to start/stop datetime objects for printing
        if "start_time" in self.__dict__ and self.start_time is not None:
            self.start_time = pytz.utc.localize(self.start_time)
        if "stop_time" in self.__dict__ and self.stop_time is not None:
            self.stop_time = pytz.utc.localize(self.stop_time)

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.data_collections_dir = os.path.join(self.data_dir, "data_collections")

        # Create directory structure for "by_dcid"
        self.by_dcid_dir = os.path.join(self.data_collections_dir, "by_dcid")
        self.dcid_hash_dir = os.path.join(self.by_dcid_dir, self.dcid[:5])
        self.dcid_dir = os.path.join(self.dcid_hash_dir, self.dcid)
        self.frames_dir = os.path.join(
            self.dcid_dir,
            "_".join([self.dcid, "frames", "b" + self.config["build_num"], "v" + self.config["processing_version"]]))
        self.dirs.extend([self.data_collections_dir, self.by_dcid_dir, self.dcid_hash_dir, self.dcid_dir,
                          self.frames_dir])

        # Create directory structure for "by_date" and symlink to frames dir above
        if "start_time" in self.__dict__ and self.start_time is not None:
            self.by_date_dir = os.path.join(self.by_date, "by_date")
            start_date_str = self.start_time.strftime("%Y%m%d")
            self.date_dir = os.path.join(self.by_date_dir, start_date_str)
            self.date_dcid_dir = os.path.join(self.date_dir, f"{start_date_str}_{self.dcid}")
            self.dirs.extend([self.by_date_dir, self.date_dir, self.date_dcid_dir])
            # Create symlink to frames dir above
            self.frames_symlink = os.path.join(
                self.date_dcid_dir,
                "_".join([self.dcid, "frames", "b" + self.config["build_num"], "v" + self.config["processing_version"]])
            )

        # Make directories and symlinks if they don't exist
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=config_path)
        for d in self.dirs:
            wm.makedirs(d)
        if "start_time" in self.__dict__ and self.start_time is not None:
            wm.symlink(self.frames_dir, self.frames_symlink)

    def has_complete_set_of_frames(self):
        frames = [os.path.basename(frame) for frame in glob.glob(os.path.join(self.frames_dir, "*"))]
        frames.sort()
        # Check incrementing frame num
        frame_nums = [int(frame.split("_")[1]) for frame in frames]
        if frame_nums != list(range(frame_nums[0], frame_nums[0] + len(frame_nums))):
            logger.warning("Set of frames is not sequential!")
            return False
        # Check that first frame has status 1 or 5
        if frames[0].split("_")[3] not in ("1", "5"):
            logger.warning("First frame in set does not begin with status 1 or 5!")
            return False
        # Check that all subsequent frames have status 0 or 4
        for frame in frames[1:]:
            if frame.split("_")[3] not in ("0", "4"):
                logger.warning("One of the frames in the set (after the first) does not have status 0 or 4!")
                return False
        # Check that we have the expected number of frames
        expected_num = int(frames[0].split("_")[2])
        if len(frames) != expected_num:
            logger.warning(f"Number of frames, {len(frames)}, does not match expected number, {expected_num}")
        return True
