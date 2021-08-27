"""
This code contains the Acquisition class that manages acquisitions and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import grp
import logging
import os
import pwd
import pytz

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class Acquisition:

    def __init__(self, config_path, acquisition_id):
        """
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self.config_path = config_path
        self.acquisition_id = acquisition_id

        # Get config properties
        self.config = Config(config_path, acquisition_id).get_dictionary()

        dm = DatabaseManager(config_path)
        self.metadata = dm.find_acquisition_by_id(self.acquisition_id)
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Add UTC tzinfo property to start/stop datetime objects for printing
        self.start_time = pytz.utc.localize(self.start_time)
        self.stop_time = pytz.utc.localize(self.stop_time)

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.acquisitions_dir = os.path.join(self.data_dir, "acquisitions")

        # Check for instrument again based on filename
        instrument_prefix = self.config["instrument"]
        if self.acquisition_id.startswith("ang"):
            instrument_prefix = "ang"
        # Get date from acquisition string
        self.date_str = self.acquisition_id[len(instrument_prefix):(8 + len(instrument_prefix))]
        self.date_dir = os.path.join(self.acquisitions_dir, self.date_str)
        self.acquisition_id_dir = os.path.join(self.date_dir, self.acquisition_id)
        self.dirs.extend([self.acquisitions_dir, self.date_dir, self.acquisition_id_dir])

        self.__dict__.update(self._build_acquisition_paths())

        # Add sub-dirs
        self.frames_dir = self.raw_img_path.replace("_raw_", "_frames_").replace(".img", "")
        self.decomp_dir = self.frames_dir.replace("_frames_", "_decomp_")
        self.dirs.extend([self.frames_dir, self.decomp_dir])

        # Make directories if they don't exist
        for d in self.dirs:
            if not os.path.exists(d):
                os.makedirs(d)
                # Change group ownership in shared environments
                if self.config["environment"] in ["dev", "test", "ops"]:
                    uid = pwd.getpwnam(pwd.getpwuid(os.getuid())[0]).pw_uid
                    gid = grp.getgrnam(self.config["instrument"] + "-" + self.config["environment"]).gr_gid
                    os.chown(d, uid, gid)

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
        if "products" not in self.metadata:
            self.metadata["products"] = {}
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}
        if "l1b" not in self.metadata["products"]:
            self.metadata["products"]["l1b"] = {}
        if "l2a" not in self.metadata["products"]:
            self.metadata["products"]["l2a"] = {}
        if "l2b" not in self.metadata["products"]:
            self.metadata["products"]["l2b"] = {}
        if "l3" not in self.metadata["products"]:
            self.metadata["products"]["l3"] = {}

    def _build_acquisition_paths(self):
        product_map = {
            "l1a": {
                "raw": ["img", "hdr"],
                "dark": ["img", "hdr"],
                "rawqa": ["txt"]
            },
            "l1b": {
                "rdn": ["img", "hdr", "png", "kmz"],
                "rdnort": ["img", "hdr"],
                "loc": ["img", "hdr"],
                "locort": ["img", "hdr"],
                "obs": ["img", "hdr"],
                "glt": ["img", "hdr"],
                "att": ["nc"],
                "geoqa": ["txt"]
            },
            "l2a": {
                "rfl": ["img", "hdr"],
                "uncert": ["img", "hdr"],
                "lbl": ["img", "hdr"],
                "lblort": ["img", "hdr"],
                "statesubs": ["img", "hdr"],
                "mask": ["img", "hdr"]
            },
            "l2b": {
                "abun": ["img", "hdr"],
                "abununcert": ["img", "hdr"]
            }
        }
        paths = {}
        for level, prod_map in product_map.items():
            level_data_dir = os.path.join(self.acquisition_id_dir, level)
            self.__dict__.update({level + "_data_dir": level_data_dir})
            self.dirs.append(level_data_dir)
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format + "_path"
                    prod_prefix = "_".join([self.acquisition_id,
                                            "o" + self.orbit,
                                            "s" + self.scene,
                                            level,
                                            prod,
                                            "b" + self.config["build_num"],
                                            "v" + self.config["processing_version"]])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(self.acquisition_id_dir, level, prod_name)
                    paths[prod_key] = prod_path
        return paths

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
