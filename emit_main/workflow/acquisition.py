"""
This code contains the Acquisition class that manages acquisitions and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import logging
import os

from emit_main.database.database_manager import DatabaseManager

logger = logging.getLogger("emit-main")


class Acquisition:

    def __init__(self, config_path, acquisition_id):
        """
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["filesystem_config"])
            self.__dict__.update(config["build_config"])

        self.database_manager = DatabaseManager(config_path)

        self.config_path = config_path
        self.acquisition_id = acquisition_id

        # TODO: Define and initialize acquisition metadata
        # TODO: What to do if entry doesn't exist yet?
        self.metadata = self._load_metadata()
#        self.metadata = {"_id": self.acquisition_id}
#        if metadata is not None:
#            self.metadata.update(metadata)

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.local_store_dir, self.instrument)
        self.environment_dir = os.path.join(self.instrument_dir, self.environment)
        self.data_dir = os.path.join(self.environment_dir, "data")

        # Check for instrument again based on filename
        instrument_prefix = self.instrument
        if self.acquisition_id.startswith("ang"):
            instrument_prefix = "ang"
        # Get date from acquisition string
        self.date_str = self.acquisition_id[len(instrument_prefix):(8 + len(instrument_prefix))]
        self.date_dir = os.path.join(self.data_dir, self.date_str)
        self.acquisition_dir = os.path.join(self.date_dir, self.acquisition_id)
        self.dirs.extend([self.date_dir, self.acquisition_dir])

        # TODO: Set orbit and scene. Defaults below are for testing only
#        acq_meta = self.database_manager.find_acquisition_by_id(self.acquisition_id)
        if "orbit" in self.metadata.keys():
            self.orbit_num = self.metadata["orbit"]
        else:
            self.orbit_num = "00001"
        if "scene" in self.metadata.keys():
            self.scene_num = self.metadata["scene"]
        else:
            self.scene_num = "001"

        self.__dict__.update(self._build_acquisition_paths())

        # Make directories if they don't exist
        for d in self.dirs:
            if not os.path.exists(d):
                os.makedirs(d)

    def _build_acquisition_paths(self):
        product_map = {
            "l1a": {
                "raw": ["img", "hdr"],
                "rawqa": ["txt"]
            },
            "l1b": {
                "rdn": ["img", "hdr", "png", "kmz"],
                "loc": ["img", "hdr"],
                "obs": ["img", "hdr"],
                "glt": ["img", "hdr"],
                "att": ["nc"],
                "geoqa": ["txt"]
            },
            "l2a": {
                "rfl": ["img", "hdr"],
                "uncert": ["img", "hdr"],
                "mask": ["img", "hdr"]
            }
        }
        paths = {}
        for level, prod_map in product_map.items():
            level_data_dir = os.path.join(self.acquisition_dir, level)
            self.__dict__.update({level + "_data_dir": level_data_dir})
            self.dirs.append(level_data_dir)
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format + "_path"
                    prod_prefix = "_".join([self.acquisition_id,
                                            "o" + self.orbit_num,
                                            "s" + self.scene_num,
                                            level,
                                            prod,
                                            "b" + self.build_num,
                                            "v" + self.processing_version])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(self.acquisition_dir, level, prod_name)
                    paths[prod_key] = prod_path
        return paths

    def _load_metadata(self):
        acquisitions = self.database_manager.db.acquisitions
        query = {"acquisition_id": self.acquisition_id, "build_num": self.build_num}
        return acquisitions.find_one(query)

    def save_metadata(self, metadata):
        acquisitions = self.database_manager.db.acquisitions
        query = {"acquisition_id": self.acquisition_id, "build_num": self.build_num}
        set_value = {"$set": metadata}
        acquisitions.update_one(query, set_value, upsert=True)

    def save_processing_log_entry(self, entry):
        acquisitions = self.database_manager.db.acquisitions
        query = {"acquisition_id": self.acquisition_id, "build_num": self.build_num}
        push_value = {"$push": {"processing_log": entry}}
        acquisitions.update_one(query, push_value)
