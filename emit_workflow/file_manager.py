"""
This code contains the FileManager class that handles file naming and lookups

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import os

class FileManager:

    def __init__(self, acquisition=None, config_path="config.json"):
        """

        :param config_path:
        """

        self.acquisition = acquisition

        # Read config.json for environment specific paths
        with open(config_path, "r") as f:
            self.__dict__.update(json.load(f))

        env_dir = os.path.join(self.local_store_path, self.instrument, self.env)
        data_dir = os.path.join(env_dir, "data")
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        if "build_num" not in self.__dict__.keys():
            self.build_num = 1
        if "processing_version" not in self.__dict__.keys():
            self.processing_version = 1

        # If we have an acquisition id, create acquisition paths
        if self.acquisition is not None:
            # Get date from acquisition string
            date_str = self.acquisition[len(self.instrument):(8 + len(self.instrument))]
            date_dir = os.path.join(data_dir, date_str)

            # TODO: Set orbit and scene
            self.orbit_num = 1
            self.scene_num = 1

            acquisition_dir = os.path.join(date_dir, self.acquisition)
            product_paths = self._build_acquisition_paths(acquisition_dir)
            self.__dict__.update(product_paths)

    def _build_acquisition_paths(self, acquisition_base_path):
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
            }
        }
        paths = {}
        for level, prod_map in product_map.items():
            paths[level] = {}
            acquisition_level_path = os.path.join(acquisition_base_path, level)
            if not os.path.exists(acquisition_level_path):
                os.makedirs(acquisition_level_path)
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format
                    prod_prefix = "_".join([self.acquisition,
                                            "o" + str(self.orbit_num).zfill(5),
                                            "s" + str(self.scene_num).zfill(3),
                                            level,
                                            prod,
                                            "b" + str(self.build_num).zfill(3),
                                            "v" + str(self.processing_version).zfill(2)])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(acquisition_base_path, level, prod_name)
                    paths[level][prod_key] = prod_path
        return paths

    def path_exists(self, path):
        return os.path.exists(path)

    def touch_path(self, path):
        os.system(" ".join(["touch", path]))

    def remove_path(self, path):
        os.system(" ".join(["rm", "-f", path]))