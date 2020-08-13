"""
This code contains the FileManager class that handles filesystem paths

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import os


class FileManager:

    def __init__(self, config_path, acquisition_id=None):
        """
        :param config_path: Path to config file containing environment settings
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self.acquisition_id = acquisition_id

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["filesystem_config"])
            self.__dict__.update(config["build_config"])

        # Create mappings to track directories and paths for an acquisition
        self.dirs = {}
        self.paths = {}

        self.dirs["environment"] = os.path.join(self.local_store_dir, self.instrument, self.environment)
        self.dirs["data"] = os.path.join(self.dirs["environment"], "data")

        # If we have an acquisition id, create acquisition paths
        if self.acquisition_id is not None:
            # Get date from acquisition string
            self.date_str = self.acquisition_id[len(self.instrument):(8 + len(self.instrument))]
            self.dirs["date"] = os.path.join(self.dirs["data"], self.date_str)

            # TODO: Set orbit and scene
            self.orbit_num = "00001"
            self.scene_num = "001"

            self.dirs["acquisition"] = os.path.join(self.dirs["date"], self.acquisition_id)

            self.paths = self._build_acquisition_paths()

        # Make directories if they don't exist
        for d in self.dirs.values():
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
            }
        }
        paths = {}
        for level, prod_map in product_map.items():
            self.dirs[level] = os.path.join(self.dirs["acquisition"], level)
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format
                    prod_prefix = "_".join([self.acquisition_id,
                                            "o" + self.orbit_num,
                                            "s" + self.scene_num,
                                            level,
                                            prod,
                                            "b" + self.build_num,
                                            "v" + self.processing_version])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(self.dirs["acquisition"], level, prod_name)
                    paths[prod_key] = prod_path
        return paths

    def path_exists(self, path):
        return os.path.exists(path)

    def touch_path(self, path):
        os.system(" ".join(["touch", path]))

    def remove_dir(self, path):
        if os.path.exists(path):
            os.system(" ".join(["rm", "-rf", path]))

    def remove_path(self, path):
        if os.path.exists((path)):
            os.system(" ".join(["rm", "-f", path]))