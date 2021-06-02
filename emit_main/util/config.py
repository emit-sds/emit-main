"""
This code contains the Config class that reads in the config file and returns its properties

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os

logger = logging.getLogger("emit-main")


class Config:

    def __init__(self, config_path, acquisition_id=None):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = config_path
        self.properties = {}

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            # Read in groups of properties
            self.properties.update(config["general_config"])
            self.properties.update(config["filesystem_config"])
            self.properties.update(config["database_config"])
            self.properties.update(config["build_config"])
            # Use build_num to read in build config
            config_dir = os.path.dirname(config_path)
            build_config_path = os.path.join(config_dir, "build", "build_" + self.properties["build_num"] + ".json")
            with open(build_config_path, "r") as b:
                build_config = json.load(b)
                self.properties.update(build_config)
            # Read in ancillary paths
            self.properties.update(self._get_ancillary_file_paths(config["ancillary_files"], acquisition_id))

    def _get_ancillary_file_paths(self, anc_files_config, acquisition_id):
        if "versions" in anc_files_config:
            if acquisition_id is not None:
                versions = anc_files_config["versions"]
                acquisition_date = self._get_date_from_acquisition(acquisition_id)
                # Look for matching date range and update top level dictionary with those key/value pairs
                for version in versions:
                    start_date = datetime.datetime.strptime(version["start_date"], "%Y-%m-%d")
                    end_date = datetime.datetime.strptime(version["end_date"], "%Y-%m-%d")
                    if start_date <= acquisition_date < end_date:
                        anc_files_config.update(version)
            # Remove "versions" and return dictionary
            del anc_files_config["versions"]
        return anc_files_config

    def _get_date_from_acquisition(self, acquisition_id):
        instrument_prefix = self.properties["instrument"]
        if acquisition_id.startswith("ang"):
            instrument_prefix = "ang"
        # Get date from acquisition string
        date_str = acquisition_id[len(instrument_prefix):(15 + len(instrument_prefix))]
        return datetime.datetime.strptime(date_str, "%Y%m%dt%H%M%S")

    def get_properties(self):
        return self.properties
