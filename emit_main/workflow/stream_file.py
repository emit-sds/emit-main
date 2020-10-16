"""
This code contains the HOSC class that manages HOSC data

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os

logger = logging.getLogger("emit-main")


class StreamFile:

    def __init__(self, config_path, stream_path):
        """
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["filesystem_config"])
            self.__dict__.update(config["build_config"])

        self.config_path = config_path
        self.path = stream_path
        self.name = os.path.basename(stream_path)
        if "hsc.bin" in self.name:
            self.stream_type = "hosc"
            tokens = self.name.split("_")
            self.apid = tokens[1]
            # Need to add first two year digits
            self.start_time_str = "20" + tokens[2]
            self.stop_time_str = "20" + tokens[3]
            self.production_time = "20" + tokens[4]
        else:
            self.stream_type = "ccsds"
            self.apid = self.name[:4]
            self.start_time_str = datetime.datetime.strptime(self.name[5:24],
                                                             "%Y-%m-%dT%H%M%S").strftime("%Y%m%d%H%M%S")

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.local_store_dir, self.instrument)
        self.environment_dir = os.path.join(self.instrument_dir, self.environment)
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.ingest_dir = os.path.join(self.environment_dir, "ingest")

        self.date_str = self.start_time_str[:8]
        self.date_dir = os.path.join(self.data_dir, self.date_str)
        self.l0_dir = os.path.join(self.date_dir, "l0")
        self.hosc_dir = os.path.join(self.l0_dir, "hosc")
        self.ccsds_dir = os.path.join(self.l0_dir, "ccsds")
        self.dirs.extend([self.date_dir, self.l0_dir, self.hosc_dir, self.ccsds_dir])

        # Make directories if they don't exist
        for d in self.dirs:
            if not os.path.exists(d):
                os.makedirs(d)
