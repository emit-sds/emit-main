"""
This code contains the Stream class that manages HOSC and CCSDS data

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import grp
import json
import logging
import os
import pwd

from emit_main.database.database_manager import DatabaseManager

logger = logging.getLogger("emit-main")


class Stream:

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

        self.hosc_name = None
        self.ccsds_name = None
        # self.edp_name = None
        # self.frames = []

        # Read metadata from db
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_stream_by_name(os.path.basename(stream_path))
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Create base directories and add to list to create directories later
        self.dirs = []
        # TODO: These don't all have to be class variables, do they?
        self.instrument_dir = os.path.join(self.local_store_dir, self.instrument)
        self.environment_dir = os.path.join(self.instrument_dir, self.environment)
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.streams_dir = os.path.join(self.data_dir, "streams")
        self.apid_dir = os.path.join(self.streams_dir, self.apid)
        self.ingest_dir = os.path.join(self.environment_dir, "ingest")

#        self.date_str = self.start_time_str[:8]
        self.date_str = self.start_time.strftime("%Y%m%d")
        self.date_dir = os.path.join(self.apid_dir, self.date_str)
        self.l0_dir = os.path.join(self.date_dir, "l0")
        self.l1a_dir = os.path.join(self.date_dir, "l1a")
        if self.hosc_name:
            self.hosc_path = os.path.join(self.l0_dir, self.hosc_name)
        if self.ccsds_name:
            self.ccsds_path = os.path.join(self.l0_dir, self.ccsds_name)
        self.dirs.extend([self.streams_dir, self.apid_dir, self.date_dir, self.l0_dir, self.l1a_dir])

        # Make directories if they don't exist
        for d in self.dirs:
            if not os.path.exists(d):
                os.makedirs(d)
                # Change group ownership in shared environments
                if self.environment in ["dev", "test", "ops"]:
                    uid = pwd.getpwnam(pwd.getpwuid(os.getuid())[0]).pw_uid
                    gid = grp.getgrnam(self.instrument + "-" + self.environment).gr_gid
                    os.chown(d, uid, gid)

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
