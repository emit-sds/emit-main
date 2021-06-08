"""
This code contains the Stream class that manages HOSC and CCSDS data

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import grp
import logging
import os
import pwd

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class Stream:

    def __init__(self, config_path, stream_path):
        """
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self.config_path = config_path
        self.stream_path = stream_path

        # Get config properties
        self.config = Config(config_path).get_dictionary()

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
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
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
                if self.config["environment"] in ["dev", "test", "ops"]:
                    uid = pwd.getpwnam(pwd.getpwuid(os.getuid())[0]).pw_uid
                    gid = grp.getgrnam(self.config["instrument"] + "-" + self.config["environment"]).gr_gid
                    os.chown(d, uid, gid)

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
