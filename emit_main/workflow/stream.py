"""
This code contains the Stream class that manages HOSC and CCSDS data

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

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

        # Declare these variables which will get populated later by tasks
        self.hosc_name = None
        self.ccsds_name = None
        self.bad_name = None

        # Read metadata from db
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_stream_by_name(os.path.basename(stream_path))
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Get config properties
        self.config = Config(config_path, self.start_time).get_dictionary()

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.streams_dir = os.path.join(self.data_dir, "streams")
        self.apid_dir = os.path.join(self.streams_dir, self.apid)
        self.ingest_dir = os.path.join(self.environment_dir, "ingest")

#        self.date_str = self.start_time_str[:8]
        self.date_str = self.start_time.strftime("%Y%m%d")
        self.date_dir = os.path.join(self.apid_dir, self.date_str)
        self.raw_dir = os.path.join(self.date_dir, "raw")
        self.l0_dir = os.path.join(self.date_dir, "l0")
        self.l1a_dir = os.path.join(self.date_dir, "l1a")
        self.dirs.extend([self.streams_dir, self.apid_dir, self.date_dir, self.raw_dir, self.l0_dir])
        if self.apid != "bad":
            self.dirs.extend([self.l1a_dir])

        if self.hosc_name:
            self.hosc_path = os.path.join(self.raw_dir, self.hosc_name)
        if self.ccsds_name:
            self.ccsds_path = os.path.join(self.l0_dir, self.ccsds_name)
            if self.apid == "1675":
                self.frames_dir = os.path.join(
                    self.l1a_dir, self.ccsds_name.replace("l0_ccsds", "l1a_frames").replace(".bin", ""))
                self.dirs.append(self.frames_dir)
        if self.bad_name:
            self.bad_path = os.path.join(self.raw_dir, self.bad_name)

        # Make directories if they don't exist
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=config_path)
        for d in self.dirs:
            wm.makedirs(d)

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
        if "products" not in self.metadata:
            self.metadata["products"] = {}
        if "raw" not in self.metadata["products"]:
            self.metadata["products"]["raw"] = {}
        if "l0" not in self.metadata["products"]:
            self.metadata["products"]["l0"] = {}
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}
