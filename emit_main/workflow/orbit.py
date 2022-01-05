"""
This code contains the Orbit class that manages orbits and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class Orbit:

    def __init__(self, config_path, orbit_id):
        """
        :param config_path: The path to the config file
        :param orbit_id: The padded string representation of the orbit number
        """

        self.config_path = config_path
        self.orbit_id = orbit_id

        # Read metadata from db
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_orbit_by_id(self.orbit_id)
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Get config properties
        self.config = Config(config_path, self.start_time).get_dictionary()

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.orbits_dir = os.path.join(self.data_dir, "orbits")
        self.orbit_id_dir = os.path.join(self.orbits_dir, self.orbit_id)
        self.raw_dir = os.path.join(self.orbit_id_dir, "raw")
        self.l1a_dir = os.path.join(self.orbit_id_dir, "l1a")
        self.dirs.extend([self.orbits_dir, self.orbit_id_dir, self.raw_dir, self.l1a_dir])

        # Create product names
        uncorr_fname = "_".join(["emit", self.start_time.strftime("%Y%m%dt%H%M%S"), f"o{self.orbit_id}",
                                 "l1a", "att", f"b{self.config['build_num']}",
                                 f"v{self.config['processing_version']}.nc"])
        self.uncorr_att_eph_path = os.path.join(self.l1a_dir, uncorr_fname)
        self.corr_att_eph_path = self.uncorr_att_eph_path.replace("l1a", "l1b")

        # Make directories and symlinks if they don't exist
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
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}

    def has_complete_bad_data(self):
        if "associated_bad_sto" not in self.metadata:
            logger.info(f"No 'associated_bad_sto' property in orbit {self.orbit_id}")
            return False

        if "stop_time" not in self.metadata:
            logger.info(f"Orbit {self.orbit_id} does not have a stop time.")
            return False

        bad_sto_files = [os.path.basename(p) for p in self.metadata["associated_bad_sto"]]
        bad_sto_files.sort()

        # Check if empty
        if len(bad_sto_files) == 0:
            logger.info(f"No associated BAD STO files for orbit {self.orbit_id}")
            return False

        # Check that associated BAD sto files encompass orbit date range
        bad_start_time = datetime.datetime.strptime(bad_sto_files[0].split("_")[1], "%Y%m%dT%H%M%S")
        bad_stop_time = datetime.datetime.strptime(bad_sto_files[-1].split("_")[2], "%Y%m%dT%H%M%S")
        if bad_start_time > self.start_time or bad_stop_time < self.stop_time:
            logger.info(f"Start and stop time for associated BAD STO files of orbit {self.orbit_id} do not "
                        f"encompass the orbit's entire time range.")
            return False

        # Check that there are no gaps
        prev_file = None
        for file in bad_sto_files:
            if prev_file == None:
                prev_file = file
                continue
            prev_stop_time = datetime.datetime.strptime(prev_file.split("_")[2], "%Y%m%dT%H%M%S")
            cur_start_time = datetime.datetime.strptime(file.split("_")[1], "%Y%m%dT%H%M%S")
            gap = cur_start_time - prev_stop_time
            # If the gap is bigger than 10 seconds return False
            if gap.total_seconds() > 10:
                logger.info(f"Found a gap of {gap.total_seconds()} while comparing associated BAD STO files for "
                            f"orbit {self.orbit_id}")
                return False
            prev_file = file

        # If we made it this far, then we have a complete set
        return True
