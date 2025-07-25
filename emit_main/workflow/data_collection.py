"""
This code contains the DataCollection class that manages data collections and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import logging
import math
import os

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class DataCollection:

    def __init__(self, config_path, dcid):
        """
        :param config_path: The path to the config file
        :param dcid: The data collection identifier
        """

        self.config_path = config_path
        self.dcid = dcid

        # Read metadata from db
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_data_collection_by_id(self.dcid)
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Get config properties
        self.config = Config(config_path, self.start_time).get_dictionary()

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.data_collections_dir = os.path.join(self.data_dir, "data_collections")

        # Create directory structure for "by_dcid" and top-level "by_date" dir
        self.by_dcid_dir = os.path.join(self.data_collections_dir, "by_dcid")
        self.by_date_dir = os.path.join(self.data_collections_dir, "by_date")
        self.dcid_hash_dir = os.path.join(self.by_dcid_dir, self.dcid[:5])
        self.dcid_dir = os.path.join(self.dcid_hash_dir, self.dcid)
        self.frames_dir = os.path.join(
            self.dcid_dir,
            "_".join([self.dcid, "frames", "b" + self.config["build_num"], "v" + self.config["processing_version"]]))
        self.decomp_dir = self.frames_dir.replace("_frames_", "_decomp_")
        self.acquisitions_dir = self.frames_dir.replace("_frames_", "_acquisitions_")
        self.ch4_dir = os.path.join(self.dcid_dir,'ghg', 'ch4')
        self.co2_dir = os.path.join(self.dcid_dir,'ghg', 'co2')
        self.dirs.extend([self.data_collections_dir, self.by_dcid_dir, self.by_date_dir, self.dcid_hash_dir,
                          self.dcid_dir, self.frames_dir, self.decomp_dir, self.acquisitions_dir, self.ch4_dir,
                          self.co2_dir])

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
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}

    def has_complete_set_of_frames(self):
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=self.config_path)

        frames = [os.path.basename(frame) for frame in glob.glob(os.path.join(self.frames_dir, "*[!txt]"))]
        frames.sort()
        # Check that we have nonzero frames
        if len(frames) == 0:
            wm.print(__name__, f"No frames found in {self.frames_dir}")
            return False

        expected_num = int(frames[0].split("_")[3])
        found_num = int(frames[-1].split("_")[2]) + 1

        # Check incrementing frame num
        frame_nums = [int(frame.split("_")[2]) for frame in frames]
        if frame_nums != list(range(0, found_num)):
            wm.print(__name__, "Set of frames is not sequential!")
            return False

        # Check that we have the expected number
        if found_num != expected_num:
            wm.print(__name__, f"Number of frames found, {found_num}, does not equal the expected number of "
                               f"{expected_num}")
            return False

        # Check that first frame has status 1 or 5
        if frames[0].split("_")[4] not in ("1", "5", "9"):
            wm.print(__name__, "First frame in set does not begin with status 1, 5, or 9!")
            return False

        # Check that all subsequent frames have status 0 or 4
        for frame in frames[1:]:
            if frame.split("_")[4] not in ("0", "4", "9"):
                wm.print(__name__, "One of the frames in the set (after the first) does not have status 0, 4, or 9!")
                return False

        # If we made it this far, then return True
        return True

    def has_complete_ch4_aqcuisitions(self):

        dm = DatabaseManager(self.config_path)

        acquisitions_coll = dm.db.acquisitions

        #Get list of acquisition ids expected to have CH4 products
        query = {
            "associated_dcid": self.dcid,
            "mean_solar_zenith": {"$lt": 80},
            "build_num": self.config["build_num"]
        }

        expected = list(acquisitions_coll.find(query))
        expected_acq_ids = [doc['acquisition_id'] for doc in expected]

        #Get list of acquisition ids with completed CH4 products
        query = {
            "associated_dcid": self.dcid,
            "products.ghg.ch4.ortch4.tif_path": {"$exists": 1},
            "products.ghg.ch4.ortsensch4.tif_path": {"$exists": 1},
            "products.ghg.ch4.ortuncertch4.tif_path": {"$exists": 1},
            "build_num": self.config["build_num"]
        }

        completed = list(acquisitions_coll.find(query))
        completed_acq_ids = [doc['acquisition_id'] for doc in completed]
 
        return set(expected_acq_ids) == set(completed_acq_ids)
    
    def has_complete_co2_aqcuisitions(self):

        dm = DatabaseManager(self.config_path)

        acquisitions_coll = dm.db.acquisitions

        #Get list of acquisition ids expected to have CO2 products
        query = {
            "associated_dcid": self.dcid,
            "mean_solar_zenith": {"$lt": 80},
            "build_num": self.config["build_num"]
        }

        expected = list(acquisitions_coll.find(query))
        expected_acq_ids = [doc['acquisition_id'] for doc in expected]

        #Get list of acquisition ids with completed CO2 products
        query = {
            "associated_dcid": self.dcid,
            "products.ghg.co2.ortco2.tif_path": {"$exists": 1},
            "products.ghg.co2.ortsensco2.tif_path": {"$exists": 1},
            "products.ghg.co2.ortuncertco2.tif_path": {"$exists": 1},
            "build_num": self.config["build_num"]
        }

        completed = list(acquisitions_coll.find(query))
        completed_acq_ids = [doc['acquisition_id'] for doc in completed]
 
        return set(expected_acq_ids) == set(completed_acq_ids)