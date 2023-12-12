"""
This code contains the Acquisition class that manages acquisitions and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import glob
import logging
import os
import pytz

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class Acquisition:

    def __init__(self, config_path, acquisition_id):
        """
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self.config_path = config_path
        self.acquisition_id = acquisition_id

        # Read metadata from db
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_acquisition_by_id(self.acquisition_id)
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Get config properties
        self.config = Config(config_path, self.start_time).get_dictionary()

        # Create start/stop time date objects with UTC tzinfo property for printing
        self.start_time_with_tz = pytz.utc.localize(self.start_time)
        self.stop_time_with_tz = pytz.utc.localize(self.stop_time)

        # Define short orbit string
        self.short_orb = self.orbit[2:] if len(self.orbit) == 7 else self.orbit

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.acquisitions_dir = os.path.join(self.data_dir, "acquisitions")

        # Check for instrument again based on filename
        self.date_str = self.start_time.strftime("%Y%m%d")
        self.date_dir = os.path.join(self.acquisitions_dir, self.date_str)
        self.acquisition_id_dir = os.path.join(self.date_dir, self.acquisition_id)
        self.dirs.extend([self.acquisitions_dir, self.date_dir, self.acquisition_id_dir])

        self.__dict__.update(self._build_acquisition_paths())

        # Add sub-dirs
        self.frames_dir = self.raw_img_path.replace("_raw_", "_frames_").replace(".img", "")
        self.decomp_dir = self.frames_dir.replace("_frames_", "_decomp_")
        self.dirs.extend([self.frames_dir, self.decomp_dir])

        # Make directories if they don't exist
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=config_path)
        for d in self.dirs:
            wm.makedirs(d)

        # Build granule ur and paths for DAAC delivery on staging server
        daac_start_time_str = self.start_time.strftime("%Y%m%dT%H%M%S")

        if "daac_scene" in self.metadata:
            self.raw_granule_ur = f"EMIT_L1A_RAW_0{self.config['product_versions']['l1a']}_{daac_start_time_str}_" \
                                  f"{self.orbit}_{self.daac_scene}"
            self.rdn_granule_ur = f"EMIT_L1B_RAD_0{self.config['product_versions']['l1b']}_{daac_start_time_str}_" \
                                  f"{self.orbit}_{self.daac_scene}"
            self.obs_granule_ur = f"EMIT_L1B_OBS_0{self.config['product_versions']['l1b']}_{daac_start_time_str}_" \
                                  f"{self.orbit}_{self.daac_scene}"
            self.rfl_granule_ur = f"EMIT_L2A_RFL_0{self.config['product_versions']['l2a']}_{daac_start_time_str}_" \
                                  f"{self.orbit}_{self.daac_scene}"
            self.rfluncert_granule_ur = f"EMIT_L2A_RFLUNCERT_0{self.config['product_versions']['l2a']}_" \
                                        f"{daac_start_time_str}_{self.orbit}_{self.daac_scene}"
            self.mask_granule_ur = f"EMIT_L2A_MASK_0{self.config['product_versions']['l2a']}_{daac_start_time_str}_" \
                                   f"{self.orbit}_{self.daac_scene}"
            self.abun_granule_ur = f"EMIT_L2B_MIN_0{self.config['product_versions']['l2b']}_{daac_start_time_str}_" \
                                   f"{self.orbit}_{self.daac_scene}"
            self.abununcert_granule_ur = f"EMIT_L2B_MINUNCERT_0{self.config['product_versions']['l2b']}_" \
                                         f"{daac_start_time_str}_{self.orbit}_{self.daac_scene}"
        self.daac_staging_dir = os.path.join(self.config["daac_base_dir"], wm.config['environment'], "products",
                                             self.start_time.strftime("%Y%m%d"))
        self.daac_uri_base = f"https://{self.config['daac_server_external']}/emit/lpdaac/{wm.config['environment']}/" \
            f"products/{self.start_time.strftime('%Y%m%d')}/"
        self.daac_partial_dir = os.path.join(self.config["daac_base_dir"], wm.config['environment'],
                                             "partial_transfers")
        self.aws_staging_dir = os.path.join(self.config["aws_s3_base_dir"], wm.config['environment'], "products",
                                            self.start_time.strftime("%Y%m%d"))
        self.aws_s3_uri_base = f"s3://{self.config['aws_s3_bucket']}{self.aws_staging_dir}/"

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
        if "products" not in self.metadata:
            self.metadata["products"] = {}
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}
        if "l1b" not in self.metadata["products"]:
            self.metadata["products"]["l1b"] = {}
        if "l2a" not in self.metadata["products"]:
            self.metadata["products"]["l2a"] = {}
        if "l2b" not in self.metadata["products"]:
            self.metadata["products"]["l2b"] = {}
        if "l3" not in self.metadata["products"]:
            self.metadata["products"]["l3"] = {}

    def _build_acquisition_paths(self):
        product_map = {
            "l1a": {
                "raw": ["img", "hdr"],
                "dark": ["img", "hdr"],
                "rawqa": ["txt"]
            },
            "l1b": {
                "rdn": ["img", "hdr", "png", "kmz", "nc"],
                "destripedark": ["img", "hdr"],
                "destripeff": ["img", "hdr"],
                "bandmask": ["img", "hdr"],
                "ffupdate": ["img", "hdr"],
                "ffmedian": ["img", "hdr"],
                "loc": ["img", "hdr"],
                "obs": ["img", "hdr", "nc"],
                "glt": ["img", "hdr"],
                "daac": ["nc", "json"]
            },
            "l2a": {
                "rfl": ["img", "hdr", "nc", "png"],
                "rfluncert": ["img", "hdr", "nc"],
                "lbl": ["img", "hdr"],
                "lblort": ["img", "hdr"],
                "statesubs": ["img", "hdr"],
                "statesubsuncert": ["img", "hdr"],
                "radsubs": ["img", "hdr"],
                "obssubs": ["img", "hdr"],
                "locsubs": ["img", "hdr"],
                "atm": ["img", "hdr"],
                "mask": ["img", "hdr", "nc"],
                "quality": ["txt"],
            },
            "l2b": {
                "tetra": ["dir"],
                "abun": ["img", "hdr", "nc", "png"],
                "abununcert": ["img", "hdr", "nc"]
            },
            "l3": {
                "cover": ["img", "hdr"],
                "coveruncert": ["img", "hdr"]
            }
        }
        paths = {}
        for level, prod_map in product_map.items():
            product_version = self.config["product_versions"][level]
            level_data_dir = os.path.join(self.acquisition_id_dir, level)
            self.__dict__.update({level + "_data_dir": level_data_dir})
            self.dirs.append(level_data_dir)
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format + "_path"
                    prod_prefix = "_".join([self.acquisition_id,
                                            "o" + self.short_orb,
                                            "s" + self.scene,
                                            level,
                                            prod,
                                            "b" + self.config["build_num"],
                                            "v" + product_version])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(self.acquisition_id_dir, level, prod_name)
                    paths[prod_key] = prod_path
        return paths
