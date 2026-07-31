"""
This code contains the Acquisition class that manages acquisitions and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

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

        # Read metadata from db and get config properties
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_acquisition_by_id(self.acquisition_id)
        self.config = Config(config_path, self.metadata["start_time"]).get_dictionary()
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

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
            self.raw_granule_ur = f"EMIT_L1A_RAW_0{self.config['prod_versions']['l1a']}_{daac_start_time_str}_{self.orbit}_{self.daac_scene}"
            self.rdn_granule_ur = f"EMIT_L1B_RAD_0{self.config['prod_versions']['l1b']}_{daac_start_time_str}"
            self.obs_granule_ur = f"EMIT_L1B_OBS_0{self.config['prod_versions']['l1b']}_{daac_start_time_str}"
            self.rfl_granule_ur = f"EMIT_L2A_RFL_0{self.config['prod_versions']['l2a']}_{daac_start_time_str}"
            self.rfluncert_granule_ur = f"EMIT_L2A_RFLUNCERT_0{self.config['prod_versions']['l2a']}_{daac_start_time_str}"
            self.maskTf_granule_ur = f"EMIT_L2A_MASK_0{self.config['prod_versions']['mask']}_{daac_start_time_str}"
            self.min_granule_ur = f"EMIT_L2B_MIN_0{self.config['prod_versions']['l2b']}_{daac_start_time_str}"
            self.minuncert_granule_ur = f"EMIT_L2B_MINUNCERT_0{self.config['prod_versions']['l2b']}_{daac_start_time_str}"
            self.ch4_granule_ur = f"EMIT_L2B_CH4ENH_0{self.config['prod_versions']['ch4']}_{daac_start_time_str}"
            self.ch4uncert_granule_ur = f"EMIT_L2B_CH4UNCERT_0{self.config['prod_versions']['ch4']}_{daac_start_time_str}"
            self.ch4sens_granule_ur = f"EMIT_L2B_CH4SENS_0{self.config['prod_versions']['ch4']}_{daac_start_time_str}"
            self.co2_granule_ur = f"EMIT_L2B_CO2ENH_0{self.config['prod_versions']['co2']}_{daac_start_time_str}"
            self.co2uncert_granule_ur = f"EMIT_L2B_CO2UNCERT_0{self.config['prod_versions']['co2']}_{daac_start_time_str}"
            self.co2sens_granule_ur = f"EMIT_L2B_CO2SENS_0{self.config['prod_versions']['co2']}_{daac_start_time_str}"
            self.frcov_granule_ur = f"EMIT_L2B_FRCOV_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovqc_granule_ur = f"EMIT_L2B_FRCOVQC_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovpv_granule_ur = f"EMIT_L2B_FRCOVPV_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovpvunc_granule_ur = f"EMIT_L2B_FRCOVPVUNC_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovnpv_granule_ur = f"EMIT_L2B_FRCOVNPV_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovnpvunc_granule_ur = f"EMIT_L2B_FRCOVNPVUNC_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovbare_granule_ur = f"EMIT_L2B_FRCOVBARE_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.frcovbareunc_granule_ur = f"EMIT_L2B_FRCOVBAREUNC_0{self.config['prod_versions']['frcov']}_{daac_start_time_str}"
            self.l3rfl_granule_ur = f"EMIT_L3_RFL_0{self.config['prod_versions']['l3rfl']}_{daac_start_time_str}"
            self.l3rfluncert_granule_ur = f"EMIT_L3_RFLUNCERT_0{self.config['prod_versions']['l3rfl']}_{daac_start_time_str}"
            self.l3obs_granule_ur = f"EMIT_L3_OBS_0{self.config['prod_versions']['l3rfl']}_{daac_start_time_str}"

        self.daac_staging_dir = os.path.join(self.config["daac_base_dir"], wm.config['environment'], "products", self.start_time.strftime("%Y%m%d"))
        self.daac_uri_base = f"https://{self.config['daac_server_external']}/emit/lpdaac/{wm.config['environment']}/products/{self.start_time.strftime('%Y%m%d')}/"
        self.daac_partial_dir = os.path.join(self.config["daac_base_dir"], wm.config['environment'], "partial_transfers")
        self.aws_staging_dir = os.path.join(self.config["aws_s3_base_dir"], wm.config['environment'], "products", self.start_time.strftime("%Y%m%d"))
        self.aws_s3_uri_base = f"s3://{self.config['aws_s3_bucket']}{self.aws_staging_dir}/"

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
        if "products" not in self.metadata:
            self.metadata["products"] = {}
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}
        if self.config["prod_versions"]["l1a"] not in self.metadata["products"]["l1a"]:
            self.metadata["products"]["l1a"][self.config["prod_versions"]["l1a"]] = {}
        if "l1b" not in self.metadata["products"]:
            self.metadata["products"]["l1b"] = {}
        if self.config["prod_versions"]["l1b"] not in self.metadata["products"]["l1b"]:
            self.metadata["products"]["l1b"][self.config["prod_versions"]["l1b"]] = {}
        else:
            mean_solar_zenith = self.metadata["products"]["l1b"].get(self.config["prod_versions"]["l1b"], {}).get("obs", {}).get("band_means", {}).get("solar_zenith")
            mean_solar_azimuth= self.metadata["products"]["l1b"].get(self.config["prod_versions"]["l1b"], {}).get("obs", {}).get("band_means", {}).get("solar_azimuth")
            if mean_solar_zenith:
                self.mean_solar_zenith = mean_solar_zenith
            if mean_solar_azimuth:
                self.mean_solar_azimuth = mean_solar_azimuth
        if "l2a" not in self.metadata["products"]:
            self.metadata["products"]["l2a"] = {}
        if self.config["prod_versions"]["l2a"] not in self.metadata["products"]["l2a"]:
            self.metadata["products"]["l2a"][self.config["prod_versions"]["l2a"]] = {}
        if "l2b" not in self.metadata["products"]:
            self.metadata["products"]["l2b"] = {}
        if self.config["prod_versions"]["l2b"] not in self.metadata["products"]["l2b"]:
            self.metadata["products"]["l2b"][self.config["prod_versions"]["l2b"]] = {}
        if "ch4" not in self.metadata["products"]:
            self.metadata["products"]["ch4"] = {}
        if self.config["prod_versions"]["ch4"] not in self.metadata["products"]["ch4"]:
            self.metadata["products"]["ch4"][self.config["prod_versions"]["ch4"]] = {}
        if "co2" not in self.metadata["products"]:
            self.metadata["products"]["co2"] = {}
        if self.config["prod_versions"]["co2"] not in self.metadata["products"]["co2"]:
            self.metadata["products"]["co2"][self.config["prod_versions"]["co2"]] = {}
        if "mask" not in self.metadata["products"]:
            self.metadata["products"]["mask"] = {}
        if self.config["prod_versions"]["mask"] not in self.metadata["products"]["mask"]:
            self.metadata["products"]["mask"][self.config["prod_versions"]["mask"]] = {}
        if "frcov" not in self.metadata["products"]:
            self.metadata["products"]["frcov"] = {}
        if self.config["prod_versions"]["frcov"] not in self.metadata["products"]["frcov"]:
            self.metadata["products"]["frcov"][self.config["prod_versions"]["frcov"]] = {}
        if "l3rfl" not in self.metadata["products"]:
            self.metadata["products"]["l3rfl"] = {}
        if self.config["prod_versions"]["l3rfl"] not in self.metadata["products"]["l3rfl"]:
            self.metadata["products"]["l3rfl"][self.config["prod_versions"]["l3rfl"]] = {}
            
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
                "state": ["img", "hdr"],
                "quality": ["txt"],
            },
            "mask": {
                "maskTf": ["img", "hdr", "nc", "png"],  
            },
            "l2b": {
                "tetra": ["dir"],
                "min": ["img", "hdr", "nc", "png"],
                "minuncert": ["img", "hdr", "nc"]
            },
            "ch4": {
                "targetch4": ["txt"],
                "ch4": ["img","hdr"],
                "ortch4": ["tif","png"],
                "sensch4": ["img","hdr"],
                "ortsensch4": ["tif",],
                "uncertch4": ["img","hdr"],
                "ortuncertch4": ["tif"],
            },
            "co2": {
                "targetco2": ["txt"],
                "co2": ["img","hdr"],
                "ortco2": ["tif","png"],
                "sensco2": ["img","hdr"],
                "ortsensco2": ["tif"],
                "uncertco2": ["img","hdr"],
                "ortuncertco2": ["tif"],
            },
            "frcov": {
                "frcov": ["img", "hdr", "png"],
                "frcovuncert": ["img", "hdr"],
                "frcovqc": ["tif"],
                "frcovbare": ["tif"],
                "frcovbareunc": ["tif"],
                "frcovpv": ["tif"],
                "frcovpvunc": ["tif"],
                "frcovnpv": ["tif"],
                "frcovnpvunc": ["tif"],      
            },
            "l3rfl": {
                "l3rfl": ["nc", "png"],
                "l3rfluncert": ["nc"],
                "l3obs": ["nc"],

            },
        }
        paths = {}
        for prod_group, prod_map in product_map.items():
            # Set the data directory for the prod group (l1a, l1b, ch4, frcov, etc.)
            prod_group_data_dir = os.path.join(self.acquisition_id_dir, prod_group)
            self.__dict__.update({prod_group + "_data_dir": prod_group_data_dir})
            self.dirs.append(prod_group_data_dir)
            product_version = self.config["prod_versions"][prod_group]
            if prod_group in ["mask"]:
                prod_group = "l2a"
            if prod_group in ["co2","ch4", "frcov"]:
                prod_group = "l2b"
            if prod_group in ["l3rfl"]:
                prod_group = "l3"
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format + "_path"
                    # L1A products before the v2 cutover date have the old file naming schema 
                    if prod_group == "l1a" and self.start_time < self.config["v2_cutover_date"]:
                        prod_prefix = "_".join([self.acquisition_id,
                                                "o" + self.short_orb,
                                                "s" + self.scene,
                                                prod_group,
                                                prod,
                                                "b0106",
                                                "v" + product_version])
                    else:
                        if prod.startswith("l3"):
                            prod_short = prod.replace("l3", "")
                        else:
                            prod_short = prod
                        prod_prefix = "_".join([self.acquisition_id,
                                                prod_group,
                                                prod_short,
                                                "v" + product_version])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(prod_group_data_dir, prod_name)
                    paths[prod_key] = prod_path
        return paths
