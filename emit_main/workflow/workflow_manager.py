"""
This code contains the WorkflowManager class that handles filesystem paths

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import logging
import os

from emit_main.database.database_manager import DatabaseManager
from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.pge import PGE

logger = logging.getLogger("emit-main")


class WorkflowManager:

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

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.local_store_dir, self.instrument)
        self.environment_dir = os.path.join(self.instrument_dir, self.environment)
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.repo_dir = os.path.join(self.environment_dir, "repos")
        self.scratch_tmp_dir = os.path.join(self.local_scratch_dir, self.instrument, self.environment, "tmp")
        self.scratch_error_dir = os.path.join(self.local_scratch_dir, self.instrument, self.environment, "errors")
        self.dirs.extend([self.instrument_dir, self.environment_dir, self.data_dir, self.repo_dir,
                     self.scratch_tmp_dir, self.scratch_error_dir])

        # If we have an acquisition id, create acquisition paths
        if self.acquisition_id is not None:
            self.acquisition = Acquisition(config_path, acquisition_id=self.acquisition_id)

        if self.acquisition_id is not None:
            # Check for instrument again based on filename
            instrument_prefix = self.instrument
            if self.acquisition_id.startswith("ang"):
                instrument_prefix = "ang"
            # Get date from acquisition string
            self.date_str = self.acquisition_id[len(instrument_prefix):(8 + len(instrument_prefix))]
            self.date_dir = os.path.join(self.data_dir, self.date_str)
            self.acquisition_dir = os.path.join(self.date_dir, self.acquisition_id)
            self.dirs.extend([self.date_dir, self.acquisition_dir])

            # TODO: Set orbit and scene. Defaults below are for testing only
            dm = DatabaseManager(config_path)
            acquisition = dm.find_acquisition(self.acquisition_id)
            # Do acquisition = Acquisition(config_path, self.acquisition_id)
            if "orbit" in acquisition.keys():
                self.orbit_num = acquisition["orbit"]
            else:
                self.orbit_num = "00001"
            if "scene" in acquisition.keys():
                self.scene_num = acquisition["scene"]
            else:
                self.scene_num = "001"

            self.__dict__.update(self._build_acquisition_paths())

        # Make directories if they don't exist
        for d in self.dirs:
            if not os.path.exists(d):
                os.makedirs(d)

        # Create repository paths and PGEs based on build config
        self.pges = {}
        for repo in self.repositories:
            if "conda_env" in repo and len(repo["conda_env"]) > 0:
                conda_env = repo["conda_env"]
            else:
                conda_env = None
            if "tag" in repo and len(repo["tag"]) > 0:
                version_tag = repo["tag"]
            else:
                version_tag = None
            if "local_paths" in repo:
                local_paths = repo["local_paths"]
            else:
                local_paths = None
            pge = PGE(
                conda_base=self.conda_base_dir,
                conda_env=conda_env,
                pge_base=self.repo_dir,
                repo_url=repo["url"],
                version_tag=version_tag,
                local_paths=local_paths
            )
            self.pges[pge.repo_name] = pge

        # Add repo specific paths
        # emit-sds-l1b paths
        self.emit_sds_l1b_repo_dir = self.pges["emit-sds-l1b"].repo_dir
        self.emitrdn_exe = os.path.join(self.emit_sds_l1b_repo_dir, "emitrdn.py")

        # isofit paths
        self.isofit_repo_dir = self.pges["isofit"].repo_dir
        self.apply_oe_exe = os.path.join(self.isofit_repo_dir, "isofit/utils/apply_oe.py")

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
            },
            "l2a": {
                "rfl": ["img", "hdr"],
                "uncert": ["img", "hdr"],
                "mask": ["img", "hdr"]
            }
        }
        paths = {}
        for level, prod_map in product_map.items():
            level_data_dir = os.path.join(self.acquisition_dir, level)
            self.__dict__.update({level + "_data_dir": level_data_dir})
            self.dirs.append(level_data_dir)
            for prod, formats in prod_map.items():
                for format in formats:
                    prod_key = prod + "_" + format + "_path"
                    prod_prefix = "_".join([self.acquisition_id,
                                            "o" + self.orbit_num,
                                            "s" + self.scene_num,
                                            level,
                                            prod,
                                            "b" + self.build_num,
                                            "v" + self.processing_version])
                    prod_name = prod_prefix + "." + format
                    prod_path = os.path.join(self.acquisition_dir, level, prod_name)
                    paths[prod_key] = prod_path
        return paths

    def build_runtime_environment(self):
        for pge in self.pges.values():
            pge.build()
            if pge.repo_name == "emit-main" and pge.repo_dir not in os.getcwd():
                logger.warning("The \"emit-main\" code should be executing inside repository %s to ensure that the "
                                "correct version is running" % pge.repo_dir)

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