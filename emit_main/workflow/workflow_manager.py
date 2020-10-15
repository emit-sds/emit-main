"""
This code contains the WorkflowManager class that handles filesystem paths

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import logging
import os
import subprocess

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

        self.config_path = config_path
        self.acquisition_id = acquisition_id

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["filesystem_config"])
            self.__dict__.update(config["build_config"])

        # Create base directories and add to list to create directories later
        dirs = []
        self.instrument_dir = os.path.join(self.local_store_dir, self.instrument)
        self.environment_dir = os.path.join(self.instrument_dir, self.environment)
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.repo_dir = os.path.join(self.environment_dir, "repos")
        self.scratch_tmp_dir = os.path.join(self.local_scratch_dir, self.instrument, self.environment, "tmp")
        self.scratch_error_dir = os.path.join(self.local_scratch_dir, self.instrument, self.environment, "error")
        dirs.extend([self.instrument_dir, self.environment_dir, self.data_dir, self.repo_dir,
                     self.scratch_tmp_dir, self.scratch_error_dir])

        # Make directories if they don't exist
        for d in dirs:
            if not os.path.exists(d):
                os.makedirs(d)

        # If we have an acquisition id and acquisition exists in db, initialize acquisition
        self.database_manager = DatabaseManager(config_path)
        if self.acquisition_id and self.database_manager.find_acquisition_by_id(self.acquisition_id):
            self.acquisition = Acquisition(config_path, self.acquisition_id)
        else:
            self.acquisition = None

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

    def run(self, cmd):
        logging.info("Running command: %s" % " ".join(cmd))
        output = subprocess.run(cmd, shell=True, capture_output=True)
        if output.returncode != 0:
            logger.error("WorkflowManager run command failed: %s" % output.args)
            raise RuntimeError(output.stderr.decode("utf-8"))