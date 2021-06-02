"""
This code contains the WorkflowManager class that handles filesystem paths

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import grp
import json
import logging
import os
import pwd

from emit_main.database.database_manager import DatabaseManager
from emit_main.util.config import Config
from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.pge import PGE
from emit_main.workflow.stream import Stream

logger = logging.getLogger("emit-main")


class WorkflowManager:

    def __init__(self, config_path, acquisition_id=None, stream_path=None):
        """
        :param config_path: Path to config file containing environment settings
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        # Update manager with properties from config file
        self.__dict__.update(Config(config_path, acquisition_id).get_properties())

        self.config_path = config_path
        self.acquisition_id = acquisition_id
        self.stream_path = stream_path
        self.database_manager = DatabaseManager(config_path)

        # Create base directories and add to list to create directories later
        dirs = []
        self.instrument_dir = os.path.join(self.local_store_dir, self.instrument)
        self.environment_dir = os.path.join(self.instrument_dir, self.environment)
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.ingest_dir = os.path.join(self.environment_dir, "ingest")
        self.ingest_duplicates_dir = os.path.join(self.ingest_dir, "duplicates")
        self.logs_dir = os.path.join(self.environment_dir, "logs")
        self.repos_dir = os.path.join(self.environment_dir, "repos")
        self.scratch_tmp_dir = os.path.join(self.local_scratch_dir, self.instrument, self.environment, "tmp")
        self.scratch_error_dir = os.path.join(self.local_scratch_dir, self.instrument, self.environment, "error")
        dirs.extend([self.instrument_dir, self.environment_dir, self.data_dir, self.ingest_dir,
                     self.ingest_duplicates_dir, self.logs_dir, self.repos_dir, self.scratch_tmp_dir,
                     self.scratch_error_dir])

        # Make directories if they don't exist
        for d in dirs:
            if not os.path.exists(d):
                os.makedirs(d)
                # Change group ownership in shared environments
                if self.environment in ["dev", "test", "ops"]:
                    uid = pwd.getpwnam(pwd.getpwuid(os.getuid())[0]).pw_uid
                    gid = grp.getgrnam(self.instrument + "-" + self.environment).gr_gid
                    os.chown(d, uid, gid)

        # If we have an acquisition id and acquisition exists in db, initialize acquisition
        if self.acquisition_id and self.database_manager.find_acquisition_by_id(self.acquisition_id):
            self.acquisition = Acquisition(config_path, self.acquisition_id)
        else:
            self.acquisition = None

        # If we have a stream path and stream exists in db, initialize a stream object
        if self.stream_path and self.database_manager.find_stream_by_name(os.path.basename(stream_path)):
            self.stream = Stream(self.config_path, self.stream_path)
        else:
            self.stream = None

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
            pge = PGE(
                conda_base=self.conda_base_dir,
                conda_env=conda_env,
                pge_base=self.repos_dir,
                repo_url=repo["url"],
                version_tag=version_tag,
                environment=self.environment
            )
            self.pges[pge.repo_name] = pge

    def _get_ancillary_file_paths(self, anc_files_config):
        if "versions" in anc_files_config:
            versions = anc_files_config["versions"]
            # Look for matching date range and update top level dictionary with those key/value pairs
            for k, v in enumerate(versions):
                pass
            # Remove "versions" and return dictionary
            del anc_files_config["versions"]
        return {}

    def checkout_repos_for_build(self):
        for pge in self.pges.values():
            pge.checkout_repo()

    def check_runtime_environment(self):
        for pge in self.pges.values():
            if pge.check_runtime_environment() is False:
                print("Checking PGE: %s" % pge.repo_name)
                return False
        return True

    def build_runtime_environment(self):
        for pge in self.pges.values():
            pge.build_runtime_environment()
            if pge.repo_name == "emit-main" and pge.repo_dir not in os.getcwd():
                logger.warning("The \"emit-main\" code should be executing inside repository %s to ensure that the "
                               "correct version is running" % pge.repo_dir)
