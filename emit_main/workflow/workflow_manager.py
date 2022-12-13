"""
This code contains the WorkflowManager class that handles filesystem paths

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import grp
import logging
import os
import pwd
import shutil
import smtplib
import socket

from email.mime.text import MIMEText

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config
from emit_main.workflow.acquisition import Acquisition
from emit_main.workflow.data_collection import DataCollection
from emit_main.workflow.orbit import Orbit
from emit_main.workflow.pge import PGE
from emit_main.workflow.stream import Stream

logger = logging.getLogger("emit-main")


class WorkflowManager:

    def __init__(self, config_path, acquisition_id=None, stream_path=None, dcid=None, orbit_id=None):
        """
        :param config_path: Path to config file containing environment settings
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self.config_path = config_path
        self.acquisition_id = acquisition_id
        self.stream_path = stream_path
        self.dcid = dcid
        self.orbit_id = orbit_id

        self.database_manager = DatabaseManager(config_path)

        # Initialize acquisition, stream, or data collection objects along with config dictionary
        self.config = None
        # If we have an acquisition id and acquisition exists in db, initialize acquisition
        if self.acquisition_id and self.database_manager.find_acquisition_by_id(self.acquisition_id):
            self.acquisition = Acquisition(config_path, self.acquisition_id)
            self.config = Config(config_path, self.acquisition.start_time).get_dictionary()
        else:
            self.acquisition = None

        # If we have a stream path and stream exists in db, initialize a stream object
        if self.stream_path and self.database_manager.find_stream_by_name(os.path.basename(stream_path)):
            self.stream = Stream(self.config_path, self.stream_path)
            self.config = Config(config_path, self.stream.start_time).get_dictionary()
        else:
            self.stream = None

        # If we have a DCID and the data collection exists in db, initialize data collection
        if self.dcid and self.database_manager.find_data_collection_by_id(self.dcid):
            self.data_collection = DataCollection(self.config_path, self.dcid)
            self.config = Config(config_path, self.data_collection.start_time).get_dictionary()
        else:
            self.data_collection = None

        # If we have an orbit_id and the orbit exists in db, initialize orbit
        if self.orbit_id and self.database_manager.find_orbit_by_id(self.orbit_id):
            self.orbit = Orbit(self.config_path, self.orbit_id)
            self.config = Config(config_path, self.orbit.start_time).get_dictionary()
        else:
            self.orbit = None

        # If we made it this far and haven't populated config, then get the config properties with no timestamp
        # specific fields
        if self.config is None:
            self.config = Config(config_path).get_dictionary()

        # Create base directories and add to list to create directories later
        dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.ingest_dir = os.path.join(self.environment_dir, "ingest")
        self.ingest_duplicates_dir = os.path.join(self.ingest_dir, "duplicates")
        self.ingest_errors_dir = os.path.join(self.ingest_dir, "errors")
        self.logs_dir = os.path.join(self.environment_dir, "logs")
        self.repos_dir = os.path.join(self.environment_dir, "repos")
        self.resources_dir = os.path.join(self.environment_dir, "resources")
        self.scratch_tmp_dir = os.path.join(self.config["local_scratch_dir"], self.config["instrument"],
                                            self.config["environment"], "tmp")
        self.scratch_error_dir = os.path.join(self.config["local_scratch_dir"], self.config["instrument"],
                                              self.config["environment"], "error")
        self.local_tmp_dir = os.path.join("/tmp", self.config["instrument"], self.config["environment"])
        self.planning_products_dir = os.path.join(self.data_dir, "planning_products")
        self.reconciliation_dir = os.path.join(self.data_dir, "reconciliation")

        dirs.extend([self.instrument_dir, self.environment_dir, self.data_dir, self.ingest_dir,
                     self.ingest_duplicates_dir, self.ingest_errors_dir, self.logs_dir, self.repos_dir,
                     self.resources_dir, self.scratch_tmp_dir, self.scratch_error_dir, self.planning_products_dir])

        # Make directories if they don't exist
        for d in dirs:
            self.makedirs(d)

        # Build paths for DAAC delivery on staging server
        self.daac_recon_staging_dir = os.path.join(self.config["daac_base_dir"], self.config['environment'],
                                                   "reconciliation")
        self.daac_recon_uri_base = f"https://{self.config['daac_server_external']}/emit/lpdaac/" \
                                   f"{self.config['environment']}/reconciliation/"
        self.daac_partial_dir = os.path.join(self.config["daac_base_dir"], self.config['environment'],
                                             "partial_transfers")

        # Create repository paths and PGEs based on build config
        self.pges = {}
        for repo in self.config["repositories"]:
            conda_env = None
            if "conda_env" in repo and len(repo["conda_env"]) > 0:
                if repo["conda_env"].startswith("/"):
                    conda_env = repo["conda_env"]
                elif self.config["environment"] in ("test", "ops"):
                    conda_env = repo["conda_env"] + "-" + self.config["environment"]
                else:
                    conda_env = repo["conda_env"] + "-dev"
            if "tag" in repo and len(repo["tag"]) > 0:
                version_tag = repo["tag"]
            else:
                version_tag = None
            pge = PGE(
                conda_base=self.config["conda_base_dir"],
                conda_env=conda_env,
                pge_base=self.repos_dir,
                repo_url=repo["url"],
                version_tag=version_tag,
                environment=self.config["environment"]
            )
            self.pges[pge.repo_name] = pge

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

    def send_failure_notification(self, task, error):
        # Create a text/plain message
        sender = self.config["email_sender"]
        recipient_list = self.config["email_recipient_list"]
        cur_user = pwd.getpwuid(os.geteuid()).pw_name
        scratch_error_dir = os.path.join(self.scratch_error_dir, os.path.basename(task.tmp_dir))
        msg_text = f"User: {cur_user}\n\nTimestamp: {datetime.datetime.now()}\n\nFailed task: {task}\n\n" \
            f"Error: {type(error)}: {error}\n\nScratch error directory: {scratch_error_dir}"
        msg = MIMEText(msg_text)
        msg["Subject"] = f"EMIT SDS Task Failure: {task.task_family}"
        msg["From"] = sender
        msg["To"] = ", ".join(recipient_list)

        try:
            # Send the message via our own SMTP server
            s = smtplib.SMTP(self.config["smtp_host"], self.config["smtp_port"], timeout=15)
            s.starttls()
            s.login(self.config["email_user"], self.config["email_password"])
            s.sendmail(sender, recipient_list, msg.as_string())
            s.quit()
        # except socket.timeout:
        #     logger.error(f"Timeout encountered while trying to send failure notification for task: {task}")
        except Exception as e:
            logger.error(f"An exception occurred while trying to send failure notification for task: {task}")
            logger.error(f"Exception: {e}")

    def change_group_ownership(self, path):
        # Change group ownership in shared environments
        if self.config["environment"] in ["dev", "test", "ops"]:
            uid = pwd.getpwnam(pwd.getpwuid(os.getuid())[0]).pw_uid
            gid = grp.getgrnam(self.config["instrument"] + "-" + self.config["environment"]).gr_gid
            # Only the owner of a file or directory can change the group ownership
            owner = pwd.getpwuid(os.stat(path, follow_symlinks=False).st_uid).pw_name
            current_user = pwd.getpwuid(os.getuid()).pw_name
            # Only change ownership if the desired gid is different from the current one
            if owner == current_user and gid != os.stat(path, follow_symlinks=False).st_gid:
                os.chown(path, uid, gid, follow_symlinks=False)

    def makedirs(self, d):
        # Make directory if it doesn't exist
        if not os.path.exists(d):
            try:
                os.makedirs(d)
                self.change_group_ownership(d)
            except Exception as e:
                self.print(__name__, f"Unable to make directory {d}, but proceeding anyway...")

    def copy(self, src, dst):
        # First check if the dst path exists
        if os.path.exists(dst):
            # If the dst file is owned by a different user, then delete it first.
            owner = pwd.getpwuid(os.stat(dst, follow_symlinks=False).st_uid).pw_name
            current_user = pwd.getpwuid(os.getuid()).pw_name
            if owner != current_user:
                self.print(__name__, f"Attempting to overwrite file owned by another user ({dst}). Will delete the "
                                     f"file first and then copy.")
                if os.path.isfile(dst):
                    os.remove(dst)
        # Now copy
        shutil.copy2(src, dst)
        self.change_group_ownership(dst)

    def copytree(self, src, dst):
        # First check if the dst dir exists. Right now this only impacts the tetracorder directory in l2b.
        # This is kept purposely very specific so as not to call rmtree on some other directory by mistake.
        if os.path.exists(dst) and "tetra" in os.path.basename(dst):
            # If the dst file is owned by a different user, then delete it first.
            owner = pwd.getpwuid(os.stat(dst, follow_symlinks=False).st_uid).pw_name
            current_user = pwd.getpwuid(os.getuid()).pw_name
            if owner != current_user:
                self.print(__name__, f"Attempting to overwrite directory owned by another user ({dst}). Will delete "
                                     f"the directory first and then copy.")
                if os.path.isdir(dst):
                    shutil.rmtree(dst)
        # Now copy
        shutil.copytree(src, dst)
        self.change_group_ownership(dst)

    def move(self, src, dst):
        shutil.move(src, dst)
        self.change_group_ownership(dst)

    def symlink(self, source, link_name):
        if not os.path.exists(link_name):
            os.symlink(source, link_name)
            self.change_group_ownership(link_name)

    # Use this print function inside of luigi work() functions in order to write to the slurm job.out file
    def print(self, module, msg, level="INFO"):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} {level.upper()} [{module.split('.')[-1]}]: {msg}")
