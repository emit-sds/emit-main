"""
This code contains the WorkflowManager class that handles filesystem paths

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import grp
import logging
import os
import pwd
import shutil
import smtplib

from email.mime.text import MIMEText

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config
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

        self.config_path = config_path
        self.acquisition_id = acquisition_id
        self.stream_path = stream_path

        # Get config properties
        self.config = Config(config_path, acquisition_id).get_dictionary()

        self.database_manager = DatabaseManager(config_path)

        # Create base directories and add to list to create directories later
        dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.ingest_dir = os.path.join(self.environment_dir, "ingest")
        self.ingest_duplicates_dir = os.path.join(self.ingest_dir, "duplicates")
        self.logs_dir = os.path.join(self.environment_dir, "logs")
        self.repos_dir = os.path.join(self.environment_dir, "repos")
        self.resources_dir = os.path.join(self.environment_dir, "resources")
        self.scratch_tmp_dir = os.path.join(self.config["local_scratch_dir"], self.config["instrument"],
                                            self.config["environment"], "tmp")
        self.scratch_error_dir = os.path.join(self.config["local_scratch_dir"], self.config["instrument"],
                                              self.config["environment"], "error")
        self.local_tmp_dir = os.path.join("/tmp", self.config["instrument"], self.config["environment"])
        dirs.extend([self.instrument_dir, self.environment_dir, self.data_dir, self.ingest_dir,
                     self.ingest_duplicates_dir, self.logs_dir, self.repos_dir, self.resources_dir,
                     self.scratch_tmp_dir, self.scratch_error_dir])

        # Make directories if they don't exist
        for d in dirs:
            if not os.path.exists(d):
                os.makedirs(d)
                self.change_group_ownership(d)

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
        for repo in self.config["repositories"]:
            if "conda_env" in repo and len(repo["conda_env"]) > 0:
                conda_env = repo["conda_env"]
            else:
                conda_env = None
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
        msg_text = f"Failed task: {task}\n\nError: {error}\n\nUser: {cur_user}\n\n" \
            f"Scratch error directory: {scratch_error_dir}"
        msg = MIMEText(msg_text)
        msg["Subject"] = f"EMIT SDS Task Failure: {task.task_family}"
        msg["From"] = sender
        msg["To"] = ", ".join(recipient_list)

        # Send the message via our own SMTP server
        s = smtplib.SMTP(self.config["smtp_host"], self.config["smtp_port"])
        s.starttls()
        s.login(self.config["email_user"], self.config["email_password"])
        s.sendmail(sender, recipient_list, msg.as_string())
        s.quit()

    def change_group_ownership(self, path):
        # Change group ownership in shared environments
        if self.config["environment"] in ["dev", "test", "ops"]:
            uid = pwd.getpwnam(pwd.getpwuid(os.getuid())[0]).pw_uid
            gid = grp.getgrnam(self.config["instrument"] + "-" + self.config["environment"]).gr_gid
            os.chown(path, uid, gid, follow_symlinks=False)

            # If this is a directory and not a symlink then apply group ownership recursively
            # if os.path.isdir(path) and not os.path.islink(path):
            #     for dirpath, dirnames, filenames in os.walk(path):
            #         for dname in dirnames:
            #             os.chown(os.path.join(dirpath, dname), uid, gid)
            #         for fname in filenames:
            #             os.chown(os.path.join(dirpath, fname), uid, gid)

    def copy(self, src, dst):
        shutil.copy2(src, dst)
        self.change_group_ownership(dst)

    def copytree(self, src, dst):
        shutil.copytree(src, dst)
        self.change_group_ownership(dst)

    def move(self, src, dst):
        shutil.move(src, dst)
        self.change_group_ownership(dst)

    def symlink(self, source, link_name):
        os.symlink(source, link_name)
        self.change_group_ownership(link_name)
