"""
pge.py includes the PGE class that handles looking up and checking out code and building its executable environment.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import subprocess

logger = logging.getLogger("emit-workflow")


class PGE:

    def __init__(self, conda_base, pge_base, repo_url, version_tag=None):
        # conda_env_base is the top level "envs" folder, e.g. ~/anaconda3/envs
        self.conda_env_base = os.path.join(conda_base, "envs")
        self.conda_sh_path = os.path.join(conda_base, "etc/profile.d/conda.sh")
        self.pge_base = pge_base
        self.repo_url = repo_url
        if version_tag is None:
            self._get_version_tag()
        else:
            self.version_tag = version_tag
        # Remove "v" from version tag to get version
        if self.version_tag.startswith("v"):
            self.version = self.version_tag[1:]
        else:
            self.version = self.version_tag
        self.repo_account = self._get_repo_account()
        self.repo_name = self.repo_url.split("/")[-1].replace(".git","")
        self.versioned_repo_name = self.repo_name + "-" + self.version
        self.repo_dir = os.path.join(self.pge_base, self.versioned_repo_name, "")
        self.conda_env_name = self.versioned_repo_name
        self.conda_env_dir = os.path.join(self.conda_env_base, self.conda_env_name)

    def _get_version_tag(self):
        # TODO: Get latest release from repo
        return ""

    def _get_repo_account(self):
        if self.repo_url.startswith("https"):
            return self.repo_url.split("/")[-2]
        elif self.repo_url.startswith("git@"):
            return self.repo_url.split("/")[-2].split(":")[-1]
        else:
            return ""

    def _clone_repo(self):
        cmd = ["git", "clone", "-b", self.version_tag, self.repo_url, self.repo_dir]
        logging.info("Cloning repo with cmd: %s" % " ".join(cmd))
        output = subprocess.run(cmd)
        print(str(output))
        print(output.returncode)
        if output.returncode != 0:
            raise RuntimeError("Failed to clone repo with cmd: %s" % str(cmd))

    def _conda_env_exists(self):
        cmd = ["conda", "env", "list", "|", "grep", self.conda_env_name]
        output = subprocess.run(" ".join(cmd), shell=True)
        if output.returncode == 0:
            return True
        else:
            return False

    def _create_conda_env(self):
        conda_env_yml_path = os.path.join(self.repo_dir, "environment.yml")
        if os.path.exists(conda_env_yml_path):
            cmd = ["conda", "env", "create", "-f", conda_env_yml_path, "-n", self.conda_env_name]
            logging.info("Creating conda env with cmd: %s" % " ".join(cmd))
            output = subprocess.run(cmd)
            if output.returncode != 0:
                raise RuntimeError("Failed to create conda env with cmd: %s" % str(cmd))

    def _install_repo(self):
        # Run install script if it exists
        install_script_path = os.path.join(self.repo_dir, "install.sh")
        if os.path.exists(install_script_path):
            cmd = ["source", self.conda_sh_path, "&&", "conda", "activate", self.conda_env_dir,
                   "&&", "cd", self.repo_dir, "&&", "./install.sh", "&&", "conda", "deactivate"]
            logging.info("Installing repo with cmd: %s" % " ".join(cmd))
            output = subprocess.run(" ".join(cmd), shell=True)
            if output.returncode != 0:
                raise RuntimeError("Failed to install repo with cmd: %s" % str(cmd))

    def build(self):
        try:
            if not os.path.exists(self.repo_dir):
                self._clone_repo()
                if not self._conda_env_exists():
                    self._create_conda_env()
                self._install_repo()
        except RuntimeError as e:
            logging.info("Cleaning up directories and conda environments after running into a problem.")
            rm_dir_cmd = ["rm", "-rf", self.repo_dir]
            subprocess.run(rm_dir_cmd)
            if self._conda_env_exists():
                rm_conda_env_cmd = ["conda", "env", "remove", "-n", self.conda_env_name]
                subprocess.run(rm_conda_env_cmd)
            print(e)

    def run(self, cmd):
        activate_cmd = ["source", self.conda_sh_path, "&&", "conda", "activate", self.conda_env_name, "&&"]
        deactivate_cmd = ["&&", "conda", "deactivate"]
        exec_cmd = " ".join(activate_cmd + cmd + deactivate_cmd)
        output = subprocess.run(exec_cmd, shell=True, capture_output=True)
        if output.returncode !=0:
            raise RuntimeError("Output of run command: %s" % str(output))

