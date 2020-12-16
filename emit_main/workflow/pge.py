"""
pge.py includes the PGE class that handles looking up and checking out code and building its executable environment.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import subprocess

logger = logging.getLogger("emit-main")


class PGE:

    def __init__(self, conda_base, conda_env, pge_base, repo_url, version_tag, local_paths):
        # conda_env_base is the top level "envs" folder, e.g. ~/anaconda3/envs
        self.conda_env_base = os.path.join(conda_base, "envs")
        self.conda_sh_path = os.path.join(conda_base, "etc/profile.d/conda.sh")
        self.conda_exe = os.path.join(conda_base, "bin/conda")
        self.pge_base = pge_base
        self.repo_url = repo_url
        self.version_tag = version_tag
        # Remove "v" from version tag to get version
        if self.version_tag.startswith("v"):
            self.version = self.version_tag[1:]
        else:
            self.version = self.version_tag

        self.repo_account = self._get_repo_account()
        self.repo_name = self.repo_url.split("/")[-1].replace(".git","")
        self.versioned_repo_name = self.repo_name + "-" + self.version
        self.repo_dir = os.path.join(self.pge_base, self.repo_name, "")

        if conda_env is None:
            self.conda_env_name = self.repo_name
        else:
            self.conda_env_name = conda_env
        self.conda_env_dir = os.path.join(self.conda_env_base, self.conda_env_name)

        if local_paths is not None:
            abs_local_paths = {}
            for k,v in local_paths.items():
                abs_local_paths[k] = os.path.join(self.repo_dir, v)
            self.__dict__.update(abs_local_paths)

    def _get_repo_account(self):
        if self.repo_url.startswith("https"):
            return self.repo_url.split("/")[-2]
        elif self.repo_url.startswith("git@"):
            return self.repo_url.split("/")[-2].split(":")[-1]
        else:
            return ""

    def _clone_repo(self):
        cmd = ["git", "clone", self.repo_url, self.repo_dir]
        logger.info("Cloning repo with cmd: %s" % " ".join(cmd))
        output = subprocess.run(cmd)
        if output.returncode != 0:
            raise RuntimeError("Failed to clone repo with cmd: %s" % str(cmd))

    def _checkout_tag(self):
        cmd = ["cd", self.repo_dir, "&&", "git", "symbolic-ref", "-q", "--short", "HEAD", "||",
              "git", "describe", "--tags", "--exact-match"]
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        if output.returncode != 0:
            logger.error("Failed to get current version tag or branch with cmd: %s" % str(cmd))
            raise RuntimeError(output.stderr.decode("utf-8"))
        current_tag = output.stdout.decode("utf-8").replace("\n", "")
        if current_tag != self.version_tag:
            cmd = ["cd", self.repo_dir, "&&", "git", "fetch", "--all", "&&", "git", "checkout", self.version_tag]
            logger.info("Checking out version tag or branch with cmd: %s" % " ".join(cmd))
            output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
            if output.returncode != 0:
                logger.error("Failed to checkout version tag or branch with cmd: %s" % str(cmd))
                raise RuntimeError(output.stderr.decode("utf-8"))

    def _conda_env_exists(self):
        cmd = [self.conda_exe, "env", "list", "|", "grep", self.conda_env_dir]
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        if output.returncode == 0:
            return True
        else:
            return False

    def _create_conda_env(self):
        conda_env_yml_path = os.path.join(self.repo_dir, "environment.yml")
        if os.path.exists(conda_env_yml_path):
            cmd = [self.conda_exe, "env", "create", "-f", conda_env_yml_path, "-n", self.conda_env_name]
            logger.info("Creating conda env with cmd: %s" % " ".join(cmd))
            output = subprocess.run(cmd, shell=True, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError("Failed to create conda env with cmd: %s" % str(cmd))

    def _install_repo(self):
        # Run install script if it exists
        install_script_path = os.path.join(self.repo_dir, "install.sh")
        if os.path.exists(install_script_path):
            cmd = ["source", self.conda_sh_path, "&&", "conda", "activate", self.conda_env_dir,
                   "&&", "cd", self.repo_dir, "&&", "./install.sh", "&&", "conda", "deactivate"]
            logger.info("Installing repo with cmd: %s" % " ".join(cmd))
            output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError("Failed to install repo with cmd: %s" % str(cmd))

    def _add_imports(self):
        for repo in self.imports:
            # Run pip install for imported package
            repo_dir = os.path.join(self.pge_base, repo)
            cmd = ["source", self.conda_sh_path, "&&", "conda", "activate", self.conda_env_dir,
                   "&&", "cd", repo_dir, "&&", "pip install -e .", "&&", "conda", "deactivate"]
            logger.info("Adding package %s to conda env %s" % (repo_dir, self.conda_env_dir))
            output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError("Failed to import repo with cmd: %s" % str(cmd))

    def build(self):
        try:
            if not os.path.exists(self.repo_dir):
                self._clone_repo()
            self._checkout_tag()
            if not self._conda_env_exists():
                self._create_conda_env()
            self._install_repo()
        except RuntimeError as e:
            logger.info("Cleaning up directories and conda environments after running into a problem.")
            rm_dir_cmd = ["rm", "-rf", self.repo_dir]
            subprocess.run(rm_dir_cmd)
#            if self._conda_env_exists():
#                rm_conda_env_cmd = ["conda", "env", "remove", "-n", self.conda_env_name]
#                subprocess.run(rm_conda_env_cmd)
            print(e)

    def run(self, cmd, cwd=None, env=None):
        cwd_args = []
        if cwd:
            cwd_args = ["--cwd", cwd]
        if env is None:
            env = os.environ.copy()
        conda_run_cmd = " ".join([self.conda_exe, "run", "-n", self.conda_env_name] + cwd_args + cmd)
        logging.info("Running command: %s" % conda_run_cmd)
        output = subprocess.run(conda_run_cmd, shell=True, capture_output=True, env=env)
        if output.returncode != 0:
            logger.error("PGE %s run command failed: %s" % (self.repo_name, output.args))
            raise RuntimeError(output.stderr.decode("utf-8"))