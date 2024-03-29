"""
pge.py includes the PGE class that handles looking up and checking out code and building its executable environment.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import subprocess

logger = logging.getLogger("emit-main")


class PGE:

    def __init__(self, conda_base, conda_env, pge_base, repo_url, version_tag, environment):
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
        self.repo_name = self.repo_url.split("/")[-1].replace(".git", "")
        self.versioned_repo_name = self.repo_name + "-" + self.version
        self.repo_dir = os.path.join(self.pge_base, self.repo_name, "")

        if conda_env is None:
            self.conda_env_name = self.repo_name
        else:
            self.conda_env_name = conda_env
        self.conda_env_dir = os.path.join(self.conda_env_base, self.conda_env_name)

        # jenkins user requires deploy keys to access repos.  These must be unique for each repo and require hostname
        # mapping.  The jenkins hostnames are configured in /home/jenkins/.ssh/config.
        if environment == "jenkins":
            self.repo_url = self.repo_url.replace("github.com", "github.com-" + self.repo_name)

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
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to clone repo with cmd: %s" % str(cmd))

    def _git_pull(self):
        cmd = ["cd", self.repo_dir, "&&", "git", "pull"]
        logger.info("Pulling code with cmd: %s" % " ".join(cmd))
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to pull code with cmd: %s" % str(cmd))

    def _repo_tag_needs_update(self):
        cmd = ["cd", self.repo_dir, "&&", "git", "symbolic-ref", "-q", "--short", "HEAD", "||",
               "git", "describe", "--tags", "--exact-match"]
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        if output.returncode != 0:
            logger.error("Failed to get current version tag or branch with cmd: %s" % str(cmd))
            raise RuntimeError(output.stderr.decode("utf-8"))
        current_tag = output.stdout.decode("utf-8").replace("\n", "")
        return True if current_tag != self.version_tag else False

    def _checkout_tag(self):
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
            output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
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
        if self.repo_name == "emit-l0edp":
            cmd = ["source", self.conda_sh_path, "&&", "conda", "activate", self.conda_env_dir,
                   "&&", "cd", self.repo_dir, "&&", "cargo", "build", "--release", "&&", "conda", "deactivate"]
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

    def checkout_repo(self):
        try:
            if not os.path.exists(self.repo_dir):
                self._clone_repo()
            if self._repo_tag_needs_update():
                self._checkout_tag()
            self._git_pull()
        except RuntimeError as e:
            print(e)

    def check_runtime_environment(self):
        if not os.path.exists(self.repo_dir):
            print("Failed to find repo at path: %s" % self.repo_dir)
            return False
        if self._repo_tag_needs_update():
            print("PGE repository needs to checkout new tag or branch")
            return False
        if not self._conda_env_exists():
            print("Conda environment for this PGE doesn't exist")
            return False
        return True

    def build_runtime_environment(self):
        try:
            new_repo = False
            if not os.path.exists(self.repo_dir):
                self._clone_repo()
                new_repo = True
            if self._repo_tag_needs_update():
                self._checkout_tag()
            self._git_pull()
            if not self._conda_env_exists():
                self._create_conda_env()
            if new_repo or self._repo_tag_needs_update():
                self._install_repo()
        except RuntimeError as e:
            # logger.info("Cleaning up directories and conda environments after running into a problem.")
            # rm_dir_cmd = ["rm", "-rf", self.repo_dir]
            # subprocess.run(rm_dir_cmd)
            # if self._conda_env_exists():
            #    rm_conda_env_cmd = ["conda", "env", "remove", "-n", self.conda_env_name]
            #    subprocess.run(rm_conda_env_cmd)
            print(e)

    def run(self, cmd, cwd=None, tmp_dir=None, env=None, use_conda_run=True):
        cwd_args = []
        if cwd:
            cwd_args = ["--cwd", cwd]
        if env is None:
            env = os.environ.copy()
        if use_conda_run is False:
            run_cmd = " ".join(cmd)
        elif self.conda_env_name.startswith("/"):
            run_cmd = " ".join([self.conda_exe, "run", "-p", self.conda_env_name] + cwd_args + cmd)
        else:
            # Otherwise, use conda run syntax
            run_cmd = " ".join([self.conda_exe, "run", "-n", self.conda_env_name] + cwd_args + cmd)
        logger.info("Running command: %s" % run_cmd)
        if tmp_dir is not None:
            with open(os.path.join(tmp_dir, "cmd.txt"), "a") as f:
                f.write("Command (using \"tmp\" directory):\n")
                f.write(run_cmd + "\n\n")
                f.write("Command (using \"error\" directory):\n")
                f.write(run_cmd.replace("/tmp/", "/error/") + "\n\n")
        output = subprocess.run(run_cmd, shell=True, capture_output=True, env=env)
        if output.returncode != 0:
            logger.error("PGE %s run command failed: %s" % (self.repo_name, output.args))
            raise RuntimeError(output.stderr.decode("utf-8"))
