"""
This code contains the FileMonitor class that watches folders to trigger activities

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import json
import logging
import os

from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class FileMonitor:

    def __init__(self, config_path):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = config_path
        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["filesystem_config"])
            self.__dict__.update(config["build_config"])

        # Build path for ingest folder
        self.ingest_dir = os.path.join(self.local_store_dir, self.instrument, self.environment, "ingest")

    def ingest_files(self):
        """
        Process all files in ingest folder
        """
        ingest_files = [os.path.basename(path) for path in glob.glob(os.path.join(self.ingest_dir, "*hsc.bin"))]
        self._ingest_file_list(ingest_files)

    def ingest_files_by_time_range(self, start_time, stop_time):
        """
        Only process files in ingest folder within a specific datetime range
        :param start_time: Start time in format YYMMDDhhmmss
        :param stop_time: Stop time in format YYMMDDhhmmss
        """
        matching_files = []
        ingest_files = [os.path.basename(path) for path in glob.glob(os.path.join(self.ingest_dir, "*hsc.bin"))]
        for file in ingest_files:
            tokens = file.split("_")
            file_start = tokens[2]
            file_stop = tokens[3]
            if file_start >= start_time and file_stop <= stop_time:
                matching_files.append(file)
        self._ingest_file_list(matching_files)

    def _ingest_file_list(self, files):
        # Group files by prefix to find matching time ranges
        prefix_hash = {}
        for file in files:
            file_prefix = file[:35]
            if file_prefix not in prefix_hash.keys():
                prefix_hash[file_prefix] = {file: os.path.getsize(os.path.join(self.ingest_dir, file))}
            else:
                prefix_hash[file_prefix].update({file: os.path.getsize(os.path.join(self.ingest_dir, file))})
        # Get run command path
        wm = WorkflowManager(self.config_path)
#        wm.build_runtime_environment()
        pge = wm.pges["emit-main"]
        run_workflow_exe = os.path.join(pge.repo_dir, "emit_main", "run_workflow.py")
        # TODO: Change "cmd" below based on APID
        for group in prefix_hash.values():
            if len(group.items()) == 1:
                for file in group.keys():
                    # Run workflow
                    path = os.path.join(self.ingest_dir, file)
                    logger.info("Running workflow on %s" % path)
                    cmd = ["python", run_workflow_exe, "-c", self.config_path, "-s", path, "-p", "l1aeng"]
                    pge.run(cmd)
            else:
                max_file = [key for (key, value) in group.items() if value == max(group.values())][0]
                for file in group.keys():
                    if file == max_file:
                        logger.info("Running workflow on max_file %s" % file)
                        cmd = ["python", run_workflow_exe, "-c", self.config_path, "-s", file, "-p", "l1aeng"]
                        pge.run(cmd)
                    else:
                        logger.info("Archiving duplicate smaller file %s" % file)