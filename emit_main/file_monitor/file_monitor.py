"""
This code contains the FileMonitor class that watches folders to trigger activities

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import json
import logging
import os

from emit_main.workflow.l1a_tasks import *

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

        self.config_path = os.path.abspath(config_path)
        # Build path for ingest folder
        self.ingest_dir = os.path.join(self.local_store_dir, self.instrument, self.environment, "ingest")
        self.logs_dir = os.path.join(self.local_store_dir, self.instrument, self.environment, "logs")
        # Build luigi logging.conf path
        self.luigi_logging_conf = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "workflow", "luigi",
                                                               "logging.conf"))

    def ingest_files(self, dry_run=False):
        """
        Process all files in ingest folder
        """
        ingest_files = [os.path.basename(path) for path in glob.glob(os.path.join(self.ingest_dir, "*hsc.bin"))]
        return self._ingest_file_list(ingest_files, dry_run=dry_run)

    def ingest_files_by_time_range(self, start_time, stop_time, dry_run=False):
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
        return self._ingest_file_list(matching_files, dry_run=dry_run)

    def _ingest_file_list(self, files, dry_run=False):
        # Group files by prefix to find matching time ranges
        prefix_hash = {}
        for file in files:
            file_prefix = file[:35]
            if file_prefix not in prefix_hash.keys():
                prefix_hash[file_prefix] = {file: os.path.getsize(os.path.join(self.ingest_dir, file))}
            else:
                prefix_hash[file_prefix].update({file: os.path.getsize(os.path.join(self.ingest_dir, file))})
        # Find paths to ingest by removing duplicates
        paths = []
        for group in prefix_hash.values():
            if len(group.items()) == 1:
                for file in group.keys():
                    # Run workflow
                    path = os.path.join(self.ingest_dir, file)
                    logger.info("Adding ingest path: %s" % path)
                    paths.append(path)
            else:
                max_file = [key for (key, value) in group.items() if value == max(group.values())][0]
                for file in group.keys():
                    if file == max_file:
                        path = os.path.join(self.ingest_dir, file)
                        logger.info("Adding ingest path (largest file for this two hour window): %s" % path)
                        paths.append(path)
                    else:
                        path = os.path.join(self.ingest_dir, file)
                        logger.info("Archiving ingest path (duplicate smaller file for this two hour window): %s"
                                    % path)

        if dry_run:
            return paths

        # Create luigi tasks and execute
        tasks = []
        # TODO: Change task based on APID
        for p in paths:
            tasks.append(L1AReformatEDP(config_path=self.config_path, stream_path=p))

        return luigi.build(tasks, workers=4, local_scheduler=self.luigi_local_scheduler,
                    logging_conf_file=self.luigi_logging_conf)
