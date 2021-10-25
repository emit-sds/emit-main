"""
This code contains the FileMonitor class that watches folders to trigger activities

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import logging
import os
import shutil

from emit_main.config.config import Config
from emit_main.workflow.l1a_tasks import L1AReformatEDP, L1ADepacketizeScienceFrames

logger = logging.getLogger("emit-main")


class FileMonitor:

    def __init__(self, config_path, level="INFO", partition="emit", miss_pkt_thresh=0.1, test_mode=False):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition
        self.miss_pkt_thresh = miss_pkt_thresh
        self.test_mode = test_mode

        # Get config properties
        self.config = Config(config_path).get_dictionary()

        # Build path for ingest folder
        self.ingest_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"],
                                       self.config["environment"], "ingest")
        self.ingest_duplicates_dir = os.path.join(self.ingest_dir, "duplicates")
        self.ingest_errors_dir = os.path.join(self.ingest_dir, "errors")
        self.logs_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"],
                                     self.config["environment"], "logs")
        self.dirs = [self.ingest_dir, self.ingest_duplicates_dir, self.ingest_errors_dir, self.logs_dir]

        # Make directories if they don't exist
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=config_path)
        for d in self.dirs:
            wm.makedirs(d)

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
                        if not dry_run:
                            path = os.path.join(self.ingest_dir, file)
                            logger.info("Moving smaller file for this two hour window to 'duplicates' subfolder: %s"
                                        % path)
                            base_name = os.path.basename(path)
                            shutil.move(path, os.path.join(self.ingest_duplicates_dir, base_name))

        paths.sort()
        if dry_run:
            return paths

        # Return luigi tasks
        tasks = []
        for p in paths:
            apid = os.path.basename(p).split("_")[1]
            # Run different tasks based on apid (engineering or science). 1674 is engineering. 1675 is science.
            if apid == "1674" or apid == "1482":
                tasks.append(L1AReformatEDP(config_path=self.config_path,
                                            stream_path=p,
                                            level=self.level,
                                            partition=self.partition,
                                            miss_pkt_thresh=self.miss_pkt_thresh))
            # Temporarily remove science data processing until it is ready
            if apid == "1675":
                tasks.append(L1ADepacketizeScienceFrames(config_path=self.config_path,
                                                         stream_path=p,
                                                         level=self.level,
                                                         partition=self.partition,
                                                         miss_pkt_thresh=self.miss_pkt_thresh,
                                                         test_mode=self.test_mode))
        return tasks
