"""
This code contains the FileMonitor class that watches folders to trigger activities

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import json
import logging
import os
import time

from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger("emit-main")


class FileMonitor:

    def __init__(self, config_path):
        """
        :param config_path: Path to config file containing environment settings
        """

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["filesystem_config"])

        # Build path for ingest folder
        self.ingest_dir = os.path.join(self.local_store_dir, self.instrument, self.environment, "ingest")

        self.observer = PollingObserver(timeout=15)

    def ingest_files(self):
        """
        Process all files in ingest folder
        """
        ingest_files = [os.path.basename(path) for path in glob.glob(os.path.join(self.ingest_dir, "*hsc.bin"))]
        self._ingest_file_list(ingest_files)

    def ingest_files_by_time_range(self, start_time_str, stop_time_str):
        """
        Only process files in ingest folder within a specific datetime range
        :param start_time_str: Start time in format YYMMDDhhmmss
        :param stop_time_str: Stop time in format YYMMDDhhmmss
        """
        matching_files = []
        ingest_files = [os.path.basename(path) for path in glob.glob(os.path.join(self.ingest_dir, "*hsc.bin"))]
        for file in ingest_files:
            tokens = file.split("_")
            file_start = tokens[2]
            file_stop = tokens[3]
            if file_start >= start_time_str and file_stop <= stop_time_str:
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
        for group in prefix_hash.values():
            if len(group.items()) == 1:
                for file in group.keys():
                    # Run workflow
                    logger.info("Running workflow on %s" % file)
            else:
                max_file = [key for (key, value) in group.items() if value == max(group.values())][0]
                for file in group.keys():
                    if file == max_file:
                        logger.info("Running workflow on max_file %s" % file)
                    else:
                        logger.info("Archiving duplicate file %s" % file)

    def run_observer(self):
        event_handler = IngestHandler()
        self.observer.schedule(event_handler, self.ingest_dir, recursive=True)
        self.observer.start()
        logger.info("Observer started.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("Observer Stopped")

        self.observer.join()


class IngestHandler(FileSystemEventHandler):

    @staticmethod
    def on_created(event):
        logger.info("Watchdog received created event - % s." % event.src_path)


    @staticmethod
    def on_modified(event):
        logger.info("Watchdog received modified event - % s." % event.src_path)
        # TODO: Move this code to on_created section
        # Check to see if there's another file with same start/stop.  If so, archive smaller file and process
        # larger one