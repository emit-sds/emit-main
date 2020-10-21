"""
This code contains the FileMonitor class that watches folders to trigger activities

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import logging
import os
import subprocess
import time

from watchdog.observers import Observer
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

        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.ingest_dir, recursive=True)
        self.observer.start()
        logger.info("Observer started.")
        try:
            while True:
                time.sleep(5)
        except RuntimeError:
            self.observer.stop()
            logger.info("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Event is created, you can process it now
            logger.info("Watchdog received created event - % s." % event.src_path)
        elif event.event_type == 'modified':
            # Event is modified, you can process it now
            logger.info("Watchdog received modified event - % s." % event.src_path)