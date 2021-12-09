"""
This code contains the IngestMonitor class that watches the ingest folder and triggers the workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import logging
import os

from emit_main.config.config import Config
from emit_main.workflow.l0_tasks import L0ProcessPlanningProduct, L0IngestBAD
from emit_main.workflow.l1a_tasks import L1AReformatEDP, L1ADepacketizeScienceFrames

logger = logging.getLogger("emit-main")


class IngestMonitor:

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

    def ingest_files(self):
        """
        Process all files in ingest folder
        """
        matches = [os.path.join(self.ingest_dir, m) for m in ("*hsc.bin", "*.json", "*.sto")]
        paths = []
        for m in matches:
            paths += glob.glob(m)
        logger.info(f"Found paths to ingest: {paths}")
        return self._ingest_file_list(paths)

    def ingest_files_by_time_range(self, start_time, stop_time):
        """
        Only process files in ingest folder within a specific datetime range
        :param start_time: Start time in format YYMMDDhhmmss
        :param stop_time: Stop time in format YYMMDDhhmmss
        """
        # TODO: Update this function when we know more about BAD and Planning Prod naming
        return self.ingest_files()

    def _ingest_file_list(self, paths):
        paths.sort()
        # Return luigi tasks
        tasks = []
        for p in paths:
            # Process HOSC files
            if p.endswith("hsc.bin"):
                apid = os.path.basename(p).split("_")[1]
                # Run different tasks based on apid (engineering or science). 1674 is engineering. 1675 is science.
                if apid == "1674" or apid == "1482":
                    logger.info(f"Creating L1AReformatEDP task for path {p}")
                    tasks.append(L1AReformatEDP(config_path=self.config_path,
                                                stream_path=p,
                                                level=self.level,
                                                partition=self.partition,
                                                miss_pkt_thresh=self.miss_pkt_thresh))
                if apid == "1675":
                    logger.info(f"Creating L1ADepacketizeScienceFrames task for path {p}")
                    tasks.append(L1ADepacketizeScienceFrames(config_path=self.config_path,
                                                             stream_path=p,
                                                             level=self.level,
                                                             partition=self.partition,
                                                             miss_pkt_thresh=self.miss_pkt_thresh,
                                                             test_mode=self.test_mode))
            # Process Planning Product files
            if p.endswith(".json"):
                logger.info(f"Creating L0ProcessPlanningProduct task for path {p}")
                tasks.append(L0ProcessPlanningProduct(config_path=self.config_path,
                                                      plan_prod_path=p,
                                                      level=self.level,
                                                      partition=self.partition))

            # Process BAD STO files
            if p.endswith(".sto"):
                logger.info(f"Creating L0IngestBAD task for path {p}")
                tasks.append(L0IngestBAD(config_path=self.config_path,
                                         stream_path=p,
                                         level=self.level,
                                         partition=self.partition))

        return tasks
