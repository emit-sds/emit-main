"""
This code contains the IngestMonitor class that watches the ingest folder and triggers the workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import logging
import os

from emit_main.config.config import Config
from emit_main.workflow.l0_tasks import L0StripHOSC, L0ProcessPlanningProduct, L0IngestBAD, L0Deliver
from emit_main.workflow.l1a_tasks import L1AReformatEDP, L1ADepacketizeScienceFrames

logger = logging.getLogger("emit-main")


class IngestMonitor:

    def __init__(self, config_path, level="INFO", partition="emit", pkt_format="1.3", miss_pkt_thresh=0.01,
                 test_mode=False, daac_ingest_queue="forward"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition
        self.pkt_format = pkt_format
        self.miss_pkt_thresh = miss_pkt_thresh
        self.test_mode = test_mode
        self.daac_ingest_queue = daac_ingest_queue

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
        self.wm = WorkflowManager(config_path=config_path)
        for d in self.dirs:
            self.wm.makedirs(d)

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

    def get_edp_reformatting_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find 1674 files in time range that don't have engineering products yet
        dm = self.wm.database_manager
        streams = dm.find_streams_for_edp_reformatting(start=start_time, stop=stop_time, date_field=date_field,
                                                       retry_failed=retry_failed)

        # If no results, just return empty list
        if len(streams) == 0:
            logger.info(f"Did not find any 1674 streams with {date_field} between {start_time} and {stop_time} needing "
                        f"EDP reformatting tasks. Not executing any tasks.")
            return tasks

        for stream in streams:
            stream_path = stream["products"]["l0"]["ccsds_path"]
            logger.info(f"Creating L1AReformatEDP task for path {stream_path}")
            tasks.append(L1AReformatEDP(config_path=self.config_path,
                                        stream_path=stream_path,
                                        level=self.level,
                                        partition=self.partition,
                                        pkt_format=self.pkt_format,
                                        miss_pkt_thresh=self.miss_pkt_thresh))

        return tasks

    def get_l0_delivery_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find 1675 L0 CCSDS files in time range that don't have UMM-G files yet
        dm = self.wm.database_manager
        streams = dm.find_streams_for_l0_delivery(start=start_time, stop=stop_time, date_field=date_field,
                                                  retry_failed=retry_failed)

        # If no results, just return empty list
        if len(streams) == 0:
            logger.info(f"Did not find any 1675 streams with {date_field} between {start_time} and {stop_time} needing "
                        f"L0 delivery tasks. Not executing any tasks.")
            return tasks

        for stream in streams:
            stream_path = stream["products"]["l0"]["ccsds_path"]
            logger.info(f"Creating L0Deliver task for path {stream_path}")
            tasks.append(L0Deliver(config_path=self.config_path,
                                   stream_path=stream_path,
                                   level=self.level,
                                   partition=self.partition,
                                   daac_ingest_queue=self.daac_ingest_queue))

        return tasks

    def ingest_files_by_time_range(self, start_time, stop_time):
        """
        Only process files in ingest folder within a specific datetime range
        :param start_time: Start time in format YYMMDDhhmmss
        :param stop_time: Stop time in format YYMMDDhhmmss
        """
        # TODO: Update this function when we know more about BAD and Planning Prod naming
        return self.ingest_files()

    def _ingest_file_list(self, paths):
        paths.sort(key=lambda x: os.path.getmtime(x))
        # Return luigi tasks
        tasks = []
        priority = len(paths)
        for p in paths:
            # Process HOSC files
            if p.endswith("hsc.bin"):
                if os.path.basename(p).lower().startswith("emit"):
                    apid = os.path.basename(p).split("_")[1]
                else:
                    apid = os.path.basename(p).split("_")[0]
                # Run different tasks based on apid (engineering or science). 1674 is engineering. 1675 is science.
                if apid in ("1674", "1676", "1482"):
                    logger.info(f"Creating L0StripHOSC task for path {p}")
                    tasks.append(L0StripHOSC(config_path=self.config_path,
                                             stream_path=p,
                                             level=self.level,
                                             partition=self.partition,
                                             miss_pkt_thresh=self.miss_pkt_thresh))

                # TODO: APID 1675 is currently ingested by another script to preserve order, but if you want to
                # TODO: ingest it here, then uncomment these lines and also the singleton_flag in slurm.py
                # if apid == "1675":
                #     logger.info(f"Creating L1ADepacketizeScienceFrames task for path {p} with priority {priority}")
                #     tasks.append(L1ADepacketizeScienceFrames(config_path=self.config_path,
                #                                              stream_path=p,
                #                                              level=self.level,
                #                                              partition=self.partition,
                #                                              pkt_format=self.pkt_format,
                #                                              miss_pkt_thresh=self.miss_pkt_thresh,
                #                                              test_mode=self.test_mode,
                #                                              priority=priority))
                #     priority -= 1

            # Process Planning Product files
            if p.endswith(".json"):
                logger.info(f"Creating L0ProcessPlanningProduct task for path {p}")
                tasks.append(L0ProcessPlanningProduct(config_path=self.config_path,
                                                      plan_prod_path=p,
                                                      level=self.level,
                                                      partition=self.partition,
                                                      test_mode=self.test_mode))

            # Process BAD STO files
            if p.endswith(".sto"):
                logger.info(f"Creating L0IngestBAD task for path {p}")
                tasks.append(L0IngestBAD(config_path=self.config_path,
                                         stream_path=p,
                                         level=self.level,
                                         partition=self.partition))

        return tasks
