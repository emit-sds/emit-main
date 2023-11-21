"""
This code contains the AcquisitionMonitor class that looks for recently modified acquisitions and creates tasks as
needed

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

from emit_main.workflow.l1a_tasks import L1ADeliver
from emit_main.workflow.l1b_tasks import L1BCalibrate, L1BRdnDeliver
from emit_main.workflow.l2a_tasks import L2AMask, L2ADeliver
from emit_main.workflow.l2b_tasks import L2BAbundance, L2BDeliver
from emit_main.workflow.l3_tasks import L3Unmix
from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class AcquisitionMonitor:

    def __init__(self, config_path, level="INFO", partition="emit", daac_ingest_queue="forward"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition
        self.daac_ingest_queue = daac_ingest_queue

        # Get workflow manager
        self.wm = WorkflowManager(config_path=config_path)

    def get_calibration_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_calibration(start=start_time, stop=stop_time, date_field=date_field,
                                                            retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"calibration tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L1BCalibrate task for acquisition {acq['acquisition_id']}")
            tasks.append(L1BCalibrate(config_path=self.config_path,
                                      acquisition_id=acq["acquisition_id"],
                                      level=self.level,
                                      partition=self.partition))

        return tasks

    def get_l2_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l2(start=start_time, stop=stop_time, date_field=date_field,
                                                   retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l2a reflectance and mask tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L2AMask task for acquisition {acq['acquisition_id']}")
            tasks.append(L2AMask(config_path=self.config_path,
                                 acquisition_id=acq["acquisition_id"],
                                 level=self.level,
                                 partition=self.partition))

        return tasks

    def get_l2b_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l2b(start=start_time, stop=stop_time, date_field=date_field,
                                                    retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l2b abundance tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L2BAbundance task for acquisition {acq['acquisition_id']}")
            tasks.append(L2BAbundance(config_path=self.config_path,
                                      acquisition_id=acq["acquisition_id"],
                                      level=self.level,
                                      partition=self.partition))

        return tasks

    def get_l3_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l3(start=start_time, stop=stop_time, date_field=date_field,
                                                   retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l3 unmix tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L3Unmix task for acquisition {acq['acquisition_id']}")
            tasks.append(L3Unmix(config_path=self.config_path,
                                 acquisition_id=acq["acquisition_id"],
                                 level=self.level,
                                 partition=self.partition))

        return tasks

    def get_l1a_delivery_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l1a_delivery(start=start_time, stop=stop_time, date_field=date_field,
                                                             retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l1a delivery tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L1ADeliver task for acquisition {acq['acquisition_id']}")
            tasks.append(L1ADeliver(config_path=self.config_path,
                                    acquisition_id=acq["acquisition_id"],
                                    level=self.level,
                                    partition=self.partition,
                                    daac_ingest_queue=self.daac_ingest_queue))

        return tasks

    def get_l1brdn_delivery_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l1brdn_delivery(start=start_time, stop=stop_time,
                                                                date_field=date_field, retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l1b radiance delivery tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L1BRdnDeliver task for acquisition {acq['acquisition_id']}")
            tasks.append(L1BRdnDeliver(config_path=self.config_path,
                                       acquisition_id=acq["acquisition_id"],
                                       level=self.level,
                                       partition=self.partition,
                                       daac_ingest_queue=self.daac_ingest_queue))

        return tasks

    def get_l2a_delivery_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l2a_delivery(start=start_time, stop=stop_time,
                                                             date_field=date_field, retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l2a reflectance delivery tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L2ADeliver task for acquisition {acq['acquisition_id']}")
            tasks.append(L2ADeliver(config_path=self.config_path,
                                    acquisition_id=acq["acquisition_id"],
                                    level=self.level,
                                    partition=self.partition,
                                    daac_ingest_queue=self.daac_ingest_queue))

        return tasks

    def get_l2b_delivery_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_l2b_delivery(start=start_time, stop=stop_time,
                                                             date_field=date_field, retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"l2b abundance delivery tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L2BDeliver task for acquisition {acq['acquisition_id']}")
            tasks.append(L2BDeliver(config_path=self.config_path,
                                    acquisition_id=acq["acquisition_id"],
                                    level=self.level,
                                    partition=self.partition,
                                    daac_ingest_queue=self.daac_ingest_queue))

        return tasks

    def get_reprocessing_tasks(self, start_time, stop_time, from_build, to_build, product_arg,
                               date_field="last_modified", retry_failed=False):
        tasks = []
        # Find acquisitions within time range that are missing DB entries
        dm = self.wm.database_manager
        acquisitions = dm.find_acquisitions_for_reprocessing(start=start_time, stop=stop_time, from_build=from_build,
                                                             to_build=to_build, product_arg=product_arg,
                                                             date_field=date_field, retry_failed=retry_failed)

        # If no results, just return empty list
        if len(acquisitions) == 0:
            logger.info(f"Did not find any acquisitions with {date_field} between {start_time} and {stop_time} needing "
                        f"reprocessing from build {from_build} to build {to_build} for product {product_arg}.")
            return tasks

        for acq in acquisitions:
            # Map tasks based on product arg
            # TODO: Add other product levels
            if product_arg == "l1bcal":
                logger.info(f"Creating L1BCalibrate task for acquisition {acq['acquisition_id']}")
                tasks.append(L1BCalibrate(config_path=self.config_path,
                                          acquisition_id=acq["acquisition_id"],
                                          level=self.level,
                                          partition=self.partition))

        return tasks
