"""
This code contains the AcquisitionMonitor class that looks for recently modified acquisitions and creates tasks as
needed

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

from emit_main.workflow.l1b_tasks import L1BCalibrate
from emit_main.workflow.l2b_tasks import L2BAbundance
from emit_main.workflow.l3_tasks import L3Unmix
from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class AcquisitionMonitor:

    def __init__(self, config_path, level="INFO", partition="emit"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition

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
                        f"MESMA tasks. Not executing any tasks.")
            return tasks

        for acq in acquisitions:
            logger.info(f"Creating L2BAbundance task for acquisition {acq['acquisition_id']}")
            tasks.append(L2BAbundance(config_path=self.config_path,
                                      acquisition_id=acq["acquisition_id"],
                                      level=self.level,
                                      partition=self.partition))
            logger.info(f"Creating L3Unmix task for acquisition {acq['acquisition_id']}")
            tasks.append(L3Unmix(config_path=self.config_path,
                                 acquisition_id=acq["acquisition_id"],
                                 level=self.level,
                                 partition=self.partition))

        return tasks
