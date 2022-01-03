"""
This code contains the OrbitMonitor class that looks for recently completed orbits and then triggers the creation of
orbit-based BAD data products and runs geolocation

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os

from emit_main.workflow.l1a_tasks import L1AReformatBAD
from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class OrbitMonitor:

    def __init__(self, config_path, level="INFO", partition="emit"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition

        # Get workflow manager
        self.wm = WorkflowManager(config_path=config_path)

    def get_recent_orbit_tasks(self):
        tasks = []
        # Find orbits from the last day
        dm = self.wm.database_manager
        stop_time = datetime.datetime.now(tz=datetime.timezone.utc)
        start_time = stop_time - datetime.timedelta(days=1)
        orbits = dm.find_orbits_touching_date_range("start_time", start_time, stop_time) + \
            dm.find_orbits_touching_date_range("stop_time", start_time, stop_time)

        # If no results, just return empty list
        if len(orbits) == 0:
            logger.info(f"Did not find any orbits in the last day. Not executing any tasks.")
            return tasks

        # Get unique orbit ids
        orbit_ids = [o["orbit_id"] for o in orbits]
        orbit_ids = list(set(orbit_ids))

        # For each orbit id, check if orbit has complete BAD data and is unprocessed, and if so create a task for it
        for orbit_id in orbit_ids:
            wm = WorkflowManager(config_path=self.config_path, orbit_id=orbit_id)
            orbit = self.wm.orbit
            if orbit.has_complete_bad_data() and "associated_bad_netcdf" not in orbit.metadata:
                logger.info(f"Creating L1AReformatBAD task for orbit {orbit_id}")
                tasks.append(L1AReformatBAD(config_path=self.config_path,
                                            orbit_id=orbit_id,
                                            level=self.level,
                                            partition=self.partition))

        return tasks
