"""
This code contains the OrbitMonitor class that looks for recently completed orbits and then triggers the creation of
orbit-based BAD data products and runs geolocation

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

from emit_main.workflow.daac_helper_tasks import AssignDAACSceneNumbers
from emit_main.workflow.l1a_tasks import L1AReformatBAD
from emit_main.workflow.l1b_tasks import L1BGeolocate
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

    def get_bad_reformatting_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find orbits within time range
        dm = self.wm.database_manager
        orbits = dm.find_orbits_for_bad_reformatting(start=start_time, stop=stop_time, date_field=date_field,
                                                     retry_failed=retry_failed)

        # If no results, just return empty list
        if len(orbits) == 0:
            logger.info(f"Did not find any orbits with {date_field} between {start_time} and {stop_time} needing BAD "
                        f"reformatting tasks. Not executing any tasks.")
            return tasks

        for orbit in orbits:
            logger.info(f"Creating L1AReformatBAD task for orbit {orbit['orbit_id']}")
            tasks.append(L1AReformatBAD(config_path=self.config_path,
                                        orbit_id=orbit['orbit_id'],
                                        level=self.level,
                                        partition=self.partition))

        return tasks

    def get_geolocation_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find orbits within time range
        dm = self.wm.database_manager
        orbits = dm.find_orbits_for_geolocation(start=start_time, stop=stop_time, date_field=date_field,
                                                retry_failed=retry_failed)

        # If no results, just return empty list
        if len(orbits) == 0:
            logger.info(f"Did not find any orbits with {date_field} between {start_time} and {stop_time} needing "
                        f"geolocation tasks. Not executing any tasks.")
            return tasks

        for orbit in orbits:
            logger.info(f"Creating L1BGeolocate task for orbit {orbit['orbit_id']}")
            tasks.append(L1BGeolocate(config_path=self.config_path,
                                      orbit_id=orbit['orbit_id'],
                                      level=self.level,
                                      partition=self.partition))

        return tasks

    def get_daac_scenes_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find orbits within time range
        dm = self.wm.database_manager
        orbits = dm.find_orbits_for_daac_scene_numbers(start=start_time, stop=stop_time, date_field=date_field,
                                                       retry_failed=retry_failed)

        # If no results, just return empty list
        if len(orbits) == 0:
            logger.info(f"Did not find any orbits with {date_field} between {start_time} and {stop_time} needing "
                        f"DAAC scene number tasks. Not executing any tasks.")
            return tasks

        for orbit in orbits:
            logger.info(f"Creating AssignDAACSceneNumbers task for orbit {orbit['orbit_id']}")
            tasks.append(AssignDAACSceneNumbers(config_path=self.config_path,
                                                orbit_id=orbit['orbit_id'],
                                                level=self.level,
                                                partition=self.partition))

        return tasks
