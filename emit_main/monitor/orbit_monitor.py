"""
This code contains the OrbitMonitor class that looks for recently completed orbits and then triggers the creation of
orbit-based BAD data products and runs geolocation

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

from emit_main.workflow.daac_helper_tasks import AssignDAACSceneNumbers
from emit_main.workflow.l1a_tasks import L1AReformatBAD
from emit_main.workflow.l1b_tasks import L1BGeolocate, L1BAttDeliver
from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class OrbitMonitor:

    def __init__(self, config_path, level="INFO", partition="emit", processing_direction="forward"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition
        self.processing_direction = processing_direction

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
                                                retry_failed=retry_failed,
                                                processing_direction=self.processing_direction)

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

    def get_l1batt_delivery_tasks(self, start_time, stop_time, date_field="last_modified", retry_failed=False):
        tasks = []
        # Find orbits within time range
        dm = self.wm.database_manager
        orbits = dm.find_orbits_for_l1batt_delivery(start=start_time, stop=stop_time, date_field=date_field,
                                                    retry_failed=retry_failed,
                                                    processing_direction=self.processing_direction)

        # If no results, just return empty list
        if len(orbits) == 0:
            logger.info(f"Did not find any orbits with {date_field} between {start_time} and {stop_time} needing "
                        f"L1B att/eph delivery tasks. Not executing any tasks.")
            return tasks

        for orbit in orbits:
            logger.info(f"Creating L1BAttDeliver task for orbit {orbit['orbit_id']}")
            tasks.append(L1BAttDeliver(config_path=self.config_path,
                                       orbit_id=orbit['orbit_id'],
                                       level=self.level,
                                       partition=self.partition,
                                       daac_ingest_queue=self.processing_direction))

        return tasks

    def get_reprocessing_tasks(self, start_time, stop_time, from_build, to_build, product_arg,
                               date_field="last_modified", retry_failed=False):
        tasks = []
        # Find orbits within time range that are missing DB entries
        dm = self.wm.database_manager
        orbits = dm.find_orbits_for_reprocessing(start=start_time, stop=stop_time, from_build=from_build,
                                                 to_build=to_build, product_arg=product_arg,
                                                 date_field=date_field, retry_failed=retry_failed)

        # If no results, just return empty list
        if len(orbits) == 0:
            logger.info(f"Did not find any orbits with {date_field} between {start_time} and {stop_time} needing "
                        f"reprocessing from build {from_build} to build {to_build} for product {product_arg}.")
            return tasks

        for orbit in orbits:
            # Map tasks based on product arg
            # TODO: Add other product levels
            if product_arg == "l1bgeo":
                logger.info(f"Creating L1BGeolocate task for orbit {orbit['orbit_id']}")
                tasks.append(L1BGeolocate(config_path=self.config_path,
                                          orbit_id=orbit["orbit_id"],
                                          level=self.level,
                                          partition=self.partition,
                                          reproc_from_build=from_build))

        return tasks
