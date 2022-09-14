"""
This code overrides the luigi.LocalTarget class and returns true if the given file type exists

Authors: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
         Philip G. Brodrick,  philip.brodrick@jpl.nasa.gov
"""

import logging
import os

import luigi

logger = logging.getLogger("emit-main")


class DataCollectionTarget(luigi.Target):
    def __init__(self, data_collection, task_family):
        self._dc = data_collection
        self._task_family = task_family

    def exists(self):
        if self._dc is None:
            return False
        for log in reversed(self._dc.processing_log):
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS":
                # Check that outputs exist on filesystem
                for val in log["output"].values():
                    if type(val) == list:
                        for v in val:
                            if not os.path.exists(v):
                                return False
                    elif not os.path.exists(val):
                        return False
                return True
        return False


class OrbitTarget(luigi.Target):
    """This class specifies success criteria to determine if an orbit file was processed correctly"""
    def __init__(self, orbit, task_family):
        self._orbit = orbit
        self._task_family = task_family

    def exists(self):
        if self._orbit is None:
            logger.debug("Checking output for %s - Failed to find orbit in DB" % self._task_family)
            return False
        for log in reversed(self._orbit.processing_log):
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS":
                # Check that outputs exist on filesystem
                for val in log["output"].values():
                    if type(val) == list:
                        for v in val:
                            if not os.path.exists(v):
                                return False
                    elif not os.path.exists(val):
                        return False
                return True
        return False


class DAACSceneNumbersTarget(luigi.Target):
    def __init__(self, acquisitions):
        self._acquisitions = acquisitions

    def exists(self):
        for acq in self._acquisitions:
            if "daac_scene" not in acq:
                logger.debug(f"Failed to find DAAC scene number for {acq.acquisition_id}")
                return False
        return True


class AcquisitionTarget(luigi.Target):
    """This class specifies success criteria to determine if an envi file was processed correctly"""
    def __init__(self, acquisition, task_family):
        self._acquisition = acquisition
        self._task_family = task_family

    def exists(self):
        if self._acquisition is None:
            logger.debug("Checking output for %s - Failed to find acquisition in DB" % self._task_family)
            return False
        for log in reversed(self._acquisition.processing_log):
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS":
                # Check that outputs exist on filesystem
                for path in log["output"].values():
                    if not os.path.exists(path):
                        return False
                return True
        return False


class StreamTarget(luigi.Target):
    """This class specifies success criteria to determine if a stream file was processed correctly"""
    def __init__(self, stream, task_family):
        self._stream = stream
        self._task_family = task_family

    def exists(self):
        if self._stream is None:
            return False
        for log in reversed(self._stream.processing_log):
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS":
                # Check that outputs exist on filesystem
                for val in log["output"].values():
                    if type(val) == list:
                        for v in val:
                            if not os.path.exists(v):
                                return False
                    elif not os.path.exists(val):
                        return False
                return True
        return False
