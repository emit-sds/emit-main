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


class ENVITarget(luigi.Target):
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
                    if os.path.splitext(path)[-1] in [".img", ".hdr"] and not os.path.exists(path):
                        logger.debug("Checking output for %s - Failed to find acquisition path %s" %
                                     (self._task_family, path))
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


class NetCDFTarget(luigi.Target):
    """This class specifies success criteria to determine if a NetCDF file was processed correctly"""
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
                    if os.path.splitext(path)[-1] in [".nc"] and not os.path.exists(path):
                        logger.debug("Checking output for %s - Failed to find acquisition path %s" %
                                     (self._task_family, path))
                        return False
                return True
        return False


class UMMGTarget(luigi.Target):
    """This class specifies success criteria to determine if a NetCDF file was processed correctly"""
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
                    if os.path.splitext(path)[-1] in [".json"] and not os.path.exists(path):
                        logger.debug("Checking output for %s - Failed to find acquisition path %s" %
                                     (self._task_family, path))
                        return False
                return True
        return False