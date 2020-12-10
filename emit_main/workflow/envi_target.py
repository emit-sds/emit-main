"""
This code overrides the luigi.LocalTarget class and returns true if both and ENVI file and its .hdr file exist

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

import luigi

logger = logging.getLogger("emit-main")


class ENVITarget(luigi.Target):
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
                        logger.debug("Checking output for %s - Failed to find acquisition path %s" %
                                     (self._task_family, path))
                        return False
                return True
        return False
