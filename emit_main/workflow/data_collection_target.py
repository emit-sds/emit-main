"""
These classes specifies success criteria to determine if a dcid was processed correctly

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os

import luigi


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
