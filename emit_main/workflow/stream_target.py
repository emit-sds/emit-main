"""
These classes specifies success criteria to determine if a stream file was processed correctly

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os

import luigi


class StreamTarget(luigi.Target):
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
