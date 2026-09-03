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
    def __init__(self, data_collection, task_family, product_version):
        self._dc = data_collection
        self._task_family = task_family
        self._product_version = product_version

    def exists(self):
        if self._dc is None:
            return False
        for log in reversed(self._dc.processing_log):
            # For backwards compatibility, default the log's product version to "01" in all cases except 
            # for ch4, co2, and maskTf tasks which were "02"
            if "product_version" in log:
                log_prod_version = log["product_version"]
            else:
                log_prod_version = "02" if any(x in self._task_family for x in ("CH4", "CO2", "MaskTf")) else "01"
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS" and log_prod_version == self._product_version:
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
    def __init__(self, orbit, task_family, product_version):
        self._orbit = orbit
        self._task_family = task_family
        self._product_version = product_version

    def exists(self):
        if self._orbit is None:
            logger.debug("Checking output for %s - Failed to find orbit in DB" % self._task_family)
            return False
        for log in reversed(self._orbit.processing_log):
            # For backwards compatibility, default the log's product version to "01" which works for all orbit targets
            log_prod_version = log.get("product_version", "01")
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS" and log_prod_version == self._product_version:
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
                logger.debug(f"Failed to find DAAC scene number for {acq['acquisition_id']}")
                return False
        return True


class AcquisitionTarget(luigi.Target):
    """This class specifies success criteria to determine if an envi file was processed correctly"""
    def __init__(self, acquisition, task_family, product_version):
        self._acquisition = acquisition
        self._task_family = task_family
        self._product_version = product_version

    def exists(self):
        if self._acquisition is None:
            logger.debug("Checking output for %s - Failed to find acquisition in DB" % self._task_family)
            return False
        for log in reversed(self._acquisition.processing_log):
            # For backwards compatibility, default the log's product version to "01" in all cases except 
            # for ch4, co2, and maskTf tasks which were "02"
            if "product_version" in log:
                log_prod_version = log["product_version"]
            else:
                log_prod_version = "02" if any(x in self._task_family for x in ("CH4", "CO2", "MaskTf")) else "01"
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS" and log_prod_version == self._product_version:
                # Check that outputs exist on filesystem
                for path in log["output"].values():
                    if not os.path.exists(path):
                        return False
                return True
        return False


class StreamTarget(luigi.Target):
    """This class specifies success criteria to determine if a stream file was processed correctly"""
    def __init__(self, stream, task_family, product_version):
        self._stream = stream
        self._task_family = task_family
        self._product_version = product_version

    def exists(self):
        if self._stream is None:
            return False
        for log in reversed(self._stream.processing_log):
            # For backwards compatibility, default the log's product version to "01" which works for all stream targets 
            log_prod_version = log.get("product_version", "01")
            if log["task"] == self._task_family and log["completion_status"] == "SUCCESS" and log_prod_version == self._product_version:
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
