"""
This code contains test functions for database_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging.config

#sys.path.insert(0,"../")
from emit_main.workflow.workflow_manager import WorkflowManager

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-main")


def test_database_manager():

    logger.debug("Running test_database_manager")

    wm = WorkflowManager("config/jenkins_test_config.json")
    dm = wm.database_manager

    acquisition = {
        "acquisition_id": "emit20200101t000000",
        "build_num": "000",
        "processing_version": "00",
        "start_time": datetime.datetime(2020, 1, 1, 0, 0, 0),
        "end_time": datetime.datetime(2020, 1, 1, 0, 11, 26),
        "orbit": "00001",
        "scene": "001"
    }

    acquisitions = dm.db.acquisitions
    acquisitions.delete_many({
        "acquisition_id": "emit20200101t000000",
        "build_num": "000"})
    acquisitions.insert_one(acquisition)
    acquisition = acquisitions.find_one({
        "acquisition_id": "emit20200101t000000",
        "build_num": "000"})
    assert acquisition["acquisition_id"] == "emit20200101t000000"


test_database_manager()
