"""
This code contains test functions for database_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging.config
import sys

sys.path.insert(0,"../")
from database_manager import DatabaseManager

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-workflow")


def test_database_manager():

    logger.debug("Running test_database_manager")

    dm = DatabaseManager("config/test_config.json", acquisition_id="emit20200101t000000")

    acquisition = {
        "_id": "emit20200101t000000",
        "lines": 1000,
        "bands": 324,
        "samples": 1280,
        "start_time": datetime.datetime(2020, 1, 1, 0, 0, 0),
        "end_time": datetime.datetime(2020, 1, 1, 0, 11, 26)
    }

    acquisitions = dm.db.acquisitions

    acquisitions.delete_one({"_id": "emit20200101t000000"})

    acquisition_id = acquisitions.insert_one(acquisition).inserted_id

    assert acquisition_id == "emit20200101t000000"

test_database_manager()