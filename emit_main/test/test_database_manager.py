"""
This code contains test functions for database_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging.config

from emit_main.database.database_manager import DatabaseManager

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-main")


def test_database_manager():

    logger.debug("Running test_database_manager")

    dm = DatabaseManager("config/test_config.json")

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
    acquisitions.delete_one({"_id": "emit20200101t000000"})
    acquisition_id = acquisitions.insert_one(acquisition).inserted_id
    assert acquisition_id == "emit20200101t000000"


test_database_manager()