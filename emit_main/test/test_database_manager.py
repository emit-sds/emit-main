"""
This code contains test functions for database_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime

from emit_main.database.database_manager import DatabaseManager


def test_acquisition_delete(config_path):

    print("\nRunning test_acquisition_delete with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    acquisitions = dm.db.acquisitions
    acquisitions.delete_one({"acquisition_id": "emit20200101t000000", "build_num": dm.build_num})

    acquisition = dm.find_acquisition_by_id("emit20200101t000000")
    assert acquisition == None


def test_acquisition_insert(config_path):

    print("\nRunning test_acquisition_insert with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    metadata = {
        "acquisition_id": "emit20200101t000000",
        "build_num": dm.build_num,
        "processing_version": dm.processing_version,
        "start_time": datetime.datetime(2020, 1, 1, 0, 0, 0),
        "end_time": datetime.datetime(2020, 1, 1, 0, 11, 26),
        "orbit": "00000",
        "scene": "000"
    }
    dm.insert_acquisition(metadata)
    acquisition = dm.find_acquisition_by_id(metadata["acquisition_id"])

    assert acquisition["acquisition_id"] == "emit20200101t000000"
