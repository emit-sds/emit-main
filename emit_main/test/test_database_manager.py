"""
This code contains test functions for database_manager.py

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime

from emit_main.database.database_manager import DatabaseManager


def test_acquisition_delete(config_path):

    print("\nRunning test_acquisition_delete with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    acquisitions = dm.db.acquisitions
    acquisitions.delete_one({"acquisition_id": "emit20200101t000000", "build_num": dm.config["build_num"]})

    acquisition = dm.find_acquisition_by_id("emit20200101t000000")
    assert acquisition is None


def test_acquisition_insert(config_path):

    print("\nRunning test_acquisition_insert with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    metadata = {
        "acquisition_id": "emit20200101t000000",
        "build_num": dm.config["build_num"],
        "processing_version": dm.config["processing_version"],
        "start_time": datetime.datetime(2020, 1, 1, 0, 0, 0),
        "stop_time": datetime.datetime(2020, 1, 1, 0, 11, 26),
        "orbit": "00000",
        "scene": "000"
    }
    dm.insert_acquisition(metadata)
    acquisition = dm.find_acquisition_by_id(metadata["acquisition_id"])

    assert acquisition["acquisition_id"] == "emit20200101t000000"


def test_acquisition_update(config_path):

    print("\nRunning test_acquisition_update with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    acquisition_id = "emit20200101t000000"

    metadata = {
        "test_key": "test_value"
    }
    dm.update_acquisition_metadata(acquisition_id, metadata)
    acquisition = dm.find_acquisition_by_id(acquisition_id)

    assert acquisition["acquisition_id"] == "emit20200101t000000" and acquisition["test_key"] == "test_value"


def test_stream_delete(config_path):

    print("\nRunning test_stream_delete with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    streams = dm.db.streams
    streams.delete_one({"hosc_name": "emit_1675_200101000000_200101020000_200101084102_hsc.bin",
                        "build_num": dm.config["build_num"]})

    stream = dm.find_stream_by_name("emit_1675_200101000000_200101020000_200101084102_hsc.bin")
    assert stream is None


def test_stream_insert(config_path):

    print("\nRunning test_stream_insert with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    dm.insert_hosc_stream("emit_1675_200101000000_200101020000_200101084102_hsc.bin")
    stream = dm.find_stream_by_name("emit_1675_200101000000_200101020000_200101084102_hsc.bin")

    assert stream["hosc_name"] == "emit_1675_200101000000_200101020000_200101084102_hsc.bin"


def test_stream_update(config_path):

    print("\nRunning test_stream_update with config: %s" % config_path)

    dm = DatabaseManager(config_path=config_path)

    hosc_name = "emit_1675_200101000000_200101020000_200101084102_hsc.bin"

    metadata = {
        "test_key": "test_value"
    }
    dm.update_stream_metadata(hosc_name, metadata)
    stream = dm.find_stream_by_name(hosc_name)

    assert stream["hosc_name"] == hosc_name and stream["test_key"] == "test_value"
