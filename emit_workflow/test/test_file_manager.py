"""
This code contains test functions for file_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import os
import shutil

from emit_workflow import file_manager

def test_file_manager():

    config = {}
    with open("test_config.json", "r") as f:
        config = json.load(f)

    test_env_path = os.path.join(
        config["local_store_path"],
        config["instrument"],
        config["environment"])

    shutil.rmtree(test_env_path)

    fm = file_manager.FileManager(
        acquisition_id="emit20200101t000000",
        config_path="test_config.json"
    )
    fm.touch_path(fm.paths["raw_img"])
    assert fm.path_exists(fm.paths["raw_img"])

test_file_manager()