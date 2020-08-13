"""
This code contains test functions for file_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging.config
import sys

sys.path.insert(0,"../")
from file_manager import FileManager

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-workflow")


def test_acquisition_paths():

    logger.debug("Running test_acquisition_paths")

    fm = FileManager("../config/test_config.json", acquisition_id="emit20200101t000000")
    fm.remove_path(fm.paths["raw_img"])
    fm.touch_path(fm.paths["raw_img"])
    assert fm.path_exists(fm.paths["raw_img"])

def test_build_runtime_environment():

    logger.debug("Running test_pge_build")

    fm = FileManager("../config/test_config.json")
    fm.build_runtime_environment()


test_acquisition_paths()
test_build_runtime_environment()