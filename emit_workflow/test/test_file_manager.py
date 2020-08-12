"""
This code contains test functions for file_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging.config
import sys

#sys.path.insert(0,"../")
from emit_workflow.file_manager import FileManager

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-workflow")


def test_file_manager():

    logger.debug("Running test_file_manager")

    fm = FileManager("../config/test_config.json", acquisition_id="emit20200101t000000")
    fm.remove_path(fm.paths["raw_img"])
    fm.touch_path(fm.paths["raw_img"])
    assert fm.path_exists(fm.paths["raw_img"])

test_file_manager()