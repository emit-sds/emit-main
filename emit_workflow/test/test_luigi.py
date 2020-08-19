"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging.config
import luigi
import sys

sys.path.insert(0,"../")
from file_manager import FileManager
from l1b_tasks import L1BCalibrate

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-workflow")


def test_luigi_build():

    logger.debug("Running test_luigi_build")

    fm = FileManager("config/test_config.json", acquisition_id="emit20200101t000000")
    fm.remove_dir(fm.l1a_data_dir)
    fm.remove_dir(fm.l1b_data_dir)

    success = luigi.build(
        [L1BCalibrate(config_path="config/test_config.json", acquisition_id="emit20200101t000000")],
        workers=2,
        local_scheduler=fm.luigi_local_scheduler,
        logging_conf_file="../luigi/logging.conf")

    assert success


# TODO: Change this to test_tasks and add another test that uses the luigi scheduler
test_luigi_build()