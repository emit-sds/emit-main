"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging.config
import luigi
import sys

sys.path.insert(0,"../")
import file_manager
import l1b_tasks

logging.config.fileConfig(fname="../logging.conf")
logger = logging.getLogger("emit-workflow")

def test_luigi_build():

    logger.debug("Running test_luigi_build")

    fm = file_manager.FileManager(acquisition_id="emit20200101t000000", config_path="test_config.json")
    fm.remove_dir(fm.dirs["l1a"])
    fm.remove_dir(fm.dirs["l1b"])

    success = luigi.build(
        [l1b_tasks.L1BCalibrate(acquisition_id="emit20200101t000000", config_path="test_config.json")],
        workers=2,
        local_scheduler=True,
        logging_conf_file="../luigi/logging.conf")

    assert success

test_luigi_build()