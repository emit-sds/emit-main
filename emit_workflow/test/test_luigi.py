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

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-workflow")


def test_luigi_build():

    logger.debug("Running test_luigi_build")

    fm = file_manager.FileManager("../config/test_config.json", acquisition_id="emit20200101t000000")
    fm.remove_dir(fm.dirs["l1a"])
    fm.remove_dir(fm.dirs["l1b"])

    success = luigi.build(
        [l1b_tasks.L1BCalibrate("../config/test_config.json", acquisition_id="emit20200101t000000")],
        workers=2,
        local_scheduler=fm.luigi_local_scheduler,
        logging_conf_file="../luigi/logging.conf")

    assert success

test_luigi_build()