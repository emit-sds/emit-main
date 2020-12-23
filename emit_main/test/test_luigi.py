"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging.config
import luigi

#sys.path.insert(0,"../")
from emit_main.workflow.workflow_manager import WorkflowManager
from emit_main.workflow.l1b_tasks import L1BCalibrate

logging.config.fileConfig(fname="test_logging.conf")
logger = logging.getLogger("emit-main")


def test_luigi_build():

    logger.debug("Running test_luigi_build")

    wm = WorkflowManager("config/test_config.json", acquisition_id="emit20200101t000000")
    acq = wm.acquisition
    wm.remove_dir(acq.l1a_data_dir)
    wm.remove_dir(acq.l1b_data_dir)

    success = luigi.build(
        [L1BCalibrate(config_path="config/test_config.json", acquisition_id="emit20200101t000000")],
        workers=wm.luigi_workers,
        local_scheduler=wm.luigi_local_scheduler,
        logging_conf_file=wm.luigi_logging_conf)

    assert success


# TODO: Change this to test_tasks and add another test that uses the luigi scheduler
#test_luigi_build()