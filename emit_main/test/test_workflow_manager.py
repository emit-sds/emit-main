"""
This code contains test functions for file_manager

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging.config
import os

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from emit_main.workflow.workflow_manager import WorkflowManager

test_dir = os.path.abspath(os.path.dirname(__file__))
config_path = os.path.join(test_dir, "config", "jenkins_test_config.json")
logging_conf_path = os.path.join(test_dir, "test_logging.conf")

logging.config.fileConfig(fname=logging_conf_path)
logger = logging.getLogger("emit-main")


def test_acquisition_paths():

    logger.debug("Running test_acquisition_paths")

    wm = WorkflowManager(config_path, acquisition_id="emit20200101t000000")
    acq = wm.acquisition
    wm.remove_path(acq.raw_img_path)
    wm.touch_path(acq.raw_img_path)
    assert wm.path_exists(acq.raw_img_path)


def test_build_runtime_environment():

    logger.debug("Running test_pge_build")

    wm = WorkflowManager(config_path)
    wm.build_runtime_environment()


test_acquisition_paths()
test_build_runtime_environment()
