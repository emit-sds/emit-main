"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os

import luigi

from emit_main.workflow.test_tasks import ExampleTask
from emit_main.workflow.workflow_manager import WorkflowManager


def test_luigi_build(config_path):

    config_path = os.path.abspath(config_path)
    print("\nRunning test_luigi_build with config: %s" % config_path)

    acquisition_id = "emit20200101t000000"

    wm = WorkflowManager(config_path=config_path, acquisition_id=acquisition_id)
    acq = wm.acquisition
    if os.path.exists(acq.rdn_hdr_path):
        os.system(" ".join(["rm", "-f", acq.rdn_hdr_path]))

    luigi_logging_conf = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "workflow", "luigi",
                                                      "logging.conf"))
    success = luigi.build(
        [ExampleTask(config_path=config_path, acquisition_id=acquisition_id)],
        workers=wm.config["luigi_workers"],
        local_scheduler=wm.config["luigi_local_scheduler"],
        logging_conf_file=luigi_logging_conf)

    assert success
