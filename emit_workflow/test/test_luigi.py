"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import luigi
from emit_workflow import l1b_tasks

def test_luigi_build():

    success = luigi.build(
        [l1b_tasks.L1BCalibrate(acquisition_id="emit20200515t110600",
                      config_path="test_config.json")],
        workers=2,
        local_scheduler=True,
        log_level="DEBUG")

    assert success

test_luigi_build()