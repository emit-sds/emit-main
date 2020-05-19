"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import luigi

from emit_workflow import file_manager
from emit_workflow import l1b_tasks

def test_luigi_build():

    fm = file_manager.FileManager(acquisition_id="emit20200101t000000", config_path="test_config.json")
    fm.remove_dir(fm.dirs["l1a"])
    fm.remove_dir(fm.dirs["l1b"])

    success = luigi.build(
        [l1b_tasks.L1BCalibrate(acquisition_id="emit20200101t000000", config_path="test_config.json")],
        workers=2,
        local_scheduler=True,
        log_level="DEBUG")

    assert success

test_luigi_build()