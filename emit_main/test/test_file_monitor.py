"""
This code contains test functions for file_monitor.py

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import os
import shutil

from emit_main.file_monitor.file_monitor import FileMonitor
from emit_main.workflow.workflow_manager import WorkflowManager


def test_ingest_files(config_path):

    config_path = os.path.abspath(config_path)
    print("\nRunning test_file_monitor with config: %s" % config_path)

    wm = WorkflowManager(config_path=config_path)
    test_data_ingest_dir = os.path.join(wm.environment_dir, "test_data", "ingest")
    if os.path.exists(test_data_ingest_dir):
        for file in glob.glob(os.path.join(test_data_ingest_dir, "*_hsc.bin")):
            shutil.copy2(file, wm.ingest_dir)
        fm = FileMonitor(config_path=config_path)
        paths = fm.ingest_files(dry_run=True)
        for file in glob.glob(os.path.join(wm.ingest_dir, "*_hsc.bin")):
            os.remove(file)
        assert len(paths) > 0

    assert True
