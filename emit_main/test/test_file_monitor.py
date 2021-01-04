"""
This code contains test functions for file_monitor.py

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

from emit_main.file_monitor.file_monitor import FileMonitor


def test_ingest_files(config_path):

    print("\nRunning test_file_monitor with config: %s" % config_path)

    fm = FileMonitor(config_path=config_path)
    paths = fm.ingest_files(dry_run=True)
    assert len(paths) == 1
