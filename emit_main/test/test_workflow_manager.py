"""
This code contains test functions for workflow_manager.py

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os

from emit_main.workflow.workflow_manager import WorkflowManager


def test_check_runtime_environment(config_path):

    print("\nRunning test_check_runtime_environment with config: %s" % config_path)

    wm = WorkflowManager(config_path=config_path)
    assert wm.check_runtime_environment() is True


def test_acquisition(config_path):

    print("\nRunning test_acquisition with config: %s" % config_path)

    wm = WorkflowManager(config_path=config_path, acquisition_id="emit20200101t000000")
    acq = wm.acquisition
    assert acq is not None and os.path.exists(acq.acquisitions_dir)


def test_stream(config_path):

    print("\nRunning test_stream with config: %s" % config_path)

    wm = WorkflowManager(config_path=config_path)
    stream_path = os.path.join(wm.ingest_dir, "emit_1675_200101000000_200101020000_200101084102_hsc.bin")
    wm = WorkflowManager(config_path=config_path, stream_path=stream_path)
    stream = wm.stream
    assert stream is not None and os.path.exists(stream.streams_dir)