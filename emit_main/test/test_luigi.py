"""
This code contains test functions for luigi

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import shutil

import luigi

from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.test_tasks import ExampleTask
from emit_main.workflow.workflow_manager import WorkflowManager


@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    print("SUCCESS: %s" % task)

    print("Deleting task's tmp folder %s" % task.tmp_dir)
    if os.path.exists(task.tmp_dir):
        shutil.rmtree(task.tmp_dir)

    print(f"Deleting task's local tmp dir: {task.local_tmp_dir}")
    if os.path.exists(task.local_tmp_dir):
        shutil.rmtree(task.local_tmp_dir)


@SlurmJobTask.event_handler(luigi.Event.FAILURE)
def task_failure(task, e):
    print("TASK FAILURE: %s" % task)

    print("Deleting task's tmp folder %s" % task.tmp_dir)
    if os.path.exists(task.tmp_dir):
        shutil.rmtree(task.tmp_dir)

    print(f"Deleting task's local tmp dir: {task.local_tmp_dir}")
    if os.path.exists(task.local_tmp_dir):
        shutil.rmtree(task.local_tmp_dir)


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
        [ExampleTask(config_path=config_path, acquisition_id=acquisition_id, level="INFO", partition="cron")],
        workers=wm.config["luigi_workers"],
        local_scheduler=wm.config["luigi_local_scheduler"],
        logging_conf_file=luigi_logging_conf)

    assert success
