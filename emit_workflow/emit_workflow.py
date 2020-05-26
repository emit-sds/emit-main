"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import luigi
import logging
import logging.config

from l0_tasks import *
from l1a_tasks import *
from l1b_tasks import *
from slurm import SlurmJobTask

logging.config.fileConfig(fname="logging.conf")
logger = logging.getLogger("emit-workflow")


#@luigi.Task.event_handler(luigi.Event.SUCCESS)
@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    logger.info("SUCCESS: %s" % task)


#@luigi.Task.event_handler(luigi.Event.FAILURE)
@SlurmJobTask.event_handler(luigi.Event.FAILURE)
def task_failure(task, e):
    logger.error("FAILURE: %s failed with exception %s" % (task, str(e)), exc_info=True)

    # Clean up tmp directories for failed try or move them to an "tmp/errors" subfolder


def main():
    """
    Parse command line arguments and initiate tasks
    """
    parser = argparse.ArgumentParser()

    luigi.build(
        [L1BCalibrate("config/dev_local_config.json", acquisition_id="emit20200101t000000")],
        workers=2,
        local_scheduler=True,
        logging_conf_file="luigi/logging.conf")


if __name__ == '__main__':
    main()