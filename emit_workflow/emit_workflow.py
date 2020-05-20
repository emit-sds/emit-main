"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse

import luigi
import logging.config
from l0_tasks import *
from l1a_tasks import *
from l1b_tasks import *

logging.config.fileConfig(fname="logging.conf")
logger = logging.getLogger("emit-workflow")

def main():
    """
    Parse command line arguments and initiate tasks
    """
    parser = argparse.ArgumentParser()

    luigi.build(
        [L1BCalibrate(acquisition_id="emit20200101t000000", config_path="test_config.json")],
        workers=2,
        local_scheduler=True)

if __name__ == '__main__':
    main()