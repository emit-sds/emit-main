"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import luigi

from l0_tasks import *
from l1a_tasks import *
from l1b_tasks import *


def main():
    """
    Parse command line arguments and initiate tasks
    """
    parser = argparse.ArgumentParser()

    luigi.build(
        [L1BCalibrate(acquisition="emit20200515t110600")],
        workers=2,
        local_scheduler=True,
        log_level="DEBUG")

if __name__ == '__main__':
    main()