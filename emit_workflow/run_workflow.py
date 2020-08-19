"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import luigi
import logging
import sys

from l0_tasks import *
from l1a_tasks import *
from l1b_tasks import *
from slurm import SlurmJobTask

logging.config.fileConfig(fname="logging.conf")
logger = logging.getLogger("emit-workflow")


def parse_args():
    product_choices = ["l1araw", "l1bcal"]
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--acquisition",
                        help="Acquisition ID")
    parser.add_argument("-c", "--config",
                        help="Path to config file")
    parser.add_argument("-p", "--products",
                        help=("Comma delimited list of products to create (no spaces). \
                        Choose from " + ", ".join(product_choices)))
    parser.add_argument("-w", "--workers", default=2,
                        help="Number of luigi workers")
    args = parser.parse_args()
    if args.products:
        product_list = args.products.split(",")
        for prod in product_list:
            if prod not in product_choices:
                print("ERROR: Product \"%s\" is not a valid product choice." % prod)
                sys.exit(1)
    else:
        args.products = "l1araw"
    return args


def get_tasks_from_args(args):
    fm = FileManager(args.config)
    products = args.products.split(",")
    acquisition_kwargs = {
        "config_path": args.config,
        "acquisition_id": args.acquisition
    }

    prod_task_map = {
        "l1araw": L1AReassembleRaw(**acquisition_kwargs)
    }

    tasks = []
    for prod in products:
        tasks.append(prod_task_map[prod])
    return tasks


#@luigi.Task.event_handler(luigi.Event.SUCCESS)
@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    logger.info("SUCCESS: %s" % task)


#@luigi.Task.event_handler(luigi.Event.FAILURE)
@SlurmJobTask.event_handler(luigi.Event.FAILURE)
def task_failure(task, e):
    # TODO: If additional debugging is needed, change exc_info to True
    logger.error("FAILURE: %s failed with exception %s" % (task, str(e)), exc_info=False)

    # Clean up tmp directories for failed try or move them to an "tmp/errors" subfolder


def main():
    """
    Parse command line arguments and initiate tasks
    """
    args = parse_args()
    tasks = get_tasks_from_args(args)

    fm = FileManager(args.config)
    fm.build_runtime_environment()

    if args.workers:
        workers = args.workers
    else:
        workers = fm.num_workers

    luigi.build(tasks, workers=workers, local_scheduler=fm.luigi_local_scheduler,
                logging_conf_file=fm.luigi_logging_conf)


if __name__ == '__main__':
    main()
