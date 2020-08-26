"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging.config
import shutil
import sys

from emit_main.workflow.l0_tasks import *
from emit_main.workflow.l1a_tasks import *
from emit_main.workflow.l1b_tasks import *
from emit_main.workflow.l2a_tasks import *
from emit_main.workflow.slurm import SlurmJobTask

logging.config.fileConfig(fname="logging.conf")
logger = logging.getLogger("emit-main")


def parse_args():
    product_choices = ["l1araw", "l1bcal", "l2arefl"]
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--acquisition_id",
                        help="Acquisition ID")
    parser.add_argument("-c", "--config_path",
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
    products = args.products.split(",")
    acquisition_kwargs = {
        "config_path": args.config_path,
        "acquisition_id": args.acquisition_id
    }

    prod_task_map = {
        "l1araw": L1AReassembleRaw(**acquisition_kwargs),
        "l1bcal": L1BCalibrate(**acquisition_kwargs),
        "l2arefl": L2AReflectance(**acquisition_kwargs)
    }

    tasks = []
    for prod in products:
        tasks.append(prod_task_map[prod])
    return tasks


#@luigi.Task.event_handler(luigi.Event.SUCCESS)
@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    logger.info("SUCCESS: %s" % task)

#    logger.debug("Deleting tmp folder %s" % task.tmp_dir)
#    shutil.rmtree(task.tmp_dir)


#@luigi.Task.event_handler(luigi.Event.FAILURE)
@SlurmJobTask.event_handler(luigi.Event.FAILURE)
def task_failure(task, e):
    # TODO: If additional debugging is needed, change exc_info to True
    logger.error("FAILURE: %s failed with exception %s" % (task, str(e)), exc_info=False)

    # Move tmp folder to errors folder
    error_dir = task.tmp_dir.replace("/tmp/", "/error/")
    logger.error("Moving tmp folder %s to %s" % (task.tmp_dir, error_dir))
    shutil.move(task.tmp_dir, error_dir)

    # TODO: Clean up DB?


def main():
    """
    Parse command line arguments and initiate tasks
    """
    args = parse_args()
    tasks = get_tasks_from_args(args)

    wm = WorkflowManager(args.config_path)
    wm.build_runtime_environment()

    if args.workers:
        workers = args.workers
    else:
        workers = wm.luigi_workers

    luigi.build(tasks, workers=workers, local_scheduler=wm.luigi_local_scheduler,
                logging_conf_file=wm.luigi_logging_conf)


if __name__ == '__main__':
    main()
