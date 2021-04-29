"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging.config
import os
import shutil
import sys

from emit_main.workflow.l0_tasks import *
from emit_main.workflow.l1a_tasks import *
from emit_main.workflow.l1b_tasks import *
from emit_main.workflow.l2a_tasks import *
from emit_main.workflow.l2b_tasks import *
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

logging_conf = os.path.join(os.path.dirname(__file__), "logging.conf")
logging.config.fileConfig(fname=logging_conf)
logger = logging.getLogger("emit-main")


def parse_args():
    product_choices = ["l0hosc", "l0obs", "l1aeng", "l1asci", "l1araw", "l1bcal", "l2arefl", "l2amask", "l2babun"]
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--acquisition_id", default="",
                        help="Acquisition ID")
    parser.add_argument("-s", "--stream_path", default="",
                        help="Path to HOSC or CCSDS stream file")
    parser.add_argument("-c", "--config_path",
                        help="Path to config file")
    parser.add_argument("-p", "--products",
                        help=("Comma delimited list of products to create (no spaces). \
                        Choose from " + ", ".join(product_choices)))
    parser.add_argument("-w", "--workers", default=2,
                        help="Number of luigi workers")
    parser.add_argument("--build_env", action="store_true",
                        help="Build the runtime environment (primarily used to setup dev environments)")
    parser.add_argument("--checkout_build", action="store_true",
                        help="Checks out all repos and tags for a given build")
    args = parser.parse_args()

    if args.config_path is None:
        print("ERROR: You must specify a configuration file with the --config_path argument.")
        sys.exit(1)

    args.config_path = os.path.abspath(args.config_path)

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
    stream_kwargs = {
        "config_path": args.config_path,
        "stream_path": args.stream_path
    }
    acquisition_kwargs = {
        "config_path": args.config_path,
        "acquisition_id": args.acquisition_id
    }

    prod_task_map = {
        "l0hosc": L0StripHOSC(**stream_kwargs),
        "l0obs": L0ProcessObservationsProduct(config_path=args.config_path),
        "l1aeng": L1AReformatEDP(**stream_kwargs),
        "l1asci": L1AReadScienceFrames(**stream_kwargs),
        "l1araw": L1AReassembleRaw(**acquisition_kwargs),
        "l1bcal": L1BCalibrate(**acquisition_kwargs),
        "l2arefl": L2AReflectance(**acquisition_kwargs),
        "l2amask": L2AMask(**acquisition_kwargs),
        "l2babun": L2BAbundance(**acquisition_kwargs)
    }

    tasks = []
    for prod in products:
        tasks.append(prod_task_map[prod])
    return tasks


@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    logger.info("SUCCESS: %s" % task)

    # TODO: Delete tmp folder
#    logger.debug("Deleting tmp folder %s" % task.tmp_dir)
#    shutil.rmtree(task.tmp_dir)

    # TODO: Trigger higher level tasks?


@SlurmJobTask.event_handler(luigi.Event.FAILURE)
def task_failure(task, e):
    # TODO: If additional debugging is needed, change exc_info to True
    logger.error("FAILURE: %s failed with exception %s" % (task, str(e)), exc_info=False)

    # Move tmp folder to errors folder
    error_dir = task.tmp_dir.replace("/tmp/", "/error/")
    logger.error("Moving tmp folder %s to %s" % (task.tmp_dir, error_dir))
    shutil.move(task.tmp_dir, error_dir)

    # Update DB processing_log with failure message
    if task.task_family == "emit.L1AReassembleRaw":
        wm = WorkflowManager(task.config_path, task.acquisition_id)
        acq = wm.acquisition
        pge = wm.pges["emit-sds-l1a"]
        log_entry = {
            "task": task.task_family,
            "pge_name": pge.repo_url,
            "pge_version": pge.version_tag,
            "pge_input_files": {
                "file1_key": "file1_value",
                "file2_key": "file2_value",
            },
            "pge_run_command": "python l1a_run.py args",
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "FAILURE",
            "error_message": str(e)
        }
        acq.save_processing_log_entry(log_entry)
    if task.task_family in ("emit.L0StripHOSC", "emit.L1AReformatEDP"):
        log_entry = {
            "task": task.task_family,
            "log_timestamp": datetime.datetime.now(),
            "completion_status": "FAILURE",
            "error_message": str(e)
        }
        dm = WorkflowManager(task.config_path, task.acquisition_id).database_manager
        dm.insert_stream_log_entry(os.path.basename(task.stream_path), log_entry)


def set_up_logging(logs_dir):
    # Add file handler logging to main logs directory
    handler = logging.FileHandler(os.path.join(logs_dir, "workflow.log"))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    """
    Parse command line arguments and initiate tasks
    """
    args = parse_args()
    wm = WorkflowManager(config_path=args.config_path)
    set_up_logging(wm.logs_dir)
    logger.info("Running workflow with cmd: %s" % str(" ".join(sys.argv)))

    # Check out code if requested
    if args.checkout_build:
        wm.checkout_repos_for_build()
        logger.info("Exiting after checking out repos for this build.")
        sys.exit(0)

    # Build the environment if requested
    if args.build_env:
        wm.build_runtime_environment()
        logger.info("Exiting after building runtime environment.")
        sys.exit(0)

    # Set up tasks and run
    tasks = get_tasks_from_args(args)
    if args.workers:
        workers = args.workers
    else:
        workers = wm.luigi_workers
    # Build luigi logging.conf path
    luigi_logging_conf = os.path.join(os.path.dirname(__file__), "workflow", "luigi", "logging.conf")

    luigi.build(tasks, workers=workers, local_scheduler=wm.luigi_local_scheduler,
                logging_conf_file=luigi_logging_conf)


if __name__ == '__main__':
    main()
