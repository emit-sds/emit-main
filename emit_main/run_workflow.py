"""
This code contains the main call to initiate an EMIT workflow

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime
import logging.config
import os
import shutil
import sys

import luigi

from emit_main.workflow.l0_tasks import L0StripHOSC, L0ProcessPlanningProduct
from emit_main.workflow.l1a_tasks import L1ADepacketizeScienceFrames, L1AReassembleRaw, L1AReformatEDP, L1AFrameReport
from emit_main.workflow.l1b_tasks import L1BGeolocate, L1BCalibrate
from emit_main.workflow.l2a_tasks import L2AMask, L2AReflectance
from emit_main.workflow.l2b_tasks import L2BAbundance
from emit_main.workflow.l3_tasks import L3Unmix
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

logging_conf = os.path.join(os.path.dirname(__file__), "logging.conf")
logging.config.fileConfig(fname=logging_conf)
logger = logging.getLogger("emit-main")


def parse_args():
    product_choices = ["l0hosc", "l0plan", "l1aeng", "l1aframe", "l1aframereport", "l1araw", "l1bcal", "l2arefl",
                       "l2amask", "l2babun", "l3unmix"]
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
    parser.add_argument("-l", "--level", default="INFO",
                        help="The log level (default: INFO)")
    parser.add_argument("--partition", default="emit",
                        help="The slurm partition to be used - emit (default), debug, standard, patient ")
    parser.add_argument("--miss_pkt_thresh", default="0.1",
                        help="The threshold of missing packets to total packets which will cause a task to fail")
    parser.add_argument("--ignore_missing", action="store_true",
                        help="Ignore missing frames when reasssembling raw cube")
    parser.add_argument("-w", "--workers",
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

    # Upper case the log level
    args.level = args.level.upper()

    args.miss_pkt_thresh = float(args.miss_pkt_thresh)

    if args.products:
        product_list = args.products.split(",")
        for prod in product_list:
            if prod not in product_choices:
                print("ERROR: Product \"%s\" is not a valid product choice." % prod)
                sys.exit(1)
    else:
        print("Please specify a product from the list: " + ", ".join(product_choices))
    return args


def get_tasks_from_args(args):
    products = args.products.split(",")
    kwargs = {
        "config_path": args.config_path,
        "level": args.level,
        "partition": args.partition
    }
    prod_task_map = {
        "l0hosc": L0StripHOSC(stream_path=args.stream_path, miss_pkt_thresh=args.miss_pkt_thresh,
                              **kwargs),
        "l0plan": L0ProcessPlanningProduct(**kwargs),
        "l1aeng": L1AReformatEDP(stream_path=args.stream_path, miss_pkt_thresh=args.miss_pkt_thresh,
                                 **kwargs),
        "l1aframe": L1ADepacketizeScienceFrames(stream_path=args.stream_path,
                                                miss_pkt_thresh=args.miss_pkt_thresh,
                                                **kwargs),
        "l1aframereport": L1AFrameReport(acquisition_id=args.acquisition_id, **kwargs),
        "l1araw": L1AReassembleRaw(acquisition_id=args.acquisition_id, ignore_missing=args.ignore_missing, **kwargs),
        "l1bcal": L1BCalibrate(acquisition_id=args.acquisition_id, **kwargs),
        "l2arefl": L2AReflectance(acquisition_id=args.acquisition_id, **kwargs),
        "l2amask": L2AMask(acquisition_id=args.acquisition_id, **kwargs),
        "l2babun": L2BAbundance(acquisition_id=args.acquisition_id, **kwargs),
        "l3unmix": L3Unmix(acquisition_id=args.acquisition_id, **kwargs)
    }
    tasks = []
    for prod in products:
        tasks.append(prod_task_map[prod])
    return tasks


@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    logger.info("SUCCESS: %s" % task)

    # If not in DEBUG mode, clean up scratch tmp and local tmp dirs
    if task.level != "DEBUG":
        logger.debug("Deleting scratch tmp folder %s" % task.tmp_dir)
        shutil.rmtree(task.tmp_dir)

        # Remove local tmp dir if exists
        if os.path.exists(task.local_tmp_dir):
            logger.debug(f"Deleting task's local tmp folder: {task.local_tmp_dir}")
            shutil.rmtree(task.local_tmp_dir)


@SlurmJobTask.event_handler(luigi.Event.FAILURE)
def task_failure(task, e):
    logger.error("TASK FAILURE: %s" % task)
    wm = WorkflowManager(config_path=task.config_path)

    # Send failure notification
    wm.send_failure_notification(task, e)

    # Move scratch tmp folder to errors folder
    error_task_dir = task.tmp_dir.replace("/tmp/", "/error/")
    logger.error("Moving scratch tmp folder %s to %s" % (task.tmp_dir, error_task_dir))
    wm.move(task.tmp_dir, error_task_dir)

    # Copy local tmp dir to error/tmp under scratch if exists
    if os.path.exists(task.local_tmp_dir):
        error_tmp_dir = error_task_dir + "_tmp"
        logger.error(f"Copying local tmp folder {task.local_tmp_dir} to {error_tmp_dir}")
        wm.copytree(task.local_tmp_dir, error_tmp_dir)
        logger.error(f"Deleting task's local tmp folder: {task.local_tmp_dir}")
        shutil.rmtree(task.local_tmp_dir)

    # If running L0StripHOSC task on ingest folder path, move file to ingest/errors
    if task.task_family == "emit.L0StripHOSC" and "ingest" in task.stream_path:
        # Move HOSC file to ingest/errors
        ingest_errors_path = os.path.join(wm.ingest_errors_dir, os.path.basename(task.stream_path))
        logger.error(f"Moving bad HOSC file to f{ingest_errors_path}")
        wm.move(task.stream_path, ingest_errors_path)

    # Update DB processing_log with failure message
    log_entry = {
        "task": task.task_family,
        "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
        "completion_status": "FAILURE",
        "error_message": str(e)
    }
    acquisition_tasks = ("emit.L1AReassembleRaw", "emit.L1AFrameReport", "emit.L1BCalibrate", "emit.L2AReflectance",
                         "emit.L2AMask", "emit.L2BAbundance", "emit.L3Unmix")
    stream_tasks = ("emit.L0StripHOSC", "emit.L1ADepacketizeScienceFrames", "emit.L1AReformatEDP")
    dm = wm.database_manager
    if task.task_family in acquisition_tasks and dm.find_acquisition_by_id(task.acquisition_id) is not None:
        dm.insert_acquisition_log_entry(task.acquisition_id, log_entry)
    elif task.task_family in stream_tasks and dm.find_stream_by_name(os.path.basename(task.stream_path)):
        dm.insert_stream_log_entry(os.path.basename(task.stream_path), log_entry)


def set_up_logging(log_path, level):
    # Add file handler logging to main logs directory
    handler = logging.FileHandler(log_path)
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    """
    Parse command line arguments and initiate tasks
    """
    args = parse_args()

    # Set up logging and change group ownership
    wm = WorkflowManager(config_path=args.config_path)
    log_path = os.path.join(wm.logs_dir, "workflow.log")
    set_up_logging(log_path, args.level)
    logger.info("Running workflow with cmd: %s" % str(" ".join(sys.argv)))
    wm.change_group_ownership(log_path)

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
        workers = wm.config["luigi_workers"]
    # Build luigi logging.conf path
    luigi_logging_conf = os.path.join(os.path.dirname(__file__), "workflow", "luigi", "logging.conf")

    luigi.build(tasks, workers=workers, local_scheduler=wm.config["luigi_local_scheduler"],
                logging_conf_file=luigi_logging_conf)


if __name__ == '__main__':
    main()
