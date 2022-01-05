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

from emit_main.monitor.email_monitor import EmailMonitor
from emit_main.monitor.frames_monitor import FramesMonitor
from emit_main.monitor.ingest_monitor import IngestMonitor
from emit_main.monitor.orbit_monitor import OrbitMonitor
from emit_main.workflow.l0_tasks import L0StripHOSC, L0ProcessPlanningProduct
from emit_main.workflow.l1a_tasks import L1ADepacketizeScienceFrames, L1AReassembleRaw, L1AReformatEDP, \
    L1AFrameReport, L1AReformatBAD
from emit_main.workflow.l1b_tasks import L1BGeolocate, L1BCalibrate, L1BFormat, L1BDeliver
from emit_main.workflow.l2a_tasks import L2AMask, L2AReflectance, L2AFormat
from emit_main.workflow.l2b_tasks import L2BAbundance, L2BFormat
from emit_main.workflow.l3_tasks import L3Unmix
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

logging_conf = os.path.join(os.path.dirname(__file__), "logging.conf")
logging.config.fileConfig(fname=logging_conf)
logger = logging.getLogger("emit-main")


def parse_args():
    product_choices = ["l0hosc", "l0plan", "l1aeng", "l1aframe", "l1aframereport", "l1araw", "l1abad", "l1bcal",
                       "l1bformat", "l1bdaac", "l2arefl", "l2amask", "l2aformat", "l2babun", "l2bformat", "l3unmix"]
    monitor_choices = ["ingest", "frames", "orbit", "email"]
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_path",
                        help="Path to config file")
    parser.add_argument("-m", "--monitor",
                        help=("Which monitor to run. Choose from " + ", ".join(monitor_choices)))
    parser.add_argument("-a", "--acquisition_id", default="",
                        help="Acquisition ID")
    parser.add_argument("-d", "--dcid", default="",
                        help="Data Collection ID")
    parser.add_argument("-s", "--stream_path", default="",
                        help="Path to HOSC or CCSDS stream file")
    parser.add_argument("-o", "--orbit_id", default="",
                        help="Orbit number in the padded format XXXXX")
    parser.add_argument("--plan_prod_path", default="",
                        help="Path to planning product file")
    parser.add_argument("-p", "--products",
                        help=("Comma delimited list of products to create (no spaces). "
                              "Choose from " + ", ".join(product_choices)))
    parser.add_argument("-l", "--level", default="INFO",
                        help="The log level (default: INFO)")
    parser.add_argument("--partition", default="emit",
                        help="The slurm partition to be used - emit (default), debug, standard, patient ")
    parser.add_argument("--miss_pkt_thresh", default="0.1",
                        help="The threshold of missing packets to total packets which will cause a task to fail")
    parser.add_argument("--ignore_missing_frames", action="store_true",
                        help="Ignore missing frames when reasssembling raw cube")
    parser.add_argument("--acq_chunksize", default=1280,
                        help="The number of lines in which to split acquisitions")
    parser.add_argument("--ignore_missing_bad", action="store_true",
                        help="Ignore missing BAD data in an orbit when reformatting BAD")
    parser.add_argument("--dry_run", action="store_true",
                        help="Just return a list of paths to process from the ingest folder, but take no action")
    parser.add_argument("--test_mode", action="store_true",
                        help="Allows tasks to skip work during I&T by skipping certain checks")
    parser.add_argument("--override_output", action="store_true",
                        help="Ignore outputs of a task and run it on demand")
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

    if args.monitor and args.monitor not in monitor_choices:
        print("ERROR: Monitor \"%s\" is not a valid monitor choice." % args.monitor)
        sys.exit(1)

    return args


def get_tasks_from_product_args(args):
    products = args.products.split(",")
    kwargs = {
        "config_path": args.config_path,
        "level": args.level,
        "partition": args.partition
    }
    prod_task_map = {
        "l0hosc": L0StripHOSC(stream_path=args.stream_path, miss_pkt_thresh=args.miss_pkt_thresh,
                              **kwargs),
        "l0plan": L0ProcessPlanningProduct(plan_prod_path=args.plan_prod_path, **kwargs),
        "l1aeng": L1AReformatEDP(stream_path=args.stream_path, miss_pkt_thresh=args.miss_pkt_thresh,
                                 **kwargs),
        "l1aframe": L1ADepacketizeScienceFrames(stream_path=args.stream_path,
                                                miss_pkt_thresh=args.miss_pkt_thresh,
                                                override_output=args.override_output,
                                                **kwargs),
        "l1aframereport": L1AFrameReport(dcid=args.dcid, ignore_missing_frames=args.ignore_missing_frames,
                                         acq_chunksize=args.acq_chunksize, test_mode=args.test_mode, **kwargs),
        "l1araw": L1AReassembleRaw(dcid=args.dcid, ignore_missing_frames=args.ignore_missing_frames,
                                   acq_chunksize=args.acq_chunksize, test_mode=args.test_mode, **kwargs),
        "l1abad": L1AReformatBAD(orbit_id=args.orbit_id, ignore_missing_bad=args.ignore_missing_bad, **kwargs),
        "l1bcal": L1BCalibrate(acquisition_id=args.acquisition_id, **kwargs),
        "l1bformat": L1BFormat(acquisition_id=args.acquisition_id, **kwargs),
        "l1bdaac": L1BDeliver(acquisition_id=args.acquisition_id, **kwargs),
        "l2arefl": L2AReflectance(acquisition_id=args.acquisition_id, **kwargs),
        "l2amask": L2AMask(acquisition_id=args.acquisition_id, **kwargs),
        "l2aformat": L2AFormat(acquisition_id=args.acquisition_id, **kwargs),
        "l2babun": L2BAbundance(acquisition_id=args.acquisition_id, **kwargs),
        "l2bformat": L2BFormat(acquisition_id=args.acquisition_id, **kwargs),
        "l3unmix": L3Unmix(acquisition_id=args.acquisition_id, **kwargs),
        # "l2aformat": L2AFormat(acquisition_id=args.acquisition_id, **kwargs),
        # "l3unmixformat": L3UnmixFormat(acquisition_id=args.acquisition_id, **kwargs)
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
        logger.error(f"Moving bad HOSC file to {ingest_errors_path}")
        wm.move(task.stream_path, ingest_errors_path)

    # If running L0ProcessPlanningProduct on ingest folder path, move file to ingest/errors
    if task.task_family == "emit.L0ProcessPlanningProduct" and "ingest" in task.plan_prod_path:
        # Move planning product to ingest/errors
        ingest_errors_path = os.path.join(wm.ingest_errors_dir, os.path.basename(task.plan_prod_path))
        logger.error(f"Moving bad Planning Product file to {ingest_errors_path}")
        wm.move(task.plan_prod_path, ingest_errors_path)

    # If running L0IngestBAD on ingest folder path, move file to ingest/errors
    if task.task_family == "emit.L0IngestBAD" and "ingest" in task.stream_path:
        # Move BAD STO file to ingest/errors
        ingest_errors_path = os.path.join(wm.ingest_errors_dir, os.path.basename(task.stream_path))
        logger.error(f"Moving erroneous BAD STO file to {ingest_errors_path}")
        wm.move(task.stream_path, ingest_errors_path)

    # Update DB processing_log with failure message
    log_entry = {
        "task": task.task_family,
        "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
        "completion_status": "FAILURE",
        "error_message": str(e)
    }

    stream_tasks = ("emit.L0StripHOSC", "emit.L1ADepacketizeScienceFrames", "emit.L1AReformatEDP", "emit.L0IngestBAD")
    data_collection_tasks = ("emit.L1AReassembleRaw", "emit.L1AFrameReport")
    acquisition_tasks = ("emit.L1BCalibrate", "emit.L1BFormat", "emit.L1BDeliver", "emit.L2AReflectance",
                         "emit.L2AMask", "emit.L2BAbundance", "emit.L3Unmix")
    orbit_tasks = ("emit.L1AReformatBAD")

    dm = wm.database_manager
    if task.task_family in acquisition_tasks and dm.find_acquisition_by_id(task.acquisition_id) is not None:
        dm.insert_acquisition_log_entry(task.acquisition_id, log_entry)
    elif task.task_family in stream_tasks and dm.find_stream_by_name(os.path.basename(task.stream_path)):
        dm.insert_stream_log_entry(os.path.basename(task.stream_path), log_entry)
    elif task.task_family in data_collection_tasks and dm.find_data_collection_by_id(task.dcid):
        dm.insert_data_collection_log_entry(task.dcid, log_entry)
    elif task.task_family in orbit_tasks and dm.find_orbit_by_id(task.orbit_id):
        dm.insert_orbit_log_entry(task.orbit_id, log_entry)


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

    # Check email
    if args.monitor and args.monitor == "email":
        em = EmailMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        em.process_daac_delivery_responses()
        logger.info("Exiting after checking email")
        sys.exit(0)

    # Initialize tasks list
    tasks = []

    # Get tasks from ingest monitor
    if args.monitor and args.monitor == "ingest":
        im = IngestMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                           miss_pkt_thresh=args.miss_pkt_thresh, test_mode=args.test_mode)
        im_tasks = im.ingest_files()
        im_tasks_str = "\n".join([str(t) for t in im_tasks])
        logger.info(f"Ingest monitor tasks to run:\n{im_tasks_str}")
        tasks += im_tasks

    # Get tasks from frames monitor
    if args.monitor and args.monitor == "frames":
        fm = FramesMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                           acq_chunksize=args.acq_chunksize, test_mode=args.test_mode)
        fm_tasks = fm.get_recent_reassembly_tasks()
        fm_tasks_str = "\n".join([str(t) for t in fm_tasks])
        logger.info(f"Frames monitor tasks to run:\n{fm_tasks_str}")
        tasks += fm_tasks

    # Get tasks from orbit monitor
    if args.monitor and args.monitor == "orbit":
        om = OrbitMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        om_tasks = om.get_recent_orbit_tasks()
        om_tasks_str = "\n".join([str(t) for t in om_tasks])
        logger.info(f"Orbit monitor tasks to run:\n{om_tasks_str}")
        tasks += om_tasks

    # Get tasks from products args
    if args.products:
        prod_tasks = get_tasks_from_product_args(args)
        prod_tasks_str = "\n".join([str(t) for t in prod_tasks])
        logger.info(f"Product tasks to run:\n{prod_tasks_str}")
        tasks += prod_tasks

    # Set up luigi tasks and execute
    if args.workers:
        workers = int(args.workers)
    elif len(tasks) > 0:
        workers = min(30, len(tasks))
    else:
        workers = wm.config["luigi_workers"]

    # If it's a dry run just print the tasks and exit
    if args.dry_run:
        tasks_str = "\n".join([str(t) for t in tasks])
        logger.info(f"Dry run flag set. Below are the tasks that Luigi would run:\n{tasks_str}")
        sys.exit(0)

    # Build luigi logging.conf path
    luigi_logging_conf = os.path.join(os.path.dirname(__file__), "workflow", "luigi", "logging.conf")

    luigi.build(tasks, workers=workers, local_scheduler=wm.config["luigi_local_scheduler"],
                logging_conf_file=luigi_logging_conf)


if __name__ == '__main__':
    main()
