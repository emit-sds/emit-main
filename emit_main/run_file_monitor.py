"""
This code contains the main call to initiate an EMIT file monitor

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime
import logging.config
import os
import shutil
import sys

import luigi

from emit_main.file_monitor.file_monitor import FileMonitor
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

logging_conf = os.path.join(os.path.dirname(__file__), "logging.conf")
logging.config.fileConfig(fname=logging_conf)
logger = logging.getLogger("emit-main")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_path",
                        help="Path to config file")
    parser.add_argument("--start_time",
                        help="Start time (YYMMDDhhmmss)")
    parser.add_argument("--stop_time",
                        help="Stop time (YYMMDDhhmmss)")
    parser.add_argument("-l", "--level", default="INFO",
                        help="The log level (default: INFO)")
    parser.add_argument("--partition", default="emit",
                        help="The slurm partition to be used - emit (default), debug, standard, patient")
    parser.add_argument("--miss_pkt_thresh", default="0.1",
                        help="The threshold of missing packets to total packets which will cause a task to fail")
    parser.add_argument("-w", "--workers",
                        help="Number of luigi workers")
    parser.add_argument("--dry_run", action="store_true",
                        help="Just return a list of paths to process from the ingest folder, but take no action")
    args = parser.parse_args()

    if args.config_path is None:
        print("ERROR: You must specify a configuration file with the --config_path argument.")
        sys.exit(1)

    args.config_path = os.path.abspath(args.config_path)

    # Upper case the log level
    args.level = args.level.upper()

    args.miss_pkt_thresh = float(args.miss_pkt_thresh)

    return args


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

    # Update DB processing_log with failure message

    log_entry = {
        "task": task.task_family,
        "log_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
        "completion_status": "FAILURE",
        "error_message": str(e)
    }
    acquisition_tasks = ("emit.L1AReassembleRaw", "emit.L1AFrameReport", "emit.L1BCalibrate", "emit.L2AReflectance",
                         "emit.L2AMask", "emit.L2BAbundance")
    stream_tasks = ("emit.L0StripHOSC", "emit.L1ADepacketizeScienceFrames", "emit.L1AReformatEDP")
    dm = wm.database_manager
    if task.task_family in acquisition_tasks and dm.find_acquisition_by_id(task.acquisition_id) is not None:
        dm.insert_acquisition_log_entry(task.acquisition_id, log_entry)
    elif task.task_family in stream_tasks and dm.find_stream_by_name(os.path.basename(task.stream_path)):
        dm.insert_stream_log_entry(os.path.basename(task.stream_path), log_entry)


def set_up_logging(logs_dir, level):
    # Add file handler logging to main logs directory
    handler = logging.FileHandler(os.path.join(logs_dir, "file_monitor.log"))
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    """
    Parse command line arguments and start file monitor
    """
    args = parse_args()

    fm = FileMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                     miss_pkt_thresh=args.miss_pkt_thresh)
    set_up_logging(fm.logs_dir, args.level)
    logger.info("Running file monitor with cmd: %s" % str(" ".join(sys.argv)))

    # Get tasks from file monitor
    dry_run = args.dry_run
    if args.start_time and args.stop_time:
        tasks = fm.ingest_files_by_time_range(args.start_time, args.stop_time, dry_run=dry_run)
    else:
        tasks = fm.ingest_files(dry_run=dry_run)

    # If it's a dry run just print the paths and exit
    if dry_run:
        logger.info("Dry run flag set. Showing list of paths to ingest:")
        logger.info("\n".join(tasks))
        sys.exit(0)

    # Set up luigi tasks and execute
    if args.workers:
        workers = args.workers
    else:
        workers = fm.config["luigi_workers"]
    luigi_logging_conf = os.path.join(os.path.dirname(__file__), "workflow", "luigi", "logging.conf")
    luigi.build(tasks, workers=workers, local_scheduler=fm.config["luigi_local_scheduler"],
                logging_conf_file=luigi_logging_conf)


if __name__ == '__main__':
    main()
