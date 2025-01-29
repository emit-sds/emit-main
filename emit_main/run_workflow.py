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

from argparse import RawTextHelpFormatter

import luigi

from emit_main.monitor.acquisition_monitor import AcquisitionMonitor
from emit_main.monitor.email_monitor import EmailMonitor
from emit_main.monitor.frames_monitor import FramesMonitor
from emit_main.monitor.ingest_monitor import IngestMonitor
from emit_main.monitor.orbit_monitor import OrbitMonitor
from emit_main.workflow.daac_helper_tasks import AssignDAACSceneNumbers, GetAdditionalMetadata, ReconciliationReport
from emit_main.workflow.l0_tasks import L0StripHOSC, L0ProcessPlanningProduct, L0IngestBAD, L0Deliver
from emit_main.workflow.l1a_tasks import L1ADepacketizeScienceFrames, L1AReassembleRaw, L1AReformatEDP, \
    L1AFrameReport, L1AReformatBAD, L1ADeliver
from emit_main.workflow.l1b_tasks import L1BGeolocate, L1BCalibrate, L1BRdnFormat, L1BRdnDeliver, L1BAttDeliver
from emit_main.workflow.l2a_tasks import L2AMask, L2AReflectance, L2AFormat, L2ADeliver
from emit_main.workflow.l2b_tasks import L2BAbundance, L2BFormat, L2BDeliver
from emit_main.workflow.l3_tasks import L3Unmix
from emit_main.workflow.ghg_tasks import CH4, CO2
from emit_main.workflow.slurm import SlurmJobTask
from emit_main.workflow.workflow_manager import WorkflowManager

logging_conf = os.path.join(os.path.dirname(__file__), "logging.conf")
logging.config.fileConfig(fname=logging_conf)
logger = logging.getLogger("emit-main")


def parse_args():
    product_choices = ["l0hosc", "l0daac", "l0plan", "l0bad", "l1aeng", "l1aframe", "l1aframereport", "l1araw",
                       "l1adaac", "l1abad", "l1bcal", "l1bgeo", "l1brdnformat", "l1brdndaac", "l1battdaac", "l2arefl",
                       "l2amask", "l2aformat", "l2adaac", "l2babun", "l2bformat", "l2bdaac", "l2bch4", "l2bco2",
                       "l2bch4daac", "l2bco2daac", "l3unmix", "daacscenes", "daacaddl", "recon"]
    monitor_choices = ["ingest", "frames", "edp", "cal", "bad", "geo", "l2", "l2b","ch4", "co2", "l3",
                       "email", "daacscenes", "dl0","dl1a", "dl1brdn", "dl1batt", "dl2a", "dl2b", "dch4", "dco2",
                       "reconresp"]
    parser = argparse.ArgumentParser(
        description="Description: This is the top-level run script for executing the various EMIT SDS workflow and "
                    "monitor tasks.\n"
                    "Operating Environment: Python 3.x. See setup.py file for specific dependencies.\n"
                    "Outputs: See list of product choices.",
        formatter_class=RawTextHelpFormatter)
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
                        help="Orbit number in the padded format XXXXXXX")
    parser.add_argument("--plan_prod_path", default="",
                        help="Path to planning product file")
    parser.add_argument("--recon_resp_path",
                        help="Path to reconciliation response file")
    parser.add_argument("-p", "--products",
                        help=("Comma delimited list of products to create (no spaces). "
                              "Choose from " + ", ".join(product_choices)))
    parser.add_argument("-l", "--level", default="INFO",
                        help="The log level (default: INFO)")
    parser.add_argument("--partition", default="emit",
                        help="The slurm partition to be used - emit (default), debug, standard, patient ")
    parser.add_argument("--start_time",
                        help="The start time to use for any monitor calls (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--stop_time",
                        help="The stop time to use for any monitor calls (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--date_field", default="last_modified",
                        help="The date field for the monitors to query")
    parser.add_argument("--retry_failed", action="store_true",
                        help="A flag to tell the monitors to retry failed tasks.")
    parser.add_argument("--pkt_format", default="1.3",
                        help="Flight software version to use for CCSDS depacketization format")
    parser.add_argument("--miss_pkt_thresh", default="0.01",
                        help="The threshold of missing packets to total packets which will cause a task to fail")
    parser.add_argument("--ignore_missing_frames", action="store_true",
                        help="Ignore missing frames when reasssembling raw cube")
    parser.add_argument("--acq_chunksize", default=1280,
                        help="The number of lines in which to split acquisitions")
    parser.add_argument("--dark_path", default="",
                        help="Path to dark file to use for L1B calibration")
    parser.add_argument("--use_future_flat", action="store_true",
                        help="Use future flat fields for destriping")
    parser.add_argument("--ignore_missing_bad", action="store_true",
                        help="Ignore missing BAD data in an orbit when reformatting BAD")
    parser.add_argument("--ignore_missing_radiance", action="store_true",
                        help="Ignore missing radiance files in an orbit when doing geolocation")
    parser.add_argument("--daac_ingest_queue", default="forward",
                        help="Options are 'forward' or 'backward' depending on which DAAC ingestion queue to use.")
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

    # Add 2-digit year to orbit id if only 5 digits
    if args.orbit_id and len(args.orbit_id) == 5:
        year = datetime.datetime.now().strftime("%y")
        args.orbit_id = year + args.orbit_id

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

    if (args.start_time is None and args.stop_time is not None) or \
            (args.stop_time is None and args.start_time is not None):
        print("ERROR: You must provide both start and stop time if one is given.")
        sys.exit(1)

    if args.stop_time:
        try:
            args.stop_time = datetime.datetime.strptime(args.stop_time, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            print("ERROR: Unable to get date from stop_time arg")
            sys.exit(1)
    else:
        # Default to UTC now
        args.stop_time = datetime.datetime.now(tz=datetime.timezone.utc)

    if args.start_time:
        try:
            args.start_time = datetime.datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            print("ERROR: Unable to get date from start_time arg")
            sys.exit(1)
    else:
        # Default to one day before UTC now
        args.start_time = args.stop_time - datetime.timedelta(days=1)

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
        "l0daac": L0Deliver(stream_path=args.stream_path, daac_ingest_queue=args.daac_ingest_queue,
                            override_output=args.override_output, **kwargs),
        "l0plan": L0ProcessPlanningProduct(plan_prod_path=args.plan_prod_path, test_mode=args.test_mode, **kwargs),
        "l0bad": L0IngestBAD(stream_path=args.stream_path, **kwargs),
        "l1aeng": L1AReformatEDP(stream_path=args.stream_path, pkt_format=args.pkt_format,
                                 miss_pkt_thresh=args.miss_pkt_thresh, **kwargs),
        "l1aframe": L1ADepacketizeScienceFrames(stream_path=args.stream_path,
                                                pkt_format=args.pkt_format,
                                                miss_pkt_thresh=args.miss_pkt_thresh,
                                                override_output=args.override_output,
                                                **kwargs),
        "l1aframereport": L1AFrameReport(dcid=args.dcid, ignore_missing_frames=args.ignore_missing_frames,
                                         acq_chunksize=args.acq_chunksize, test_mode=args.test_mode, **kwargs),
        "l1araw": L1AReassembleRaw(dcid=args.dcid, ignore_missing_frames=args.ignore_missing_frames,
                                   acq_chunksize=args.acq_chunksize, test_mode=args.test_mode, **kwargs),
        "l1adaac": L1ADeliver(acquisition_id=args.acquisition_id, daac_ingest_queue=args.daac_ingest_queue,
                              override_output=args.override_output, **kwargs),
        "l1abad": L1AReformatBAD(orbit_id=args.orbit_id, ignore_missing_bad=args.ignore_missing_bad, **kwargs),
        "l1bcal": L1BCalibrate(acquisition_id=args.acquisition_id, dark_path=args.dark_path,
                               use_future_flat=args.use_future_flat, **kwargs),
        "l1bgeo": L1BGeolocate(orbit_id=args.orbit_id, ignore_missing_radiance=args.ignore_missing_radiance, **kwargs),
        "l1brdnformat": L1BRdnFormat(acquisition_id=args.acquisition_id, **kwargs),
        "l1brdndaac": L1BRdnDeliver(acquisition_id=args.acquisition_id, daac_ingest_queue=args.daac_ingest_queue,
                                    override_output=args.override_output, **kwargs),
        "l1battdaac": L1BAttDeliver(orbit_id=args.orbit_id, daac_ingest_queue=args.daac_ingest_queue,
                                    override_output=args.override_output, **kwargs),
        "l2arefl": L2AReflectance(acquisition_id=args.acquisition_id, **kwargs),
        "l2amask": L2AMask(acquisition_id=args.acquisition_id, **kwargs),
        "l2aformat": L2AFormat(acquisition_id=args.acquisition_id, **kwargs),
        "l2adaac": L2ADeliver(acquisition_id=args.acquisition_id, daac_ingest_queue=args.daac_ingest_queue,
                              override_output=args.override_output, **kwargs),
        "l2babun": L2BAbundance(acquisition_id=args.acquisition_id, **kwargs),
        "l2bformat": L2BFormat(acquisition_id=args.acquisition_id, **kwargs),
        "l2bdaac": L2BDeliver(acquisition_id=args.acquisition_id, daac_ingest_queue=args.daac_ingest_queue,
                              override_output=args.override_output, **kwargs),
        "l2bch4":  CH4(acquisition_id=args.acquisition_id, **kwargs),
        "l2bco2":  CO2(acquisition_id=args.acquisition_id, **kwargs),
        "l3unmix": L3Unmix(acquisition_id=args.acquisition_id, **kwargs),
        # "l3unmixformat": L3UnmixFormat(acquisition_id=args.acquisition_id, **kwargs)
        "daacscenes": AssignDAACSceneNumbers(orbit_id=args.orbit_id, override_output=args.override_output, **kwargs),
        "daacaddl": GetAdditionalMetadata(acquisition_id=args.acquisition_id, **kwargs),
        "recon": ReconciliationReport(start_time=args.start_time.strftime("%Y%m%dT%H%M%S"),
                                      stop_time=args.stop_time.strftime("%Y%m%dT%H%M%S"), **kwargs)
    }
    tasks = []
    for prod in products:
        tasks.append(prod_task_map[prod])
    return tasks


@SlurmJobTask.event_handler(luigi.Event.SUCCESS)
def task_success(task):
    logger.info("SUCCESS: %s" % task)
    wm = WorkflowManager(config_path=task.config_path)

    # If running L0ProcessPlanningProduct then copy the job.out file back to /store
    if task.task_family == "emit.L0ProcessPlanningProduct":
        tmp_log_path = os.path.join(task.tmp_dir, "job.out")
        target_pge_log_path = os.path.join(wm.planning_products_dir,
                                           os.path.basename(task.plan_prod_path).replace(
                                               ".json", f"_b{wm.config['build_num']}_pge.log"))
        if os.path.exists(tmp_log_path):
            wm.copy(tmp_log_path, target_pge_log_path)

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

    stream_tasks = ("emit.L0StripHOSC", "emit.L1ADepacketizeScienceFrames", "emit.L1AReformatEDP", "emit.L0IngestBAD",
                    "emit.L0Deliver")
    data_collection_tasks = ("emit.L1AReassembleRaw", "emit.L1AFrameReport")
    acquisition_tasks = ("emit.L1ADeliver", "emit.L1BCalibrate", "emit.L1BRdnFormat", "emit.L1BRdnDeliver",
                         "emit.L2AReflectance", "emit.L2AMask", "emit.L2AFormat", "emit.L2ADeliver",
                         "emit.L2BAbundance", "emit.L2BFormat", "emit.L2BDeliver", "emit.L3Unmix",
                         "emit.GetAdditionalMetadata")
    orbit_tasks = ("emit.L1AReformatBAD", "emit.L1BGeolocate", "emit.L1BAttDeliver", "emit.AssignDAACSceneNumbers")

    dm = wm.database_manager
    if task.task_family in acquisition_tasks and dm.find_acquisition_by_id(task.acquisition_id) is not None:
        dm.insert_acquisition_log_entry(task.acquisition_id, log_entry)
    elif task.task_family in stream_tasks and dm.find_stream_by_name(os.path.basename(task.stream_path)):
        dm.insert_stream_log_entry(os.path.basename(task.stream_path), log_entry)
    elif task.task_family in data_collection_tasks and dm.find_data_collection_by_id(task.dcid):
        dm.insert_data_collection_log_entry(task.dcid, log_entry)
    elif task.task_family in orbit_tasks and dm.find_orbit_by_id(task.orbit_id):
        dm.insert_orbit_log_entry(task.orbit_id, log_entry)

    # Send failure notification
    wm.send_failure_notification(task, e)


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

    if args.monitor and args.monitor == "reconresp":
        em = EmailMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                          daac_ingest_queue=args.daac_ingest_queue)
        em_tasks = em.process_daac_reconciliation_responses(
            reconciliation_response_path=args.recon_resp_path, retry_failed=args.retry_failed)
        em_tasks_str = "\n".join([str(t) for t in em_tasks])
        logger.info(f"Email monitor reconciliation response tasks to run:\n{em_tasks_str}")
        tasks += em_tasks

    # Get tasks from ingest monitor
    if args.monitor and args.monitor == "ingest":
        im = IngestMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                           pkt_format=args.pkt_format, miss_pkt_thresh=args.miss_pkt_thresh, test_mode=args.test_mode)
        im_tasks = im.ingest_files()
        im_tasks_str = "\n".join([str(t) for t in im_tasks])
        logger.info(f"Ingest monitor tasks to run:\n{im_tasks_str}")
        tasks += im_tasks

    # Get tasks from frames monitor
    if args.monitor and args.monitor == "frames":
        fm = FramesMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                           acq_chunksize=args.acq_chunksize, test_mode=args.test_mode)
        fm_tasks = fm.get_reassembly_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                           date_field=args.date_field, retry_failed=args.retry_failed)
        fm_tasks_str = "\n".join([str(t) for t in fm_tasks])
        logger.info(f"Frames monitor tasks to run:\n{fm_tasks_str}")
        tasks += fm_tasks

    # Get tasks from edp monitor
    if args.monitor and args.monitor == "edp":
        im = IngestMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                           pkt_format=args.pkt_format, miss_pkt_thresh=args.miss_pkt_thresh, test_mode=args.test_mode)
        im_edp_tasks = im.get_edp_reformatting_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                     date_field=args.date_field, retry_failed=args.retry_failed)
        im_edp_tasks_str = "\n".join([str(t) for t in im_edp_tasks])
        logger.info(f"Ingest monitor EDP tasks to run:\n{im_edp_tasks_str}")
        tasks += im_edp_tasks

    # Get tasks from orbit monitor for BAD tasks
    if args.monitor and args.monitor == "bad":
        om = OrbitMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        om_bad_tasks = om.get_bad_reformatting_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                     date_field=args.date_field, retry_failed=args.retry_failed)
        om_bad_tasks_str = "\n".join([str(t) for t in om_bad_tasks])
        logger.info(f"Orbit monitor BAD tasks to run:\n{om_bad_tasks_str}")
        tasks += om_bad_tasks

    # Get tasks from orbit monitor for geolocation tasks
    if args.monitor and args.monitor == "geo":
        om = OrbitMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        om_geo_tasks = om.get_geolocation_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                date_field=args.date_field, retry_failed=args.retry_failed)
        om_geo_tasks_str = "\n".join([str(t) for t in om_geo_tasks])
        logger.info(f"Orbit monitor geolocation tasks to run:\n{om_geo_tasks_str}")
        tasks += om_geo_tasks

    # Get tasks from acquisition monitor for calibration tasks
    if args.monitor and args.monitor == "cal":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        am_cal_tasks = am.get_calibration_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                date_field=args.date_field, retry_failed=args.retry_failed)
        am_cal_tasks_str = "\n".join([str(t) for t in am_cal_tasks])
        logger.info(f"Acquisition monitor calibration tasks to run:\n{am_cal_tasks_str}")
        tasks += am_cal_tasks

    # Get tasks from acquisition monitor for L2 tasks
    if args.monitor and args.monitor == "l2":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        am_l2_tasks = am.get_l2_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                      date_field=args.date_field, retry_failed=args.retry_failed)
        am_l2_tasks_str = "\n".join([str(t) for t in am_l2_tasks])
        logger.info(f"Acquisition monitor L2 tasks to run:\n{am_l2_tasks_str}")
        tasks += am_l2_tasks

    # Get tasks from acquisition monitor for L2b tasks
    if args.monitor and args.monitor == "l2b":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        am_l2b_tasks = am.get_l2b_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                        date_field=args.date_field, retry_failed=args.retry_failed)
        am_l2b_tasks_str = "\n".join([str(t) for t in am_l2b_tasks])
        logger.info(f"Acquisition monitor L2B tasks to run:\n{am_l2b_tasks_str}")
        tasks += am_l2b_tasks

    # Get tasks from acquisition monitor for ch4 tasks
    if args.monitor and args.monitor == "ch4":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        am_ch4_tasks = am.get_ch4_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                        date_field=args.date_field, retry_failed=args.retry_failed)
        am_ch4_tasks_str = "\n".join([str(t) for t in am_ch4_tasks])
        logger.info(f"Acquisition monitor CH4 tasks to run:\n{am_ch4_tasks_str}")
        tasks += am_ch4_tasks

    # Get tasks from acquisition monitor for co2 tasks
    if args.monitor and args.monitor == "co2":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        am_co2_tasks = am.get_co2_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                        date_field=args.date_field, retry_failed=args.retry_failed)
        am_co2_tasks_str = "\n".join([str(t) for t in am_co2_tasks])
        logger.info(f"Acquisition monitor CO2 tasks to run:\n{am_co2_tasks_str}")
        tasks += am_co2_tasks

    # Get tasks from acquisition monitor for L3 tasks
    if args.monitor and args.monitor == "l3":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        am_l3_tasks = am.get_l3_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                      date_field=args.date_field, retry_failed=args.retry_failed)
        am_l3_tasks_str = "\n".join([str(t) for t in am_l3_tasks])
        logger.info(f"Acquisition monitor L3 tasks to run:\n{am_l3_tasks_str}")
        tasks += am_l3_tasks

    # Get tasks from daacscenes monitor
    if args.monitor and args.monitor == "daacscenes":
        om = OrbitMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
        om_scenes_tasks = om.get_daac_scenes_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                   date_field=args.date_field, retry_failed=args.retry_failed)
        om_scenes_tasks_str = "\n".join([str(t) for t in om_scenes_tasks])
        logger.info(f"Orbit monitor DAAC scene number tasks to run:\n{om_scenes_tasks_str}")
        tasks += om_scenes_tasks

    # Get tasks from dl0 (deliver l0) monitor
    if args.monitor and args.monitor == "dl0":
        im = IngestMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                           daac_ingest_queue=args.daac_ingest_queue)
        im_dl0_tasks = im.get_l0_delivery_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                date_field=args.date_field, retry_failed=args.retry_failed)
        im_dl0_tasks_str = "\n".join([str(t) for t in im_dl0_tasks])
        logger.info(f"Ingest monitor deliver l0 tasks to run:\n{im_dl0_tasks_str}")
        tasks += im_dl0_tasks

    # Get tasks from dl1a (deliver l1a) monitor
    if args.monitor and args.monitor == "dl1a":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                                daac_ingest_queue=args.daac_ingest_queue)
        am_dl1a_tasks = am.get_l1a_delivery_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                  date_field=args.date_field, retry_failed=args.retry_failed)
        am_dl1a_tasks_str = "\n".join([str(t) for t in am_dl1a_tasks])
        logger.info(f"Acquisition monitor deliver l1a tasks to run:\n{am_dl1a_tasks_str}")
        tasks += am_dl1a_tasks

    # Get tasks from dl1brdn (deliver dl1brdn) monitor
    if args.monitor and args.monitor == "dl1brdn":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                                daac_ingest_queue=args.daac_ingest_queue)
        am_dl1brdn_tasks = am.get_l1brdn_delivery_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                        date_field=args.date_field, retry_failed=args.retry_failed)
        am_dl1brdn_tasks_str = "\n".join([str(t) for t in am_dl1brdn_tasks])
        logger.info(f"Acquisition monitor deliver l1b radiance tasks to run:\n{am_dl1brdn_tasks_str}")
        tasks += am_dl1brdn_tasks

    # Get tasks from dl1batt (deliver l1b att/eph) monitor
    if args.monitor and args.monitor == "dl1batt":
        om = OrbitMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                          daac_ingest_queue=args.daac_ingest_queue)
        om_dl1batt_tasks = om.get_l1batt_delivery_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                        date_field=args.date_field, retry_failed=args.retry_failed)
        om_dl1batt_tasks_str = "\n".join([str(t) for t in om_dl1batt_tasks])
        logger.info(f"Orbit monitor deliver l1batt tasks to run:\n{om_dl1batt_tasks_str}")
        tasks += om_dl1batt_tasks

    # Get tasks from dl2a (deliver l2a) monitor
    if args.monitor and args.monitor == "dl2a":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                                daac_ingest_queue=args.daac_ingest_queue)
        am_dl2a_tasks = am.get_l2a_delivery_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                  date_field=args.date_field, retry_failed=args.retry_failed)
        am_dl2a_tasks_str = "\n".join([str(t) for t in am_dl2a_tasks])
        logger.info(f"Acquisition monitor deliver l2a reflectance tasks to run:\n{am_dl2a_tasks_str}")
        tasks += am_dl2a_tasks

    # Get tasks from dl2b (deliver l2b) monitor
    if args.monitor and args.monitor == "dl2b":
        am = AcquisitionMonitor(config_path=args.config_path, level=args.level, partition=args.partition,
                                daac_ingest_queue=args.daac_ingest_queue)
        am_dl2b_tasks = am.get_l2b_delivery_tasks(start_time=args.start_time, stop_time=args.stop_time,
                                                  date_field=args.date_field, retry_failed=args.retry_failed)
        am_dl2b_tasks_str = "\n".join([str(t) for t in am_dl2b_tasks])
        logger.info(f"Acquisition monitor deliver l2b abundance tasks to run:\n{am_dl2b_tasks_str}")
        tasks += am_dl2b_tasks

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
        workers = min(40, len(tasks))
    else:
        workers = wm.config["luigi_workers"]

    # If it's a dry run just print the tasks and exit
    if args.dry_run:
        tasks_str = "\n".join([str(t) for t in tasks])
        logger.info(f"Dry run flag set. Below are the {len(tasks)} tasks that Luigi would run:\n{tasks_str}")
        sys.exit(0)

    # Build luigi logging.conf path
    luigi_logging_conf = os.path.join(os.path.dirname(__file__), "workflow", "luigi", "logging.conf")

    luigi.build(tasks, workers=workers, local_scheduler=wm.config["luigi_local_scheduler"],
                logging_conf_file=luigi_logging_conf)


if __name__ == '__main__':
    main()
