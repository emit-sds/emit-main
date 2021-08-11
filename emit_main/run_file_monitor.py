"""
This code contains the main call to initiate an EMIT file monitor

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging.config
import os
import sys

from emit_main.file_monitor.file_monitor import FileMonitor

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
                        help="The slurm partition to be used - emit (default), debug, standard, patient ")
    args = parser.parse_args()

    if args.config_path is None:
        print("ERROR: You must specify a configuration file with the --config_path argument.")
        sys.exit(1)

    args.config_path = os.path.abspath(args.config_path)

    # Upper case the log level
    args.level = args.level.upper()

    return args


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

    fm = FileMonitor(config_path=args.config_path, level=args.level, partition=args.partition)
    set_up_logging(fm.logs_dir, args.level)
    logger.info("Running file monitor with cmd: %s" % str(" ".join(sys.argv)))

    if args.start_time and args.stop_time:
        fm.ingest_files_by_time_range(args.start_time, args.stop_time)
    else:
        fm.ingest_files()


if __name__ == '__main__':
    main()
