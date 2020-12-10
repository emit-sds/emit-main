"""
This code contains the main call to initiate an EMIT file monitor

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging.config

from emit_main.file_monitor.file_monitor import FileMonitor

logging.config.fileConfig(fname="logging.conf")
logger = logging.getLogger("emit-main")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_path",
                        help="Path to config file")
    parser.add_argument("--start_time",
                        help="Start time (YYMMDDhhmmss)")
    parser.add_argument("--stop_time",
                        help="Stop time (YYMMDDhhmmss)")
    args = parser.parse_args()
    return args


def main():
    """
    Parse command line arguments and start file monitor
    """
    args = parse_args()
    fm = FileMonitor(config_path=args.config_path)
    if args.start_time and args.stop_time:
        fm.ingest_files_by_time_range(args.start_time, args.stop_time)
    else:
        fm.ingest_files()


if __name__ == '__main__':
    main()