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
    args = parser.parse_args()
    return args


def main():
    """
    Parse command line arguments and start file monitor
    """
    args = parse_args()
    fm = FileMonitor(config_path=args.config_path)
    fm.run()


if __name__ == '__main__':
    main()
