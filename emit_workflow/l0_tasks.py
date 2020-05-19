"""
This code contains tasks for executing EMIT Level 0 PGEs and helper utilities.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import luigi

from emit_workflow.file_manager import FileManager

# TODO: Full implementation TBD
class L0StripEthernet(luigi.Task):
    """
    Strips HOSC ethernet headers from raw data in ingest folder
    :returns Ordered APID specific packet stream
    """

    apid = luigi.Parameter()
    start_time = luigi.DateSecondParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_time = luigi.DateSecondParameter(default=datetime.date.today())

    task_namespace = "emit"

    def requires(self):

        return None

    def output(self):

        fm = FileManager()
        return luigi.LocalTarget("ccsds_path")

    def run(self):

        pass
