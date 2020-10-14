"""
This code contains the DatabaseManager class that handles database updates

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json

from pymongo import MongoClient


class DatabaseManager:

    def __init__(self, config_path):
        """
        :param config_path: Path to config file containing environment settings
        """

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            self.__dict__.update(config["general_config"])
            self.__dict__.update(config["database_config"])
            self.__dict__.update(config["build_config"])

        self.client = MongoClient(self.mongodb_host, self.mongodb_port)
#        self.mongodb_db_name = self.instrument + self.environment.capitalize() + "DB" + "_v" + self.processing_version
        self.db = self.client[self.mongodb_db_name]
        return

    def find_acquisition_by_id(self, acquisition_id):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"acquisition_id": acquisition_id, "build_num": self.build_num})

    def insert_acquisition(self, acquisition_metadata):
        if self.find_acquisition_by_id(acquisition_metadata["acquisition_id"]) is None:
            acquisitions_coll = self.db.acquisitions
            acquisitions_coll.insert_one(acquisition_metadata)
