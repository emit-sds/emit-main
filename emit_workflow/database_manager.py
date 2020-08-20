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
            self.__dict__.update(json.load(f)["database_config"])

        self.client = MongoClient(self.mongodb_host, self.mongodb_port)
        self.db = self.client[self.mongodb_db_name]

    def find_acquisition(self, acquisition_id):
        acquisitions = self.db.acquisitions
        return acquisitions.find_one({"_id": acquisition_id})