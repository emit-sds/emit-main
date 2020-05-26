"""
This code contains the DatabaseManager class that handles database updates

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json

from pymongo import MongoClient


class DatabaseManager:

    def __init__(self, config_path, acquisition_id=None):
        """
        :param config_path: Path to config file containing environment settings
        :param acquisition_id: The name of the acquisition with timestamp (eg. "emit20200519t140035")
        """

        self.acquisition_id = acquisition_id

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            self.__dict__.update(json.load(f))

        # TODO: Check if db exists and create it if needed
        self.client = MongoClient(self.mongodb_host, self.mongodb_port)
        self.db = self.client[self.mongodb_db_name]
