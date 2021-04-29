"""
This code contains the DatabaseManager class that handles database updates

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
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

    def find_acquisition_by_dcid(self, dcid):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"dcid": dcid, "build_num": self.build_num})

    def insert_acquisition(self, metadata):
        if self.find_acquisition_by_id(metadata["acquisition_id"]) is None:
            metadata["creation_time"] = datetime.datetime.now()
            acquisitions_coll = self.db.acquisitions
            acquisitions_coll.insert_one(metadata)

    def update_acquisition_metadata(self, acquisition_id, metadata):
        acquisitions_coll = self.db.acquisitions
        query = {"acquisition_id": acquisition_id, "build_num": self.build_num}
        set_value = {"$set": metadata}
        acquisitions_coll.update_one(query, set_value, upsert=True)

    def update_acquisition_dimensions(self, acquisition_id, dimensions):
        meta = self.find_acquisition_by_id(acquisition_id)
        dim = meta["dimensions"]
        dim.update(dimensions)
        query = {"acquisition_id": acquisition_id, "build_num": self.build_num}
        set_value = {"$set": {"dimensions": dim}}
        acquisitions_coll = self.db.acquisitions
        acquisitions_coll.update_one(query, set_value, upsert=True)

    def insert_acquisition_log_entry(self, acquisition_id, entry):
        acquisitions_coll = self.db.acquisitions
        query = {"acquisition_id": acquisition_id, "build_num": self.build_num}
        push_value = {"$push": {"processing_log": entry}}
        acquisitions_coll.update_one(query, push_value)

    def find_stream_by_name(self, name):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.build_num}
        else:
            query = {"ccsds_name": name, "build_num": self.build_num}
        return streams_coll.find_one(query)

    def insert_hosc_stream(self, hosc_name):
        if self.find_stream_by_name(hosc_name) is None:
            tokens = hosc_name.split("_")
            apid = tokens[1]
            # Need to add first two year digits
            start_time_str = "20" + tokens[2]
            stop_time_str = "20" + tokens[3]
            start_time = datetime.datetime.strptime(start_time_str, "%Y%m%d%H%M%S")
            stop_time = datetime.datetime.strptime(stop_time_str, "%Y%m%d%H%M%S")
            metadata = {
                "apid": apid,
                "start_time": start_time,
                "stop_time": stop_time,
                "build_num": self.build_num,
                "processing_version": self.processing_version,
                "hosc_name": hosc_name,
                "processing_log": [],
                "creation_time": datetime.datetime.now()
            }
            streams_coll = self.db.streams
            streams_coll.insert_one(metadata)

    def update_stream_metadata(self, name, metadata):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.build_num}
        else:
            query = {"ccsds_name": name, "build_num": self.build_num}
        set_value = {"$set": metadata}
        streams_coll.update_one(query, set_value, upsert=True)

    def insert_stream_log_entry(self, name, entry):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.build_num}
        else:
            query = {"ccsds_name": name, "build_num": self.build_num}
        push_value = {"$push": {"processing_log": entry}}
        streams_coll.update_one(query, push_value)
