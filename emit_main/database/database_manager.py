"""
This code contains the DatabaseManager class that handles database updates

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import pytz

from pymongo import MongoClient

from emit_main.config.config import Config


class DatabaseManager:

    def __init__(self, config_path):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = config_path

        # Get config properties
        self.config = Config(config_path).get_dictionary()

        # Set timezone from config
        self.timezone = pytz.timezone(self.config["timezone"])

        self.client = MongoClient(self.config["mongodb_host"], self.config["mongodb_port"])
        self.db = self.client[self.config["mongodb_db_name"]]

    def find_acquisition_by_id(self, acquisition_id):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"acquisition_id": acquisition_id, "build_num": self.config["build_num"]})

    def find_acquisition_by_dcid(self, dcid):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"dcid": dcid, "build_num": self.config["build_num"]})

    def insert_acquisition(self, metadata):
        if self.find_acquisition_by_id(metadata["acquisition_id"]) is None:
            metadata["created"] = self.timezone.localize(datetime.datetime.now())
            metadata["last_modified"] = self.timezone.localize(datetime.datetime.now())
            acquisitions_coll = self.db.acquisitions
            acquisitions_coll.insert_one(metadata)

    def update_acquisition_metadata(self, acquisition_id, metadata):
        acquisitions_coll = self.db.acquisitions
        query = {"acquisition_id": acquisition_id, "build_num": self.config["build_num"]}
        metadata["last_modified"] = self.timezone.localize(datetime.datetime.now())
        set_value = {"$set": metadata}
        acquisitions_coll.update_one(query, set_value, upsert=True)

    def insert_acquisition_log_entry(self, acquisition_id, entry):
        acquisitions_coll = self.db.acquisitions
        query = {"acquisition_id": acquisition_id, "build_num": self.config["build_num"]}
        push_value = {"$push": {"processing_log": entry}}
        acquisitions_coll.update_one(query, push_value)

    def find_stream_by_name(self, name):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.config["build_num"]}
        else:
            query = {"ccsds_name": name, "build_num": self.config["build_num"]}
        return streams_coll.find_one(query)

    def insert_hosc_stream(self, hosc_name):
        if self.find_stream_by_name(hosc_name) is None:
            if "_hsc.bin" not in hosc_name:
                raise RuntimeError(f"Attempting to insert HOSC stream file in DB with name {hosc_name}. Does not "
                                   f"appear to be a HOSC file")
            tokens = hosc_name.split("_")
            apid = tokens[1]
            # Need to add first two year digits
            start_time_str = "20" + tokens[2]
            stop_time_str = "20" + tokens[3]
            start_time = self.timezone.localize(datetime.datetime.strptime(start_time_str, "%Y%m%d%H%M%S"))
            stop_time = self.timezone.localize(datetime.datetime.strptime(stop_time_str, "%Y%m%d%H%M%S"))
            metadata = {
                "apid": apid,
                "start_time": start_time,
                "stop_time": stop_time,
                "build_num": self.config["build_num"],
                "processing_version": self.config["processing_version"],
                "hosc_name": hosc_name,
                "processing_log": [],
                "created": self.timezone.localize(datetime.datetime.now()),
                "last_modified": self.timezone.localize(datetime.datetime.now())
            }
            streams_coll = self.db.streams
            streams_coll.insert_one(metadata)

    def update_stream_metadata(self, name, metadata):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.config["build_num"]}
        else:
            query = {"ccsds_name": name, "build_num": self.config["build_num"]}
        metadata["last_modified"] = self.timezone.localize(datetime.datetime.now())
        set_value = {"$set": metadata}
        streams_coll.update_one(query, set_value, upsert=True)

    def insert_stream_log_entry(self, name, entry):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.config["build_num"]}
        else:
            query = {"ccsds_name": name, "build_num": self.config["build_num"]}
        push_value = {"$push": {"processing_log": entry}}
        streams_coll.update_one(query, push_value)
