"""
This code contains the DatabaseManager class that handles database updates

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime

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

        self.client = MongoClient(self.config["db_host"], self.config["db_port"], username=self.config["db_user"],
                                  password=self.config["db_password"], authSource=self.config["db_name"],
                                  authMechanism="SCRAM-SHA-256")

        self.db = self.client[self.config["db_name"]]

    def find_acquisition_by_id(self, acquisition_id):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"acquisition_id": acquisition_id, "build_num": self.config["build_num"]})

    def find_acquisition_by_dcid(self, dcid):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"dcid": dcid, "build_num": self.config["build_num"]})

    def find_acquisitions_by_orbit_id(self, orbit_id):
        acquisitions_coll = self.db.acquisitions
        query = {"orbit": orbit_id, "build_num": self.config["build_num"]}
        return list(acquisitions_coll.find(query).sort("acquisition_id", 1))

    def find_acquisitions_touching_date_range(self, submode, field, start, stop, sort=1):
        acquisitions_coll = self.db.acquisitions
        query = {
            "submode": submode,
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        return list(acquisitions_coll.find(query).sort(field, sort))

    def insert_acquisition(self, metadata):
        if self.find_acquisition_by_id(metadata["acquisition_id"]) is None:
            utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
            metadata["created"] = utc_now
            metadata["last_modified"] = utc_now
            acquisitions_coll = self.db.acquisitions
            acquisitions_coll.insert_one(metadata)

    def update_acquisition_metadata(self, acquisition_id, metadata):
        acquisitions_coll = self.db.acquisitions
        query = {"acquisition_id": acquisition_id, "build_num": self.config["build_num"]}
        metadata["last_modified"] = datetime.datetime.now(tz=datetime.timezone.utc)
        set_value = {"$set": metadata}
        acquisitions_coll.update_one(query, set_value, upsert=True)

    def insert_acquisition_log_entry(self, acquisition_id, entry):
        entry["extended_build_num"] = self.config["extended_build_num"]
        acquisitions_coll = self.db.acquisitions
        query = {"acquisition_id": acquisition_id, "build_num": self.config["build_num"]}
        push_value = {"$push": {"processing_log": entry}}
        acquisitions_coll.update_one(query, push_value)

        # Update last modified
        metadata = {"last_modified": entry["log_timestamp"]}
        set_value = {"$set": metadata}
        acquisitions_coll.update_one(query, set_value, upsert=True)

    def find_stream_by_name(self, name):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.config["build_num"]}
        elif "ccsds" in name:
            query = {"ccsds_name": name, "build_num": self.config["build_num"]}
        elif ".sto" in name:
            query = {"bad_name": name, "build_num": self.config["build_num"]}
        return streams_coll.find_one(query)

    def find_streams_touching_date_range(self, apid, field, start, stop, sort=1):
        streams_coll = self.db.streams
        query = {
            "apid": apid,
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        return list(streams_coll.find(query).sort(field, sort))

    def find_streams_encompassing_date_range(self, apid, start_field, stop_field, query_start, query_stop, sort=1):
        streams_coll = self.db.streams
        query = {
            "apid": apid,
            start_field: {"$lt": query_start},
            stop_field: {"$gt": query_stop},
            "build_num": self.config["build_num"]
        }
        return list(streams_coll.find(query).sort(start_field, sort))

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
            # These dates are already in UTC and will be stored in the DB as UTC by default
            start_time = datetime.datetime.strptime(start_time_str, "%Y%m%d%H%M%S")
            stop_time = datetime.datetime.strptime(stop_time_str, "%Y%m%d%H%M%S")
            utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
            metadata = {
                "apid": apid,
                "start_time": start_time,
                "stop_time": stop_time,
                "build_num": self.config["build_num"],
                "processing_version": self.config["processing_version"],
                "hosc_name": hosc_name,
                "processing_log": [],
                "created": utc_now,
                "last_modified": utc_now
            }
            streams_coll = self.db.streams
            streams_coll.insert_one(metadata)

    def insert_bad_stream(self, bad_name):
        if self.find_stream_by_name(bad_name) is None:
            if ".sto" not in bad_name:
                raise RuntimeError(f"Attempting to insert BAD stream file in DB with name {bad_name}. Does not "
                                   f"appear to be a BAD STO file")
            # TODO: This format is not defined yet (e.g. emit_<start_time>_<stop_time>_<production_time>_bad.sto)
            tokens = bad_name.split(".")[0].split("_")
            start_time = datetime.datetime.strptime(tokens[1], "%Y%m%dT%H%M%S")
            stop_time = datetime.datetime.strptime(tokens[2], "%Y%m%dT%H%M%S")
            utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
            metadata = {
                "apid": "bad",
                "start_time": start_time,
                "stop_time": stop_time,
                "build_num": self.config["build_num"],
                "processing_version": self.config["processing_version"],
                "bad_name": bad_name,
                "processing_log": [],
                "created": utc_now,
                "last_modified": utc_now
            }
            streams_coll = self.db.streams
            streams_coll.insert_one(metadata)

    def update_stream_metadata(self, name, metadata):
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.config["build_num"]}
        elif "ccsds" in name:
            query = {"ccsds_name": name, "build_num": self.config["build_num"]}
        elif ".sto" in name:
            query = {"bad_name": name, "build_num": self.config["build_num"]}
        metadata["last_modified"] = datetime.datetime.now(tz=datetime.timezone.utc)
        set_value = {"$set": metadata}
        streams_coll.update_one(query, set_value, upsert=True)

    def insert_stream_log_entry(self, name, entry):
        entry["extended_build_num"] = self.config["extended_build_num"]
        streams_coll = self.db.streams
        if "hsc.bin" in name:
            query = {"hosc_name": name, "build_num": self.config["build_num"]}
        elif "ccsds" in name:
            query = {"ccsds_name": name, "build_num": self.config["build_num"]}
        elif ".sto" in name:
            query = {"bad_name": name, "build_num": self.config["build_num"]}
        push_value = {"$push": {"processing_log": entry}}
        streams_coll.update_one(query, push_value)

        # Update last modified
        metadata = {"last_modified": entry["log_timestamp"]}
        set_value = {"$set": metadata}
        streams_coll.update_one(query, set_value, upsert=True)

    def find_data_collection_by_id(self, dcid):
        data_collections_coll = self.db.data_collections
        return data_collections_coll.find_one({"dcid": dcid, "build_num": self.config["build_num"]})

    def find_data_collections_for_reassembly(self, start, stop):
        data_collections_coll = self.db.data_collections
        # Query for data collections with complete set of frames, last modified within start/stop range and
        # that don't have associated acquisitions
        query = {
            "frames_status": "complete",
            "frames_last_modified": {"$gte": start, "$lte": stop},
            "associated_acquisitions": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        return list(data_collections_coll.find(query))

    def insert_data_collection(self, metadata):
        if self.find_data_collection_by_id(metadata["dcid"]) is None:
            utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
            metadata["created"] = utc_now
            metadata["last_modified"] = utc_now
            data_collections_coll = self.db.data_collections
            data_collections_coll.insert_one(metadata)

    def update_data_collection_metadata(self, dcid, metadata):
        data_collections_coll = self.db.data_collections
        query = {"dcid": dcid, "build_num": self.config["build_num"]}
        metadata["last_modified"] = datetime.datetime.now(tz=datetime.timezone.utc)
        set_value = {"$set": metadata}
        data_collections_coll.update_one(query, set_value, upsert=True)

    def insert_data_collection_log_entry(self, dcid, entry):
        entry["extended_build_num"] = self.config["extended_build_num"]
        data_collections_coll = self.db.data_collections
        query = {"dcid": dcid, "build_num": self.config["build_num"]}
        push_value = {"$push": {"processing_log": entry}}
        data_collections_coll.update_one(query, push_value)

        # Update last modified
        metadata = {"last_modified": entry["log_timestamp"]}
        set_value = {"$set": metadata}
        data_collections_coll.update_one(query, set_value, upsert=True)

    def find_orbit_by_id(self, orbit_id):
        orbits_coll = self.db.orbits
        return orbits_coll.find_one({"orbit_id": orbit_id, "build_num": self.config["build_num"]})

    def find_orbits_touching_date_range(self, field, start, stop, sort=1):
        orbits_coll = self.db.orbits
        query = {
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        return list(orbits_coll.find(query).sort(field, sort))

    def find_orbits_encompassing_date_range(self, start, stop, sort=1):
        orbits_coll = self.db.orbits
        query = {
            "start_time": {"$lt": start},
            "stop_time": {"$gt": stop},
            "build_num": self.config["build_num"]
        }
        return list(orbits_coll.find(query).sort("start_time", sort))

    def find_orbits_for_bad_reformatting(self, start, stop):
        orbits_coll = self.db.orbits
        # Query for orbits with complete set of bad data, last modified within start/stop range and
        # that don't have an associated bad netcdf file
        query = {
            "bad_status": "complete",
            "last_modified": {"$gte": start, "$lte": stop},
            "associated_bad_netcdf": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        return list(orbits_coll.find(query))

    def insert_orbit(self, metadata):
        if self.find_orbit_by_id(metadata["orbit_id"]) is None:
            utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
            metadata["created"] = utc_now
            metadata["last_modified"] = utc_now
            orbits_coll = self.db.orbits
            orbits_coll.insert_one(metadata)

    def update_orbit_metadata(self, orbit_id, metadata):
        orbits_coll = self.db.orbits
        query = {"orbit_id": orbit_id, "build_num": self.config["build_num"]}
        metadata["last_modified"] = datetime.datetime.now(tz=datetime.timezone.utc)
        set_value = {"$set": metadata}
        orbits_coll.update_one(query, set_value, upsert=True)

    def insert_orbit_log_entry(self, orbit_id, entry):
        entry["extended_build_num"] = self.config["extended_build_num"]
        orbits_coll = self.db.orbits
        query = {"orbit_id": orbit_id, "build_num": self.config["build_num"]}
        push_value = {"$push": {"processing_log": entry}}
        orbits_coll.update_one(query, push_value)

        # Update last modified
        metadata = {"last_modified": entry["log_timestamp"]}
        set_value = {"$set": metadata}
        orbits_coll.update_one(query, set_value, upsert=True)

    def find_granule_report_by_id(self, submission_id):
        granule_reports_coll = self.db.granule_reports
        return granule_reports_coll.find_one({"submission_id": submission_id})

    def insert_granule_report(self, report):
        granule_reports_coll = self.db.granule_reports
        granule_reports_coll.insert_one(report)

    def update_granule_report_submission_statuses(self, submission_id, status):
        granule_reports_coll = self.db.granule_reports
        query = {"submission_id": submission_id}
        set_value = {
            "$set": {
                "submission_status": status
            }
        }
        granule_reports_coll.update_many(query, set_value, upsert=True)
