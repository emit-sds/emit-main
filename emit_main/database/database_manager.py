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

    def _remove_results_with_failed_tasks(self, results, tasks):
        non_failed_results = []
        for r in results:
            failed = False
            if "processing_log" in r:
                for log in reversed(r["processing_log"]):
                    if log["task"] in tasks and log["completion_status"] == "FAILURE":
                        failed = True
            if not failed:
                non_failed_results.append(r)
        return non_failed_results

    def find_acquisition_by_id(self, acquisition_id):
        acquisitions_coll = self.db.acquisitions
        return acquisitions_coll.find_one({"acquisition_id": acquisition_id, "build_num": self.config["build_num"]})

    def find_acquisitions_by_orbit_id(self, orbit_id, submode, min_valid_lines=0):
        acquisitions_coll = self.db.acquisitions
        query = {
            "orbit": orbit_id,
            "submode": submode,
            "num_valid_lines": {"$gte": min_valid_lines},
            "build_num": self.config["build_num"]
        }
        return list(acquisitions_coll.find(query).sort("acquisition_id", 1))

    def find_acquisitions_touching_date_range(self, submode, field, start, stop, instrument_mode="cold_img",
                                              min_valid_lines=0, sort=1):
        acquisitions_coll = self.db.acquisitions
        query = {
            "submode": submode,
            "instrument_mode": instrument_mode,
            "num_valid_lines": {"$gte": min_valid_lines},
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        return list(acquisitions_coll.find(query).sort(field, sort))

    def find_nearby_acquisitions_with_ffupdate(self, start_time, use_future_flat, limit=350):
        if use_future_flat:
            q_start = start_time
            q_stop = start_time + datetime.timedelta(days=30)
        else:
            q_start = start_time - datetime.timedelta(days=30)
            q_stop = start_time
        acquisitions_coll = self.db.acquisitions
        query = {
            "start_time": {"$gt": q_start, "$lt": q_stop},
            "products.l1b.ffupdate": {"$exists": 1},
            "mean_solar_zenith": {"$lt": 60},
            "build_num": self.config["build_num"]
        }
        projection = {
            "acquisition_id": 1,
            "start_time": 1,
            "products.l1b.ffupdate": 1,
            "build_num": 1
        }
        if use_future_flat:
            return list(acquisitions_coll.find(query, projection).sort("start_time", 1).limit(limit))
        else:
            return list(acquisitions_coll.find(query, projection).sort("start_time", -1).limit(limit))

    def find_acquisitions_for_calibration(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for "science" acquisitions with non-zero valid lines and with complete l1a raw outputs but no l1b rdn
        # outputs in time range
        query = {
            "submode": "science",
            "num_valid_lines": {"$gte": 2},
            "products.l1a.raw.img_path": {"$exists": 1},
            "products.l1b.rdn.img_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # Get acquistions for calibration - sort by start time to nominally forward process (
        # not technically necessary, but helps destriping stay somewhat ordered)
        results = list(acquisitions_coll.find(query).sort("start_time", 1))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1BCalibrate"])
        acqs_ready_for_cal = []
        for acq in results:
            recent_darks = self.find_acquisitions_touching_date_range(
                "dark",
                "stop_time",
                acq["start_time"] - datetime.timedelta(minutes=800),
                acq["start_time"],
                instrument_mode=acq["instrument_mode"],
                min_valid_lines=512,
                sort=-1)
            future_darks = self.find_acquisitions_touching_date_range(
                "dark",
                "start_time",
                acq["stop_time"],
                acq["stop_time"] + datetime.timedelta(minutes=800),
                instrument_mode=acq["instrument_mode"],
                min_valid_lines=512,
                sort=1)
            if (recent_darks is not None and len(recent_darks) > 0) or \
                    (future_darks is not None and len(future_darks) > 0):
                acqs_ready_for_cal.append(acq)
        return acqs_ready_for_cal

    def find_acquisitions_for_l2(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with complete l1b outputs but no rfl outputs in time range
        query = {
            "products.l1b.rdn.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "products.l1b.obs.img_path": {"$exists": 1},
            "products.l2a.rfl.img_path": {"$exists": 0},
            "products.l2a.mask.img_path": {"$exists": 0},
            "mean_solar_zenith": {"$lt": 80},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(acquisitions_coll.find(query))
        # Also query for case where rfl exists but not mask
        query = {
            "products.l1b.rdn.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "products.l1b.obs.img_path": {"$exists": 1},
            "products.l2a.rfl.img_path": {"$exists": 1},
            "products.l2a.mask.img_path": {"$exists": 0},
            "mean_solar_zenith": {"$lt": 80},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results += list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L2AReflectance", "emit.L2AMask"])
        return results

    def find_acquisitions_for_l2b(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with complete l2a outputs but no l2b abun outputs in time range
        query = {
            "products.l2a.rfl.img_path": {"$exists": 1},
            "products.l2a.mask.img_path": {"$exists": 1},
            "products.l2b.abun.img_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L2BAbundance"])
        return results

    def find_acquisitions_for_ch4(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with complete l2a outputs but no l2b ch4 outputs in time range
        query = {
            "products.l1b.rdn.img_path": {"$exists": 1},
            "products.l1b.bandmask.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "products.l1b.obs.img_path": {"$exists": 1},
            "products.l2a.rfl.img_path": {"$exists": 1},
            "products.l2a.mask.img_path": {"$exists": 1},
            "mean_solar_zenith": {"$lt": 80},
            "products.ghg.ch4.ortch4.tif_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # TODO: Remove this block when GHG reprocessing is complete
        if date_field == "last_modified":
            ghg_forward_start = datetime.datetime.strptime("2025-04-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
            query["start_time"] = {"$gte": ghg_forward_start}
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.CH4"])
        return results

    def find_acquisitions_for_co2(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with complete l2a outputs but no l2b co2 outputs in time range
        query = {
            "products.l1b.rdn.img_path": {"$exists": 1},
            "products.l1b.bandmask.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "products.l1b.obs.img_path": {"$exists": 1},
            "products.l2a.rfl.img_path": {"$exists": 1},
            "products.l2a.mask.img_path": {"$exists": 1},
            "mean_solar_zenith": {"$lt": 80},
            "products.ghg.co2.ortco2.tif_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # TODO: Remove this block when GHG reprocessing is complete
        if date_field == "last_modified":
            ghg_forward_start = datetime.datetime.strptime("2025-04-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
            query["start_time"] = {"$gte": ghg_forward_start}
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.CO2"])
        return results

    def find_acquisitions_for_l3(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with complete l2a outputs but no l3 cover outputs in time range
        query = {
            "products.l2a.rfl.img_path": {"$exists": 1},
            "products.l2a.mask.img_path": {"$exists": 1},
            "products.l3.cover.img_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L3Unmix"])
        return results

    def find_acquisitions_for_l1a_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with daac scene numbers but no daac ummg products.  If science, then we also need the
        # l1b browse image
        query = {
            "products.l1a.raw.img_path": {"$exists": 1},
            "daac_scene": {"$exists": 1},
            "products.l1a.raw_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1ADeliver"])
        return results

    def find_acquisitions_for_l1brdn_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with daac scene numbers but no daac ummg products. We also need the
        # l1b browse image
        query = {
            "products.l1b.rdn.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "products.l1b.obs.img_path": {"$exists": 1},
            "products.l1b.rdn_png.png_path": {"$exists": 1},
            "cloud_fraction": {"$exists": 1},
            "daac_scene": {"$exists": 1},
            "products.l1b.rdn_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }

        results = list(acquisitions_coll.find(query))
        # Also query for case where nighttime science RDN exists
        query = {
            "products.l1b.rdn.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "products.l1b.obs.img_path": {"$exists": 1},
            "products.l1b.rdn_png.png_path": {"$exists": 1},
            "cloud_fraction": {"$exists": 0},
            "submode": {"science"},
            "mean_solar_zenith": {"$gte: 80"},
            "daac_scene": {"$exists": 1},
            "products.l1b.rdn_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results += list(acquisitions_coll.find(query))

        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1BRdnFormat", "emit.L1BRdnDeliver"])
        return results

    def find_acquisitions_for_l2a_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with daac scene numbers but no daac ummg products. We also need the
        # l2a browse image
        query = {
            "products.l2a.rfl.img_path": {"$exists": 1},
            "products.l2a.rfluncert.img_path": {"$exists": 1},
            "products.l2a.mask.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "cloud_fraction": {"$exists": 1},
            "daac_scene": {"$exists": 1},
            "products.l2a.rfl_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L2AFormat", "emit.L2ADeliver"])
        return results

    def find_acquisitions_for_l2b_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with daac scene numbers but no daac ummg products.
        query = {
            "products.l2b.abun.img_path": {"$exists": 1},
            "products.l2b.abununcert.img_path": {"$exists": 1},
            "products.l1b.glt.img_path": {"$exists": 1},
            "products.l1b.loc.img_path": {"$exists": 1},
            "cloud_fraction": {"$exists": 1},
            "daac_scene": {"$exists": 1},
            "products.l2b.abun_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L2BFormat", "emit.L2BDeliver"])
        return results

    def find_acquisitions_for_ch4_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with daac scene numbers but no daac ummg products.
        query = {
            "products.ghg.ch4.ortch4.tif_path": {"$exists": 1},
            "products.ghg.ch4.ortsensch4.tif_path": {"$exists": 1},
            "products.ghg.ch4.ortuncertch4.tif_path": {"$exists": 1},
            "cloud_fraction": {"$exists": 1},
            "daac_scene": {"$exists": 1},
            "products.ghg.ch4.ch4_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # TODO: Remove this block when GHG reprocessing is complete
        if date_field == "last_modified":
            ghg_forward_start = datetime.datetime.strptime("2025-04-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
            query["start_time"] = {"$gte": ghg_forward_start}
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.CH4Deliver"])
        return results

    def find_acquisitions_for_co2_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        acquisitions_coll = self.db.acquisitions
        # Query for acquisitions with daac scene numbers but no daac ummg products.
        query = {
            "products.ghg.co2.ortco2.tif_path": {"$exists": 1},
            "products.ghg.co2.ortsensco2.tif_path": {"$exists": 1},
            "products.ghg.co2.ortuncertco2.tif_path": {"$exists": 1},
            "cloud_fraction": {"$exists": 1},
            "daac_scene": {"$exists": 1},
            "products.ghg.co2.co2_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # TODO: Remove this block when GHG reprocessing is complete
        if date_field == "last_modified":
            ghg_forward_start = datetime.datetime.strptime("2025-04-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
            query["start_time"] = {"$gte": ghg_forward_start}
        results = list(acquisitions_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.CO2Deliver"])
        return results

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

    def find_streams_for_edp_reformatting(self, start, stop, date_field="last_modified", retry_failed=False):
        streams_coll = self.db.streams
        # Query for 1674 streams that have l0 ccsds products but no l1a products which were last modified between
        # start and stop times (typically they need to be older than a certain amount of time to make sure the
        # 1676 ancillary file exists
        query = {
            "apid": "1674",
            date_field: {"$gte": start, "$lte": stop},
            "products.l0.ccsds_path": {"$exists": 1},
            "products.l1a": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        results = list(streams_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1AReformatEDP"])
        return results

    def find_streams_for_l0_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        streams_coll = self.db.streams
        # Query for 1675 streams that have l0 ccsds products but no umm-g products
        query = {
            "apid": "1675",
            date_field: {"$gte": start, "$lte": stop},
            "products.l0.ccsds_path": {"$exists": 1},
            "products.daac.ccsds_ummg": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        results = list(streams_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L0Deliver"])
        return results

    def insert_stream(self, name, metadata):
        if self.find_stream_by_name(name) is None:
            utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
            metadata["created"] = utc_now
            metadata["last_modified"] = utc_now
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

    def find_data_collections_touching_date_range(self, field, start, stop, sort=1):
        data_collections_coll = self.db.data_collections
        query = {
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        return list(data_collections_coll.find(query).sort(field, sort))

    def delete_data_collections_touching_date_range(self, field, start, stop, sort=1):
        data_collections_coll = self.db.data_collections
        query = {
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # First, find the data_collections and then return them below
        data_collections = list(data_collections_coll.find(query).sort(field, sort))
        # Next, delete them from db
        data_collections_coll.delete_many(query)
        return data_collections

    def find_data_collections_by_orbit_id(self, orbit_id, submode="science"):
        data_collections_coll = self.db.data_collections
        return list(data_collections_coll.find({
            "orbit": orbit_id,
            "submode": submode,
            "build_num": self.config["build_num"]}))

    def find_data_collections_for_reassembly(self, start, stop, date_field="frames_last_modified", retry_failed=False):
        data_collections_coll = self.db.data_collections
        # Use frames_last_modified for date field by default
        if date_field == "last_modified":
            date_field = "frames_last_modified"
        # Query for data collections with complete set of frames, last modified within start/stop range and
        # that don't have associated acquisitions
        query = {
            "frames_status": "complete",
            date_field: {"$gte": start, "$lte": stop},
            "associated_acquisitions": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        results = list(data_collections_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1AReassembleRaw", "emit.L1AFrameReport"])
        return results

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

    def delete_orbits_touching_date_range(self, field, start, stop, sort=1):
        orbits_coll = self.db.orbits
        query = {
            field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        # First, find the orbits and return them below
        orbits = list(orbits_coll.find(query).sort(field, sort))
        # Next, delete them from db
        orbits_coll.delete_many(query)
        return orbits

    def find_orbits_encompassing_date_range(self, start, stop, sort=1):
        orbits_coll = self.db.orbits
        query = {
            "start_time": {"$lt": start},
            "stop_time": {"$gt": stop},
            "build_num": self.config["build_num"]
        }
        return list(orbits_coll.find(query).sort("start_time", sort))

    def find_orbits_for_bad_reformatting(self, start, stop, date_field="last_modified", retry_failed=False):
        orbits_coll = self.db.orbits
        # Query for orbits with complete set of bad data, last modified within start/stop range and
        # that don't have an associated bad netcdf file
        query = {
            "bad_status": "complete",
            date_field: {"$gte": start, "$lte": stop},
            "associated_bad_netcdf": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        results = list(orbits_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1AReformatBAD"])
        return results

    def find_orbits_for_geolocation(self, start, stop, date_field="last_modified", retry_failed=False):
        orbits_coll = self.db.orbits
        # Query for orbits with complete set of radiance files, an associated BAD netcdf file, last modified within
        # start/stop range, and no products.l1b.acquisitions
        query = {
            "radiance_status": "complete",
            date_field: {"$gte": start, "$lte": stop},
            "associated_bad_netcdf": {"$exists": 1},
            "products.l1b.acquisitions": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        results = list(orbits_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1BGeolocate"])
        return results

    def find_orbits_for_daac_scene_numbers(self, start, stop, date_field="last_modified", retry_failed=False):
        orbits_coll = self.db.orbits
        # Query for orbits with complete set of raw files.
        query = {
            "raw_status": "complete",
            date_field: {"$gte": start, "$lte": stop},
            "num_scenes": {"$exists": 0},
            "build_num": self.config["build_num"]
        }
        results = list(orbits_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.AssignDAACSceneNumbers"])
        return results

    def find_orbits_for_l1batt_delivery(self, start, stop, date_field="last_modified", retry_failed=False):
        orbits_coll = self.db.orbits
        # Query for orbits with complete set of raw files.
        query = {
            "products.l1b.corr_att_eph.nc_path": {"$exists": 1},
            "products.l1b.att_ummg.ummg_json_path": {"$exists": 0},
            date_field: {"$gte": start, "$lte": stop},
            "build_num": self.config["build_num"]
        }
        results = list(orbits_coll.find(query))
        if not retry_failed:
            results = self._remove_results_with_failed_tasks(results, ["emit.L1BAttDeliver"])
        return results

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

    def find_files_for_reconciliation_report(self, start, stop):
        granule_reports_coll = self.db.granule_reports
        # Query for granules that haven't been reconciled or that have failed
        # Also query for ones that have submitted a reconciliation report. This overwrites the last try.
        query = {
            "timestamp": {"$gte": start, "$lte": stop},
            "last_reconciliation_status": {"$exists": 0}
        }
        results = list(granule_reports_coll.find(query))
        query = {
            "timestamp": {"$gte": start, "$lte": stop},
            "last_reconciliation_status": {"$regex": "FAILURE.*"}
        }
        results += list(granule_reports_coll.find(query))
        query = {
            "timestamp": {"$gte": start, "$lte": stop},
            "last_reconciliation_status": "submitted"
        }
        results += list(granule_reports_coll.find(query))
        return results

    def find_files_by_last_reconciliation_report(self, report):
        granule_reports_coll = self.db.granule_reports
        # Query for all files in a report
        query = {
            "last_reconciliation_report": report
        }
        results = list(granule_reports_coll.find(query))
        return results

    def update_reconciliation_submission_status(self, daac_filename, submission_id, report, status):
        granule_reports_coll = self.db.granule_reports
        query = {
            "daac_filename": daac_filename,
            "submission_id": submission_id
        }
        set_value = {
            "$set": {
                "last_reconciliation_report": report,
                "last_reconciliation_status": status
            }
        }
        granule_reports_coll.update_one(query, set_value, upsert=True)
