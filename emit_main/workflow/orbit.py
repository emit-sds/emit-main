"""
This code contains the Orbit class that manages orbits and their metadata

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import logging
import os

from emit_main.database.database_manager import DatabaseManager
from emit_main.config.config import Config

logger = logging.getLogger("emit-main")


class Orbit:

    def __init__(self, config_path, orbit_id):
        """
        :param config_path: The path to the config file
        :param orbit_id: The padded string representation of the orbit number
        """

        self.config_path = config_path
        self.orbit_id = orbit_id
        self.short_oid = self.orbit_id[2:] if len(self.orbit_id) == 7 else self.orbit_id

        # Read metadata from db
        dm = DatabaseManager(config_path)
        self.metadata = dm.find_orbit_by_id(self.orbit_id)
        self._initialize_metadata()
        self.__dict__.update(self.metadata)

        # Get config properties
        self.config = Config(config_path, self.start_time).get_dictionary()

        # Create base directories and add to list to create directories later
        self.dirs = []
        self.instrument_dir = os.path.join(self.config["local_store_dir"], self.config["instrument"])
        self.environment_dir = os.path.join(self.instrument_dir, self.config["environment"])
        self.data_dir = os.path.join(self.environment_dir, "data")
        self.orbits_dir = os.path.join(self.data_dir, "orbits")
        self.date_str = self.start_time.strftime("%Y%m%d")
        self.date_dir = os.path.join(self.orbits_dir, self.date_str)
        self.orbit_id_dir = os.path.join(self.date_dir, self.orbit_id)
        self.raw_dir = os.path.join(self.orbit_id_dir, "raw")
        self.l1a_dir = os.path.join(self.orbit_id_dir, "l1a")
        self.l1b_dir = os.path.join(self.orbit_id_dir, "l1b")
        self.l1b_geo_work_dir = os.path.join(
            self.l1b_dir, f"o{orbit_id}_l1b_geo_b{self.config['build_num']}_v{self.config['product_versions']['l1b']}_work")
        self.dirs.extend([self.orbits_dir, self.date_dir, self.orbit_id_dir, self.raw_dir, self.l1a_dir, self.l1b_dir])

        # Create product names
        uncorr_fname = "_".join([f"emit{self.start_time.strftime('%Y%m%dt%H%M%S')}", f"o{self.short_oid}",
                                 "l1a", "att", f"b{self.config['build_num']}",
                                 f"v{self.config['product_versions']['l1a']}.nc"])
        self.uncorr_att_eph_path = os.path.join(self.l1a_dir, uncorr_fname)
        corr_fname = "_".join([f"emit{self.start_time.strftime('%Y%m%dt%H%M%S')}", f"o{self.short_oid}",
                                 "l1b", "att", f"b{self.config['build_num']}",
                                 f"v{self.config['product_versions']['l1b']}.nc"])
        self.corr_att_eph_path = os.path.join(self.l1b_dir, corr_fname)

        # Make directories and symlinks if they don't exist
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=config_path)
        for d in self.dirs:
            wm.makedirs(d)

        # Build paths for DAAC delivery on staging server
        self.daac_staging_dir = os.path.join(self.config["daac_base_dir"], wm.config['environment'], "products",
                                             self.start_time.strftime("%Y%m%d"))
        self.daac_uri_base = f"https://{self.config['daac_server_external']}/emit/lpdaac/{wm.config['environment']}/" \
            f"products/{self.start_time.strftime('%Y%m%d')}/"
        self.daac_partial_dir = os.path.join(self.config["daac_base_dir"], wm.config['environment'], "partial_transfers")
        self.aws_staging_dir = os.path.join(self.config["aws_s3_base_dir"], wm.config['environment'], "products",
                                            self.start_time.strftime("%Y%m%d"))
        self.aws_s3_uri_base = f"s3://{self.config['aws_s3_bucket']}{self.aws_staging_dir}/"

    def _initialize_metadata(self):
        # Insert some placeholder fields so that we don't get missing keys on updates
        if "processing_log" not in self.metadata:
            self.metadata["processing_log"] = []
        if "products" not in self.metadata:
            self.metadata["products"] = {}
        if "raw" not in self.metadata["products"]:
            self.metadata["products"]["raw"] = {}
        if "l1a" not in self.metadata["products"]:
            self.metadata["products"]["l1a"] = {}
        if "l1b" not in self.metadata["products"]:
            self.metadata["products"]["l1b"] = {}

    def has_complete_bad_data(self):
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=self.config_path)

        if "associated_bad_sto" not in self.metadata:
            wm.print(__name__, f"No 'associated_bad_sto' property in orbit {self.orbit_id}")
            return False

        if "stop_time" not in self.metadata:
            wm.print(__name__, f"Orbit {self.orbit_id} does not have a stop time.")
            return False

        bad_sto_files = [os.path.basename(p) for p in self.metadata["associated_bad_sto"]]
        bad_sto_files.sort(key=lambda x: x.split("_")[-2])

        # Check if empty
        if len(bad_sto_files) == 0:
            wm.print(__name__, f"No associated BAD STO files for orbit {self.orbit_id}")
            return False

        # Check that associated BAD sto files encompass orbit date range
        bad_start_time = datetime.datetime.strptime(bad_sto_files[0].split("_")[-2], "%Y%m%dT%H%M%S")
        bad_stop_time = datetime.datetime.strptime(bad_sto_files[-1].split("_")[-1].replace(".sto", ""), "%Y%m%dT%H%M%S")
        if bad_start_time > self.start_time - datetime.timedelta(seconds=10) \
                or bad_stop_time < self.stop_time + datetime.timedelta(seconds=10):
            wm.print(__name__, f"Start and stop time for associated BAD STO files of orbit {self.orbit_id} do not "
                     f"encompass the orbit's entire time range.")
            return False

        # Check that there are no gaps
        prev_file = None
        for file in bad_sto_files:
            if prev_file is None:
                prev_file = file
                continue
            prev_stop_time = datetime.datetime.strptime(prev_file.split("_")[-1].replace(".sto", ""), "%Y%m%dT%H%M%S")
            cur_start_time = datetime.datetime.strptime(file.split("_")[-2], "%Y%m%dT%H%M%S")
            gap = cur_start_time - prev_stop_time
            # If the gap is bigger than 10 seconds return False
            if gap.total_seconds() > 10:
                wm.print(__name__, f"Found a gap of {gap.total_seconds()} while comparing associated BAD STO files for "
                         f"orbit {self.orbit_id}")
                return False
            prev_file = file

        # If we made it this far, then we have a complete set
        return True

    def has_complete_radiance(self, build_nums=None):
        # Check to see if all the radiance files for this orbit have been generated
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=self.config_path)

        # First find all the DCIDs in an orbit
        dm = DatabaseManager(self.config_path)
        data_collections = dm.find_data_collections_by_orbit_id(self.orbit_id, build_nums=build_nums)

        if len(data_collections) == 0:
            wm.print(__name__, f"Did not find any data collections associated with orbit {self.orbit_id}")
            return False

        # Then find all the associated acquisitions
        acquisition_ids = []
        for dc in data_collections:
            if "associated_acquisitions" in dc and len(dc["associated_acquisitions"]) > 0:
                for id in dc["associated_acquisitions"]:
                    acquisition_ids.append(id)
            else:
                wm.print(__name__, f"Found data collections associated with orbit {self.orbit_id}, but at least one of "
                                   f"them doesn't have any associated acquisitions yet.")
                return False

        if len(acquisition_ids) == 0:
            wm.print(__name__, f"Did not find any acquisitions associated with orbit {self.orbit_id}")
            return False

        acquisition_ids = list(set(acquisition_ids))
        acquisition_ids.sort()

        # Look up acquisitions to see if it is science data and rdn product has been generated. Return False if not.
        # Keep track of number of science acquisitions
        # NOTE: We have to lookup both previous build_num values and the current one since some acquisitions will not
        # get calibrated due to clouds, but we still need to check them for completion
        if build_nums is not None and wm.config["build_num"] not in build_nums:
            build_nums.append(wm.config["build_num"])
        num_science = 0
        for id in acquisition_ids:
            acq = dm.find_acquisition_by_id(id, build_nums=build_nums)
            if acq is not None and acq["submode"] == "science" and acq["num_valid_lines"] >= 2:
                # If the returned science acquisition is not from the current build, then fail because not complete
                if acq["build_num"] != wm.config["build_num"]:
                    wm.print(__name__, f"Acquisition {id} in orbit {self.orbit_id} does not have a radiance product "
                                       f"yet for build {wm.config['build_num']}.")
                    return False
                num_science += 1
                try:
                    rdn_img_path = acq["products"]["l1b"]["rdn"]["img_path"]
                except KeyError:
                    wm.print(__name__, f"Acquisition {id} in orbit {self.orbit_id} does not have a radiance product "
                             f"yet for build {wm.config['build_num']}.")
                    return False
                if not os.path.exists(rdn_img_path):
                    wm.print(__name__, f"Acquisition {id} in orbit {self.orbit_id} has rdn_img_path of {rdn_img_path} "
                             f"but file does not exist.")
                    return False
            elif acq is None:
                # If we didn't find the acquisition, then it doesn't exist
                wm.print(__name__, f"Acquisition {id} in orbit {self.orbit_id} was not found for build numbers "
                                   f"{build_nums}")

        if num_science == 0:
            wm.print(__name__, f"Did not find any science acquisitions while checking acquisitions in orbit "
                     f"{self.orbit_id}")
            return False

        # If we made it this far, then return True
        return True

    def has_complete_raw(self):
        # Check to see if all the raw files for this orbit have been generated
        from emit_main.workflow.workflow_manager import WorkflowManager
        wm = WorkflowManager(config_path=self.config_path)

        # First find all the DCIDs in an orbit
        dm = DatabaseManager(self.config_path)
        data_collections = dm.find_data_collections_by_orbit_id(self.orbit_id, submode="science")
        data_collections += dm.find_data_collections_by_orbit_id(self.orbit_id, submode="dark")

        if len(data_collections) == 0:
            wm.print(__name__, f"Did not find any data collections associated with orbit {self.orbit_id}")
            return False

        # Then find all the associated acquisitions
        acquisition_ids = []
        for dc in data_collections:
            if "associated_acquisitions" in dc and len(dc["associated_acquisitions"]) > 0:
                for id in dc["associated_acquisitions"]:
                    acquisition_ids.append(id)
            else:
                wm.print(__name__, f"Found data collections associated with orbit {self.orbit_id}, but at least one of "
                                   f"them doesn't have any associated acquisitions yet.")
                return False

        if len(acquisition_ids) == 0:
            wm.print(__name__, f"Did not find any acquisitions associated with orbit {self.orbit_id}")
            return False

        acquisition_ids = list(set(acquisition_ids))
        acquisition_ids.sort()

        # Look up acquisitions to see if the raw product has been generated. Return False if not.
        for id in acquisition_ids:
            acq = dm.find_acquisition_by_id(id)
            if acq is None:
                wm.print(__name__, f"Couldn't find acquisition {id} in DB while checking for complete set of raw "
                                   f"scenes")
                return False
            try:
                raw_img_path = acq["products"]["l1a"]["raw"]["img_path"]
            except KeyError:
                wm.print(__name__, f"Acquisition {id} in orbit {self.orbit_id} does not have a raw product yet.")
                return False
            if not os.path.exists(raw_img_path):
                wm.print(__name__, f"Acquisition {id} in orbit {self.orbit_id} has raw_img_path of {raw_img_path} "
                         f"but file does not exist.")
                return False

        # If we made it this far, then return True
        return True
