"""
This code contains the Config class that reads in the config file and returns its properties

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime
import json
import logging
import os

from cryptography.fernet import Fernet

logger = logging.getLogger("emit-main")


class Config:

    def __init__(self, config_path, timestamp=None):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = config_path
        self.dictionary = {}

        # Read config file for environment specific paths
        with open(config_path, "r") as f:
            config = json.load(f)
            # Read in groups of properties
            self.dictionary.update(config["general_config"])
            self.dictionary.update(config["filesystem_config"])
            self.dictionary.update(config["database_config"])
            self.dictionary.update(config["email_config"])
            self.dictionary.update(config["build_config"])

            # Use build_num to read in build config
            config_dir = os.path.dirname(config_path)
            build_config_path = os.path.join(config_dir, "build", "build_" + self.dictionary["build_num"] + ".json")
            with open(build_config_path, "r") as b:
                build_config = json.load(b)
                self.dictionary.update(build_config)

            # Read in ancillary paths
            self.dictionary.update(self._get_ancillary_file_paths(config["ancillary_paths"], timestamp))

            # Get passwords from resources/credentials directory
            self.dictionary.update(self._get_passwords())

    def _get_ancillary_file_paths(self, anc_files_config, timestamp):
        # Get the ancillary paths that are either absolute paths or relative to the environment directory
        # (eg. /store/emit/ops).
        if "versions" in anc_files_config:
            if timestamp is not None:
                versions = anc_files_config["versions"]
                # Look for matching date range and update top level dictionary with those key/value pairs
                for version in versions:
                    # These dates are all in UTC by default and do not require any timezone conversion
                    start_date = datetime.datetime.strptime(version["version_date_range"][0], "%Y-%m-%dT%H:%M:%S")
                    end_date = datetime.datetime.strptime(version["version_date_range"][1], "%Y-%m-%dT%H:%M:%S")
                    if start_date <= timestamp < end_date:
                        anc_files_config.update(version)

            # Remove "versions" and return dictionary
            del anc_files_config["versions"]

        # Convert file paths to absolute paths
        environment_dir = os.path.join(self.dictionary["local_store_dir"], self.dictionary["instrument"],
                                       self.dictionary["environment"])
        for key, path in anc_files_config.items():
            if type(path) is str and not path.startswith("/"):
                anc_files_config[key] = os.path.join(environment_dir, path)
        return anc_files_config

    def _get_date_from_acquisition(self, acquisition_id):
        instrument_prefix = self.dictionary["instrument"]
        if acquisition_id.startswith("ang"):
            instrument_prefix = "ang"
        # Get date from acquisition string
        date_str = acquisition_id[len(instrument_prefix):(15 + len(instrument_prefix))]
        return datetime.datetime.strptime(date_str, "%Y%m%dt%H%M%S")

    def _get_passwords(self):
        # Get encrypted passwords
        passwords_path = os.path.join(self.dictionary["local_store_dir"], self.dictionary["instrument"],
                                      self.dictionary["environment"], "resources", "credentials",
                                      "encrypted_passwords.json")
        with open(passwords_path, "r") as f:
            enc_passwords = json.load(f)

        # Decrypt passwords
        key_path = os.path.join(os.path.dirname(passwords_path), "key.txt")
        with open(key_path) as f:
            key = f.read()
            key_bytes = bytes(key, 'utf-8')

        passwords = {}
        crypto_key = Fernet(key_bytes)
        for k, v in enc_passwords.items():
            pass_bytes = bytes(v, "utf-8")
            passwords[k] = (crypto_key.decrypt(pass_bytes)).decode("utf-8")

        return passwords

    def get_dictionary(self):
        return self.dictionary
