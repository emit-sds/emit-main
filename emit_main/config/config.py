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
            self.dictionary.update(config["daac_config"])

            # Use first four digits of extended_build_num to define build_num which is used in file naming and in DB
            self.dictionary["build_num"] = self.dictionary["extended_build_num"][:4]

            # Use extended_build_num to look up build configuration
            config_dir = os.path.dirname(config_path)
            build_config_path = os.path.join(config_dir, "build",
                                             "build_" + self.dictionary["extended_build_num"] + ".json")
            with open(build_config_path, "r") as b:
                build_config = json.load(b)
                self.dictionary.update(build_config)

            # Read in product specific paths
            self.dictionary.update(self._get_product_config_paths(config["product_config"], timestamp))

            # Get passwords from resources/credentials directory
            self.dictionary.update(self._get_passwords())

    def _get_product_config_paths(self, product_config, timestamp):
        # Get the products paths that are either absolute paths or relative to the environment directory
        # (eg. /store/emit/ops).
        if "date_ranges" in product_config:
            if timestamp is not None:
                date_ranges = product_config["date_ranges"]
                # Look for matching date range and update top level dictionary with those key/value pairs
                for date_range, values in date_ranges.items:
                    # These dates are all in UTC by default and do not require any timezone conversion
                    start_date = datetime.datetime.strptime(date_range.split('_to_')[0], "%Y-%m-%dT%H:%M:%S")
                    end_date = datetime.datetime.strptime(date_range.split('_to_')[1], "%Y-%m-%dT%H:%M:%S")
                    if start_date <= timestamp < end_date:
                        product_config.update(values)

            # Remove "date_ranges" and return dictionary
            del product_config["date_ranges"]

        # Convert file paths to absolute paths
        environment_dir = os.path.join(self.dictionary["local_store_dir"], self.dictionary["instrument"],
                                       self.dictionary["environment"])
        for key, path in product_config.items():
            if type(path) is str and not path.startswith("/"):
                product_config[key] = os.path.join(environment_dir, path)
        return product_config

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
