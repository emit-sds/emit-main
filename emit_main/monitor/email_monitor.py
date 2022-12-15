"""
This code contains the EmailMonitor class that checks the emit-sds@jpl.nasa.gov email account and parses responses
from the DAAC AWS notification service

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import logging
import os

from exchangelib import Account, Configuration, Credentials, FaultTolerance, DELEGATE

from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class EmailMonitor:

    def __init__(self, config_path, level="INFO", partition="emit"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition
        # Get workflow manager
        self.wm = WorkflowManager(config_path=config_path)

        # Get Exchange account
        self.user_address = self.wm.config["email_user"]
        self.username = f"JPL\\{self.user_address.split('@')[0]}"
        self.password = self.wm.config["email_password"]
        credentials = Credentials(username=self.username, password=self.password)
        exchange_config = Configuration(
            server=self.wm.config["exchange_server"],
            retry_policy=FaultTolerance(max_wait=600), credentials=credentials
        )
        self.acct = Account(
            primary_smtp_address=self.user_address, config=exchange_config, autodiscover=False, access_type=DELEGATE
        )
        self.acct.msg_folder_root.refresh()

        # Get delivery response folders
        env = self.wm.config["environment"]
        if env in ("dev", "test", "ops"):
            self.delivery_success_folder = \
                self.acct.msg_folder_root // "EMIT Delivery Responses" // env.capitalize() // "Success"
            self.delivery_failure_folder = \
                self.acct.msg_folder_root // "EMIT Delivery Responses" // env.capitalize() // "Failure"
        else:
            self.delivery_success_folder = \
                self.acct.msg_folder_root // "EMIT Delivery Responses" // "Dev" // "Success"
            self.delivery_failure_folder = \
                self.acct.msg_folder_root // "EMIT Delivery Responses" // "Dev" // "Failure"
        pass

    def process_daac_delivery_responses(self):
        items = self.acct.inbox.all()
        logger.info(f"Attempting to process {self.acct.inbox.total_count} messages in inbox for DAAC delivery responses.")
        dm = self.wm.database_manager
        for item in items:
            # Get time received
            time_received = item.datetime_received.astimezone(self.acct.default_timezone)

            # Check that the subject includes "AWS Notification Message"
            if "AWS Notification Message" not in item.subject:
                continue

            # Check that body starts with "{" to indicate a JSON response
            if not item.body.startswith("{"):
                logger.info(f"Unable to find JSON at start of message with subject \"{item.subject}\" dated "
                            f"{time_received}. Leaving in inbox.")
                continue

            # Now get JSON response
            response = json.loads(item.body.split("\n")[0].rstrip("\r"))
            # Get identifier
            try:
                identifier = response["identifier"]
            except KeyError:
                logger.info(f"Unable to find identifier in email message with subject \"{item.subject}\" dated "
                            f"{time_received}. Leaving in inbox.")
                continue
            # Get response status
            try:
                response_status = response["response"]["status"].upper()
            except KeyError:
                logger.info(f"Unable to find response status in email message with subject \"{item.subject}\" dated "
                            f"{time_received}. Leaving in inbox.")
                continue

            # Lookup granule report by identifier. If not found, then assume it was submitted by a different environment
            if dm.find_granule_report_by_id(identifier) is None:
                logger.info(f"Cannot find granule report for submission {response['identifier']} dated "
                            f"{time_received}. Leaving message in inbox.")
                continue

            # Otherwise, look at response status.  Update DB and move message out of inbox to success or failure folders
            if response_status == "SUCCESS":
                dm.update_granule_report_submission_statuses(identifier, response_status)
                item.move(self.delivery_success_folder)
            if response_status == "FAILURE":
                if "errorCode" in response["response"]:
                    error_code = response["response"]["errorCode"]
                else:
                    error_code = ""
                if "errorMessage" in response["response"]:
                    error_message = response["response"]["errorMessage"]
                else:
                    error_message = ""
                dm.update_granule_report_submission_statuses(identifier,
                                                             ",".join([response_status, error_code, error_message]))
                item.move(self.delivery_failure_folder)

    def process_daac_reconciliation_responses(self):
        # TODO: This is just a placeholder for now - needs correct implementation
        items = self.acct.inbox.all()
        logger.info(f"Attempting to process {self.acct.inbox.total_count} messages in inbox for DAAC reconciliation "
                    f"responses.")
        dm = self.wm.database_manager
        env = self.wm.config["environment"]
        for item in items:
            # Get time received
            time_received = item.datetime_received.astimezone(self.acct.default_timezone)

            # Check that the subject includes "Rec-Report for lp-prod-reconciliation" or
            # "Rec-Report for lp-uat-reconciliation"
            if env == "ops" and "Rec-Report for lp-prod-reconciliation" not in item.subject:
                continue

            if env == "test" and "Rec-Report for lp-uat-reconciliation" not in item.subject:
                continue

            # TODO: Pick up here
            if not item.body.startswith("No discrepencies found"):
                logger.info(f"Unable to find JSON at start of message with subject \"{item.subject}\" dated "
                            f"{time_received}. Leaving in inbox.")
                continue

            # Now get JSON response
            response = json.loads(item.body.split("\n")[0].rstrip("\r"))
            # Get identifier
            try:
                identifier = response["identifier"]
            except KeyError:
                logger.info(f"Unable to find identifier in email message with subject \"{item.subject}\" dated "
                            f"{time_received}. Leaving in inbox.")
                continue
            # Get response status
            try:
                response_status = response["response"]["status"].upper()
            except KeyError:
                logger.info(f"Unable to find response status in email message with subject \"{item.subject}\" dated "
                            f"{time_received}. Leaving in inbox.")
                continue

            # Lookup granule report by identifier. If not found, then assume it was submitted by a different environment
            if dm.find_granule_report_by_id(identifier) is None:
                logger.info(f"Cannot find granule report for submission {response['identifier']} dated "
                            f"{time_received}. Leaving message in inbox.")
                continue

            # Otherwise, look at response status.  Update DB and move message out of inbox to success or failure folders
            if response_status == "SUCCESS":
                dm.update_granule_report_submission_statuses(identifier, response_status)
                item.move(self.delivery_success_folder)
            if response_status == "FAILURE":
                if "errorCode" in response["response"]:
                    error_code = response["response"]["errorCode"]
                else:
                    error_code = ""
                if "errorMessage" in response["response"]:
                    error_message = response["response"]["errorMessage"]
                else:
                    error_message = ""
                dm.update_granule_report_submission_statuses(identifier,
                                                             ",".join([response_status, error_code, error_message]))
                item.move(self.delivery_failure_folder)
