"""
This code contains the EmailMonitor class that checks the emit-sds@jpl.nasa.gov email account and parses responses
from the DAAC AWS notification service

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import json
import logging
import os
import re

from exchangelib import Account, Configuration, Credentials, FaultTolerance, DELEGATE

from emit_main.workflow.l0_tasks import L0Deliver
from emit_main.workflow.l1a_tasks import L1ADeliver
from emit_main.workflow.l1b_tasks import L1BRdnDeliver, L1BAttDeliver
from emit_main.workflow.l2a_tasks import L2ADeliver
from emit_main.workflow.workflow_manager import WorkflowManager

logger = logging.getLogger("emit-main")


class EmailMonitor:

    def __init__(self, config_path, level="INFO", partition="emit", daac_ingest_queue="forward"):
        """
        :param config_path: Path to config file containing environment settings
        """

        self.config_path = os.path.abspath(config_path)
        self.level = level
        self.partition = partition
        self.daac_ingest_queue = daac_ingest_queue
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
            self.reconciliation_success_folder = \
                self.acct.msg_folder_root // "EMIT Reconciliation Responses" // env.capitalize() // "Success"
            self.reconciliation_failure_folder = \
                self.acct.msg_folder_root // "EMIT Reconciliation Responses" // env.capitalize() // "Failure"
        else:
            self.delivery_success_folder = \
                self.acct.msg_folder_root // "EMIT Delivery Responses" // "Dev" // "Success"
            self.delivery_failure_folder = \
                self.acct.msg_folder_root // "EMIT Delivery Responses" // "Dev" // "Failure"
            self.reconciliation_success_folder = \
                self.acct.msg_folder_root // "EMIT Reconciliation Responses" // "Dev" // "Success"
            self.reconciliation_failure_folder = \
                self.acct.msg_folder_root // "EMIT Reconciliation Responses" // "Dev" // "Failure"
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

    def process_daac_reconciliation_responses(self, reconciliation_response_path=None, retry_failed=False):
        dm = self.wm.database_manager
        env = self.wm.config["environment"]
        # If the response path is not specified, then check the inbox for responses
        if reconciliation_response_path is None:
            items = self.acct.inbox.all()
            logger.info(f"Attempting to process {self.acct.inbox.total_count} messages in inbox for DAAC reconciliation "
                        f"responses.")
            for item in items:
                # Get time received
                time_received = item.datetime_received.astimezone(self.acct.default_timezone)

                # Check that the subject includes "Rec-Report for lp-prod-reconciliation" or
                # "Rec-Report for lp-uat-reconciliation"
                if env == "ops" and "Rec-Report EMIT lp-prod" not in item.subject:
                    continue

                if env == "test" and "Rec-Report EMIT lp-uat" not in item.subject:
                    continue

                match = re.search("ER_.+rpt", item.subject)
                if match is None:
                    logger.warning(f"Found email with subject \"{item.subject}\" dated {time_received} but unable to "
                                   f"identify the reconciliation report name from the subject")
                    continue

                report = match.group()
                # Find all files in this report
                files = dm.find_files_by_last_reconciliation_report(report=report)
                if item.body.startswith("No discrepencies found"):
                    logger.info(f"No discrepancies found in email with subject \"{item.subject}\" dated {time_received}. "
                                f"Marking all files as reconciled.")
                    # Mark all files as reconciled
                    for f in files:
                        dm.update_reconciliation_submission_status(f["daac_filename"], f["submission_id"], report,
                                                                   "RECONCILED")
                    item.move(self.reconciliation_success_folder)
                else:
                    report_response_name = report.replace(".rpt", ".json")
                    reconciliation_response_path = os.path.join(self.wm.reconciliation_dir, report_response_name)
                    logger.info(f"Discrepancies found in email with subject \"{item.subject}\" dated {time_received}. "
                                f"Downloading report file to {reconciliation_response_path}.")
                    pge = self.wm.pges["emit-main"]
                    # cmd: aws s3 cp s3://lp-prod-reconciliation/reports/EMIT_RECON_PROD.json ./
                    if env == "ops":
                        s3_link = f"s3://lp-prod-reconciliation/reports/{report_response_name}"
                    if env == "test":
                        s3_link = f"s3://lp-prod-reconciliation/reports/{report_response_name}"
                    cmd = [self.wm.config["aws_cli_exe"], "s3", "cp", s3_link, reconciliation_response_path,
                           "--profile", self.wm.config["aws_profile"]]
                    pge.run(cmd)
                    self.wm.change_group_ownership(reconciliation_response_path)
                    item.move(self.reconciliation_failure_folder)

        # Check if we have response path now and update files accordingly
        granule_urs = set()
        if reconciliation_response_path is not None:
            # Check that file exists
            if not os.path.exists(reconciliation_response_path):
                raise RuntimeError(f"Attempting to process reconciliation responses in {reconciliation_response_path} "
                                   f"but file does not exist! Exiting...")
            report = os.path.basename(reconciliation_response_path).replace(".json", ".rpt")
            # Look up all files in report
            files = dm.find_files_by_last_reconciliation_report(report=report)
            # Create lookup for failed ingests
            failed_files = {}
            with open(reconciliation_response_path, "r") as f:
                collections = json.load(f)
                for coll in collections:
                    for k in coll:
                        if "report" in coll[k]:
                            for file in coll[k]["report"]:
                                # if coll[k]["report"][file]["status"] != "queued":
                                granule_urs.add(coll[k]["report"][file]["granuleId"])
                                # TODO: If there are multiple files in the report with the same name but different
                                # TODO: checksums, this could be an issue.  Use <file>_<checksum> here
                                failed_files[file] = {
                                    "checksum": coll[k]["report"][file]["cksum"],
                                    "status": coll[k]["report"][file]["status"]
                                }
                                if "error" in coll[k]["report"][file] and "Cause" in coll[k]["report"][file]["error"]:
                                    failed_files[file]["cause"] = coll[k]["report"][file]["error"]["Cause"]

            # Now go through the files from the report and update their statuses accordingly
            for f in files:
                if f["daac_filename"] in failed_files and f["checksum"] == failed_files[f["daac_filename"]]["checksum"]:
                    f["last_reconciliation_status"] = "FAILURE," + failed_files[f["daac_filename"]]["status"]
                    if "cause" in failed_files[f["daac_filename"]]:
                        f["last_reconciliation_status"] = f["last_reconciliation_status"] + "," + failed_files[f["daac_filename"]]["cause"]

                # TODO: What about case where the checksum doesn't match?

                if f["daac_filename"] not in failed_files:
                    # This means the file in the report had no errors
                    f["last_reconciliation_status"] = "RECONCILED"

                # Now update the DB
                dm.update_reconciliation_submission_status(f["daac_filename"], f["submission_id"], report,
                                                           f["last_reconciliation_status"])

        # If retry failed is true then set up tasks to run based on failed granules
        tasks = []
        if retry_failed is True:
            # Now create tasks based on granule urs
            for g in granule_urs:
                if g.startswith("EMIT_L0"):
                    # Need to find the stream name
                    stream_files = [f for f in files if f["granule_ur"] == g]
                    ccsds_name = None
                    if len(stream_files) > 0:
                        ccsds_name = stream_files[0]["sds_filename"]
                        if ccsds_name.endswith(".cmr.json"):
                            ccsds_name = ccsds_name.replace(".cmr.json", ".bin")
                    if ccsds_name is not None:
                        stream = dm.find_stream_by_name(ccsds_name)
                        stream_path = stream["products"]["l0"]["ccsds_path"]
                        logger.info(f"Creating L0Deliver task for path {stream_path}")
                        tasks.append(L0Deliver(config_path=self.config_path,
                                               stream_path=stream_path,
                                               level=self.level,
                                               partition=self.partition,
                                               daac_ingest_queue=self.daac_ingest_queue,
                                               override_output=True))
                if g.startswith("EMIT_L1A_RAW"):
                    # Get acquisition id
                    timestamp = g.split("_")[4].replace("T", "t")
                    acquisition_id = f"emit{timestamp}"
                    logger.info(f"Creating L1ADeliver task for acquisition {acquisition_id}")
                    tasks.append(L1ADeliver(config_path=self.config_path,
                                            acquisition_id=acquisition_id,
                                            level=self.level,
                                            partition=self.partition,
                                            daac_ingest_queue=self.daac_ingest_queue,
                                            override_output=True))

                if g.startswith("EMIT_L1B_RAD"):
                    # Get acquisition id
                    timestamp = g.split("_")[4].replace("T", "t")
                    acquisition_id = f"emit{timestamp}"
                    logger.info(f"Creating L1BRdnDeliver task for acquisition {acquisition_id}")
                    tasks.append(L1BRdnDeliver(config_path=self.config_path,
                                               acquisition_id=acquisition_id,
                                               level=self.level,
                                               partition=self.partition,
                                               daac_ingest_queue=self.daac_ingest_queue,
                                               override_output=True))

                if g.startswith("EMIT_L1B_ATT"):
                    # Get orbit
                    orbit_id = g.split("_")[-1]
                    logger.info(f"Creating L1BAttDeliver task for orbit {orbit_id}")
                    tasks.append(L1BAttDeliver(config_path=self.config_path,
                                               orbit_id=orbit_id,
                                               level=self.level,
                                               partition=self.partition,
                                               daac_ingest_queue=self.daac_ingest_queue,
                                               override_output=True))

                if g.startswith("EMIT_L2A_RFL"):
                    # Get acquisition id
                    timestamp = g.split("_")[4].replace("T", "t")
                    acquisition_id = f"emit{timestamp}"
                    logger.info(f"Creating L2ADeliver task for acquisition {acquisition_id}")
                    tasks.append(L2ADeliver(config_path=self.config_path,
                                            acquisition_id=acquisition_id,
                                            level=self.level,
                                            partition=self.partition,
                                            daac_ingest_queue=self.daac_ingest_queue,
                                            override_output=True))

        return tasks
