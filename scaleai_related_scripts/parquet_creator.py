###
# This is a script to reprocess the session connected to a specific GT JSON
# Test Version
###

import argparse
import logging
import requests
from requests.auth import HTTPBasicAuth
import os
import json
from pathlib import Path

SUBMIT_JOB_URL = (
    "https://jms-fca-sensor-reprocessing.apps.usprd.p4avd.fcagroup.com/v1/jobs"
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Reprocess the session connected to a specific GT JSON."
    )
    parser.add_argument(
        "--input", type=str, required=True, help="Path to the input JSON file"
    )
    parser.add_argument(
        "--password",
        type=str,
        required=True,
        help="Password to authenticate to the RAAS API",
    )
    return parser.parse_args()


def extract_metadata_from_gt_json(file_path):
    data = parse_input_file(file_path)
    session, start_timestamp, end_timestamp = extract_session_meta_data(data)
    return session, start_timestamp, end_timestamp


def parse_input_file(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def extract_session_meta_data(data):
    metadata = data["metadata"]
    session = metadata["session_id"]
    start_timestamp = metadata["start_timestamp"]
    end_timestamp = metadata["end_timestamp"]
    return session, start_timestamp, end_timestamp


def submit_raas_job(username, password, data):
    response = requests.post(
        SUBMIT_JOB_URL, json=data, auth=HTTPBasicAuth(username, password), verify=False
    )
    return response


class GTFinder:

    def __init__(self, base_path: str, logger: logging.Logger):
        self.base_path = base_path
        self.logger = logger

    def find_gt_files(self):
        self.logger.info(f"Searching for GT files in {self.base_path}")
        base_dir = Path(self.base_path)
        if not base_dir.is_dir():
            self.logger.error(f"The provided path {self.base_path} is not a directory.")
            return []

        gt_files = list(base_dir.rglob("*.json"))
        return gt_files


class ParquetCreator:
    """This class triggers P4AVD jobs to reprocess sessions connected to GT JSON files."""

    def __init__(
        self,
        gt_file_base_path: str,
        username_p4avd: str,
        password_p4avd: str,
        gt_finder: GTFinder,
        logger: logging.Logger,
    ):
        self.gt_file_base_path = gt_file_base_path
        self.username_p4avd = username_p4avd
        self.password_p4avd = password_p4avd
        self.gt_finder = gt_finder
        self.logger = logger

        self.start_timestamp_offset_ns = 20 * 1_000_000_000  # 20 seconds in nanoseconds
        self.end_timestamp_offset_ns = 5 * 1_000_000_000  # 5 seconds in nanoseconds

        self.session = None
        self.start_timestamp = None
        self.end_timestamp = None

        self.SUBMIT_JOB_URL = (
            "https://jms-fca-sensor-reprocessing.apps.usprd.p4avd.fcagroup.com/v1/jobs"
        )

        self.request_body = {
            "tags": ["gen3"],
            "RPUs": [
                {
                    "rpu_type": "mPAD",
                    "software_version": "sp25_cp60_up1_2025_01_22_c39ffa2f58cdf5ae234da870cf67d01be6043bc0",
                }
            ],
            "session": "",
            "loggerStartTime": "",
            "loggerEndTime": "",
        }

    def parse_gt_file(self, gt_file_path: str):
        with open(gt_file_path, "r") as file:
            data = json.load(file)
        return data

    def extract_session_meta_data(self, data):
        metadata = data["metadata"]
        self.session = metadata["session_id"]
        self.start_timestamp = metadata["start_timestamp"]
        self.end_timestamp = metadata["end_timestamp"]

    def extract_metadata_from_gt_json(self, gt_file_path: str):
        data = self.parse_gt_file(gt_file_path)
        self.extract_session_meta_data(data)

    def request_body_builder(self, session, start_timestamp, end_timestamp):
        self.request_body["session"] = session
        self.request_body["loggerStartTime"] = (
            start_timestamp - self.start_timestamp_offset_ns
        )
        self.request_body["loggerEndTime"] = (
            end_timestamp + self.end_timestamp_offset_ns
        )

    def submit_raas_job(self):
        response = requests.post(
            self.SUBMIT_JOB_URL,
            json=self.request_body,
            auth=HTTPBasicAuth(self.username_p4avd, self.password_p4avd),
            verify=False,
        )
        return response

    def run(self):
        gt_files = self.gt_finder.find_gt_files()
        for gt_file in gt_files:
            gt_file_path = gt_file
            self.logger.info(f"Extracting GT file: {gt_file_path}")
            self.extract_metadata_from_gt_json(gt_file_path)
            self.request_body_builder(
                self.session, self.start_timestamp, self.end_timestamp
            )
            response = self.submit_raas_job()
            self.logger.info(f"Response: {json.dumps(response.json(), indent=4)}")
            self.logger.info(f"Status code: {response.status_code}")
