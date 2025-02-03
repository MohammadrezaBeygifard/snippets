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

SUBMIT_JOB_URL = "https://jms-fca-sensor-reprocessing.apps.usprd.p4avd.fcagroup.com/v1/jobs"


def parse_arguments():
    parser = argparse.ArgumentParser(description="Reprocess the session connected to a specific GT JSON.")
    parser.add_argument("--input", type=str, required=True, help="Path to the input JSON file")
    parser.add_argument("--password", type=str, required=True, help="Password to authenticate to the RAAS API")
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
    response = requests.post(SUBMIT_JOB_URL, json=data, auth=HTTPBasicAuth(username, password), verify=False)
    return response


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    args = parse_arguments()
    logging.info(f"Input file: {args.input}")
    username = os.getenv("USER")
    password = args.password
    session, start_timestamp, end_timestamp = extract_metadata_from_gt_json(args.input)
    data = {
        "tags": ["gen3"],
        "RPUs": [
            {
                "rpu_type": "mPAD",
                "software_version": "sp25_cp60_up1_2025_01_22_c39ffa2f58cdf5ae234da870cf67d01be6043bc0",
            }
        ],
        "session": session,
        "loggerStartTime": start_timestamp,
        "loggerEndTime": end_timestamp,
    }
    response = submit_raas_job(username, password, data)
    logging.info(f"Response: {json.dumps(response.json(), indent=4)}")
    logging.info(f"Status code: {response.status_code}")
