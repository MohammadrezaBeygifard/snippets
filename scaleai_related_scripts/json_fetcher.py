from enum import Enum
import subprocess
from typing import List, Tuple
import datetime
from pathlib import Path
import shutil
import logging
from tqdm import tqdm

logging.basicConfig(
    filename="scaleai_fetcher_results.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

subprocess_logger = logging.getLogger("subprocess")
subprocess_handler = logging.FileHandler("subprocess_output.log")
subprocess_handler.setLevel(logging.INFO)
subprocess_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
subprocess_logger.addHandler(subprocess_handler)


class JsonFetcher:
    class FetchResult(Enum):
        SUCCESS = 0
        FAILURE = 1

    def __init__(
        self,
        scaleai_script_path: str,
        list_of_task_ids: List[str],
        destination_path: str = None,
    ):
        self.scaleai_script_path = scaleai_script_path
        self.list_of_task_ids = list_of_task_ids
        self.fetch_command = [
            "python",
            "fetch_merged_scale_response.py",
            "--scale_task_id={task_id}",
        ]
        self.result_list: List[Tuple[str, str]] = []
        self.destination_path = None
        if destination_path:
            self.destination_path = Path(destination_path)
            self.destination_path.mkdir(parents=True, exist_ok=True)

    def run_fetch_command(self, command):
        proc = subprocess.Popen(
            command,
            cwd=self.scaleai_script_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout, stderr = proc.communicate()

        if stdout:
            subprocess_logger.info(f"stdout: {stdout}")
        if stderr:
            subprocess_logger.error(f"stderr: {stderr}")

        return proc.returncode

    def log_results(self):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Results for {current_time}:")
        for task_id, result in self.result_list:
            logging.info(f"Task ID: {task_id}, Result: {result}")

    def copy_files(self, destination_path: Path, task_id: str):
        logging.info(f"Starting to copy files for task_id: {task_id}")
        try:
            source_folder = Path(self.scaleai_script_path) / task_id
            destination_folder = Path(destination_path) / task_id
            if destination_folder.exists():
                logging.info(
                    f"Destination folder {destination_folder} exists. Removing it."
                )
                shutil.rmtree(destination_folder)
            logging.info(f"Copying from {source_folder} to {destination_folder}")
            shutil.copytree(source_folder, destination_folder)
            logging.info(f"Finished copying files for task_id: {task_id}")
        except Exception as e:
            logging.error(f"Error while copying files for task_id: {task_id}")
            logging.exception(e)

    def fetch_json(self):
        logging.info("Starting to fetch JSONs")
        for task_id in tqdm(self.list_of_task_ids, desc="Fetching JSONs"):
            command = [arg.format(task_id=task_id) for arg in self.fetch_command]
            result = self.run_fetch_command(command)
            if result == 0:
                self.result_list.append((task_id, self.FetchResult.SUCCESS))
            else:
                self.result_list.append((task_id, self.FetchResult.FAILURE))
        self.log_results()

        if self.destination_path:
            for task_id in self.list_of_task_ids:
                self.copy_files(self.destination_path, task_id)

    def copy_files_without_triggering_scale_api(self):
        logging.info("Starting to copy files without triggering Scale API")
        for task_id in self.list_of_task_ids:
            self.copy_files(self.destination_path, task_id)
        logging.info("Finished copying files without triggering Scale API")


if __name__ == "__main__":

    list_of_task_ids = [
        "6740edbdc4d4fb995633b047",
        "6740edc98d8e980043f457ec",
        "6740edcfcc3dd8758b8f1164",
        "6740edb637068390b6521594",
        "6740edc3254df7117b20ca4c",
        "6740f1def65ff89f928e59dd",
        "6740f1ef04177b3df386c98b",
        "6740f1f5ddffb496be191008",
        "6740f1d76ebd08d125109b5c",
        "6740f1e4043697233c902688",
        "6740f1d24807ac54030a33d9",
        "6740f1ea36c75a1161e6e3a3",
        "6740f1cc5982cccf831f8109",
        "6759dcc0130e429cc479508a",
        "6759dc820ef3c682a527f7e0",
        "6759dd08e52df5ff8f6be955",
        "6759dc79bec3bdec452bb796",
        "6759dc9a7be71e350569a520",
        "6759dcf6ccd038b9b9fc0e4b",
        "6759dcacd79e8da94083e3a3",
        "6759dcc6e9658cce014db400",
        "6759dca3d79e8da94083e145",
        "6759dd401b3d9a6c57aa7824",
        "6759dd39f32f70397f27a6fb",
        "6759dd4aa18b7584773ad956",
        "6759dc90f32f70397f279751",
        "6759dd336a209a7adbe761c4",
        "6759dccb2310d7f32d50f465",
        "6759dd227f35635c709b5361",
        "6759dd447be71e350569b931",
        "6759dcd1cb162d0a625ff4f8",
        "6759dd2ad79e8da940840300",
        "674f2d1d54fba0a4185f5853",
        "6759dcff1d40785c26fff495",
        "6759dcd9a3161d835587d969",
        "6759dd1a422b5f5b7f436ddd",
        "6759dced4f49168527f17a80",
        "6759dce5130e429cc4795473",
        "6759dc9a7be71e350569a520",
        "6759dcf6ccd038b9b9fc0e4b",
        "6759dcacd79e8da94083e3a3",
        "6759dcc6e9658cce014db400",
        "6759dca3d79e8da94083e145",
        "6759dd401b3d9a6c57aa7824",
        "6759dd39f32f70397f27a6fb",
        "6759dd4aa18b7584773ad956",
        "6759dc90f32f70397f279751",
        "6759dd336a209a7adbe761c4",
        "6759dccb2310d7f32d50f465",
        "6759dd227f35635c709b5361",
        "6759dd447be71e350569b931",
        "6759dcd1cb162d0a625ff4f8",
        "6759dd2ad79e8da940840300",
        "6759dcff1d40785c26fff495",
        "6759dcd9a3161d835587d969",
        "6759dd1a422b5f5b7f436ddd",
        "6759dced4f49168527f17a80",
        "6759dce5130e429cc4795473",
        "6762213b2a01a25f86f18e26",
        "6762219ea5f650343518db0f",
        "6762203a89d6819a9f5dbf01",
        "67622092430f4f3cc5a8410f",
        "67621fd17a6bc4aebb75b740",
        "676221197a6bc4aebb75d329",
        "67621f9533c19737115d6c68",
        "67622195d9a404a433275a39",
        "67621f8cf00708184ca7a5e2",
        "67621feca5f650343518af2a",
        "6762217a346a1db40148c560",
        "67622015e35e2e81e0bf2df7",
        "676220cd79ae9a33495177ec",
        "67621ff521d532b98952f033",
        "67622267e49cb60ee53526e8",
        "6762221a0a3e65e821036fc9",
        "67622212e2cbf0172a9d0327",
        "6762200c3809c46873b6475f",
        "6762226fee0a5b12628df179",
        "67621fa6894d3de3fd220aa3",
        "676221d3b7be5f980943db25",
        "676220d6d9a404a4332741b0",
        "676221c1b6d4930b77a73568",
        "6762222c8affb9da3488074d",
        "676220f88e00ceba4a9fc2d0",
        "676221ca6e573e3f77076c32",
        "6762209b79ae9a33495174d2",
        "67622158d769e614184887c6",
        "676220608e00ceba4a9b93af",
        "6762208a8043663cdb5f8ba6",
        "676221836f8094167c16b365",
        "67622129d9a404a433274ae8",
        "676220efd5dc7f8d3fc6713c",
        "676221b8513546279293198c",
        "676221325135462792930a0e",
        "676221a62db89497dda176e6",
        "6762216ad769e61418488971",
        "67622172c45d687bc4e087b8",
        "6762220014b2bf36f45ccf4d",
        "67622081a5f650343518c112",
        "6762216033c19737115d85ea",
        "67621fc8736259e3c8e81053",
        "676220b02b51ecbb0d6be12c",
        "67622026e35e2e81e0bf2fcc",
        "6762201f189b562d4f93e3e2",
        "6762203284b1e1a6f5fb0745",
        "676222392fa2d2a32b43c272",
        "6762204ee35e2e81e0bf33e1",
        "67622272880bec7f6b2210d7",
        "676220046d7e7ec00de45f49",
        "676220ffaa63011319cc1b06",
        "67621f9e0f22e965d49d34fc",
        "676221073793084bc8e59115",
        "67621fe332a93d256d80eb1c",
        "676220de3793084bc8e58d44",
        "676221af8ac5a59f9fd84662",
        "67622058b7be5f980943c24c",
        "67622223c5137244492ba5db",
        "6762204585b542d2c1c8be2c",
        "676220697a6bc4aebb75c54c",
        "67621fb89c9c448ef62d17be",
        "676221102764731b2c768881",
        "67621fafaa63011319c7af07",
        "676221dc2b51ecbb0d6bfa04",
        "67622241c45d687bc4e095b6",
        "676222089ef41c8bcbbdb402",
        "6762218bb7be5f980943d67c",
        "67621ffdf00708184ca7b9ed",
        "676221ed6e573e3f77076c86",
        "676221e55135462792932066",
        "676220c2f00708184ca7c855",
        "676220ba38dff72c1608173c",
        "67621fc06f8094167c16ab63",
        "676220e7334cda912dc07866",
        "676220a8f00708184ca7c559",
        "676221f72fc180ef79eae68c",
        "67621fda8e00ceba4a9b7a52",
    ]
    scaleai_script_path = "/home/sc62291/stla/ScaleAICollaboration"
    destination_path = "/home/sc62291/stla/gt_to_explore"
    json_fetcher = JsonFetcher(scaleai_script_path, list_of_task_ids, destination_path)

    json_fetcher.fetch_json()
    # json_fetcher.copy_files_without_triggering_scale_api()
