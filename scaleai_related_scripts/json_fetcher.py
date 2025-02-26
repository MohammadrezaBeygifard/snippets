from enum import Enum
import subprocess
from typing import List, Tuple
import datetime
from pathlib import Path
import shutil
import logging
from tqdm import tqdm


class JsonFetcher:
    class FetchResult(Enum):
        SUCCESS = 0
        FAILURE = 1

    def __init__(
        self,
        scaleai_script_path: str,
        list_of_task_ids: List[str],
        logger: logging.Logger,
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
        self.logger = logger
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
            self.logger.info(f"stdout: {stdout}")
        if stderr:
            self.logger.error(f"stderr: {stderr}")

        return proc.returncode

    def log_results(self):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"Results for {current_time}:")
        for task_id, result in self.result_list:
            self.logger.info(f"Task ID: {task_id}, Result: {result}")

    def copy_files(self, destination_path: Path, task_id: str):
        self.logger.info(f"Starting to copy files for task_id: {task_id}")
        try:
            source_folder = Path(self.scaleai_script_path) / task_id
            destination_folder = Path(destination_path) / task_id
            if destination_folder.exists():
                self.logger.info(
                    f"Destination folder {destination_folder} exists. Removing it."
                )
                shutil.rmtree(destination_folder)
            self.logger.info(f"Copying from {source_folder} to {destination_folder}")
            shutil.copytree(source_folder, destination_folder)
            self.logger.info(f"Finished copying files for task_id: {task_id}")
        except Exception as e:
            self.logger.error(f"Error while copying files for task_id: {task_id}")
            self.logger.exception(e)

    def run(self):
        self.logger.info("Starting to fetch JSONs")
        for task_id in tqdm(self.list_of_task_ids, desc="Fetching JSONs"):
            self.logger.info(f"Fetching JSON for task_id: {task_id}")
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
        self.logger.info("Starting to copy files without triggering Scale API")
        for task_id in self.list_of_task_ids:
            self.copy_files(self.destination_path, task_id)
        self.logger.info("Finished copying files without triggering Scale API")
