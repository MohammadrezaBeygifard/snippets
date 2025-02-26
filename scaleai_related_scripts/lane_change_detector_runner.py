import subprocess
import logging
from pathlib import Path
from .parquet_creator import GTFinder


def find_json_files(base_path: str):
    logging.info(f"Searching for JSON files in {base_path}")
    base_dir = Path(base_path)
    if not base_dir.is_dir():
        logging.error(f"The provided path {base_path} is not a directory.")
        return []

    json_files = list(base_dir.rglob("*.json"))
    return json_files


def run_command(cwd, command):
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout, stderr = proc.communicate()

    if stdout:
        logging.info(f"stdout: {stdout}")
    if stderr:
        logging.error(f"stderr: {stderr}")

    return proc.returncode


class LaneChangeDetectorRunner:

    def __init__(
        self,
        logger: logging.Logger,
        cwd_ddad: str,
        gt_finder: GTFinder,
    ):
        self.logger = logger
        self.cwd_ddad = cwd_ddad
        self.gt_finder = gt_finder
        self.command_raw = [
            "bazel",
            "run",
            "--config=stla_gcc9",
            "//application/adp_fca/tools/eval/examples:object_prediction_gt_example",
            "--",
            "--json",
            "{json_file}",
        ]

    def run_command(self, command):
        proc = subprocess.Popen(
            command,
            cwd=self.cwd_ddad,
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

    def run(self):
        json_files = self.gt_finder.find_gt_files()
        for json_file in json_files:
            self.logger.info(json_file)
            command = [arg.format(json_file=json_file) for arg in self.command_raw]
            self.run_command(command)
