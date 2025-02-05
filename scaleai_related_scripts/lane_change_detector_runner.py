import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    filename="lcd_runner.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)


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


if __name__ == "__main__":
    command = [
        "bazel",
        "run",
        "--config=stla_gcc9",
        "//application/adp_fca/tools/eval/examples:object_prediction_gt_example",
        "--",
        "--json",
        "{json_file}",
        "--parquet",
        "/home/sc62291/Desktop/01_66ba59ed2efa0b97e2300f79/output_0.parquet",
        "--init_ts=0",
        "--end_ts=9999999999999999",
    ]

    gt_base_path = "/home/sc62291/stla/gt_to_explore"
    cwd_ddad = "/home/sc62291/stla/ddad"
    json_files = find_json_files(gt_base_path)
    for json_file in json_files:
        #print(json_file)
        command = [arg.format(json_file=json_file) for arg in command]
        run_command(cwd_ddad, command)
    # json_file = "/home/sc62291/stla/gt_to_explore/6740edb637068390b6521594/51573d75-501b-4cdb-acf4-0192d321a72f_1730118482005356832.json"
    # command = [arg.format(json_file=json_file) for arg in command]
    # cwd_ddad = "/home/sc62291/stla/ddad"
    # run_command(cwd_ddad, command)
