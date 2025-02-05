from .parquet_creator import ParquetCreator, GTFinder
from .json_fetcher import JsonFetcher
from .task_id_list import list_of_task_ids
import logging
import os
import argparse

GT_BASE_PATH: str = "/home/sc62291/stla/gt_to_explore"
SCALE_AI_SCRIPT_PATH = "/home/sc62291/stla/ScaleAICollaboration"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Reprocess the session connected to a specific GT JSON."
    )
    parser.add_argument(
        "--password",
        type=str,
        required=True,
        help="Password to authenticate to the RAAS API",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        filename="ground_truth_pipeline.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )
    main_logger = logging.getLogger("main_logger")
    parquet_creator_logger = logging.getLogger("parquet_creator_logger")
    fetch_gt_logger = logging.getLogger("fetch_gt")

    main_logger.info("Starting ground truth pipeline")
    username = os.getenv("USER")
    args = parse_arguments()
    password = args.password

    json_fetcher = JsonFetcher(
        scaleai_script_path=SCALE_AI_SCRIPT_PATH,
        list_of_task_ids=list_of_task_ids,
        logger=fetch_gt_logger,
        destination_path=GT_BASE_PATH,
    )

    gt_finder = GTFinder(GT_BASE_PATH, parquet_creator_logger)
    parquet_creator = ParquetCreator(
        GT_BASE_PATH,
        username,
        password,
        gt_finder,
        parquet_creator_logger,
    )
    main_logger.info("Fetching GT JSON files")
    json_fetcher.fetch_json()
    main_logger.info("Creating parquet files by triggering RAAS jobs")
    # TODO: Add parquet_creator_copy inside the run method
    parquet_creator.run()
    main_logger.info("Ground truth pipeline completed")
