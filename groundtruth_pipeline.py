from scaleai_related_scripts.parquet_creator import ParquetCreator, GTFinder
from scaleai_related_scripts.json_fetcher import JsonFetcher
from scaleai_related_scripts.lane_change_detector_runner import LaneChangeDetectorRunner
from scaleai_related_scripts.task_id_list import (
    list_of_task_ids_legacy,
    list_of_task_ids_prediction,
    list_of_task_ids_others,
)
from scaleai_related_scripts.pose_remover import remove_poses_json
import logging
import os
import argparse

# GT_BASE_PATH: str = "/home/sc62291/stla/gt_to_explore_28_02_2025"
GT_BASE_PATH: str = "/home/sc62291/stla/gt_to_explore_23_04_2025"
SCALE_AI_SCRIPT_PATH = "/home/sc62291/stla/ScaleAICollaboration"
DDAD_PATH = "/home/sc62291/stla/ddad"
LIST_OF_TASK_IDS = list_of_task_ids_legacy


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
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main_logger = logging.getLogger("main_logger")
    fetch_gt_logger = logging.getLogger("fetch_gt")
    lane_change_detector_runner_logger = logging.getLogger(
        "lane_change_detector_runner"
    )
    parquet_creator_logger = logging.getLogger("parquet_creator_logger")

    main_logger.info("Starting ground truth pipeline")
    username = os.getenv("USER")
    args = parse_arguments()
    password = args.password

    json_fetcher = JsonFetcher(
        scaleai_script_path=SCALE_AI_SCRIPT_PATH,
        list_of_task_ids=LIST_OF_TASK_IDS,
        logger=fetch_gt_logger,
        destination_path=GT_BASE_PATH,
    )

    gt_finder = GTFinder(GT_BASE_PATH, parquet_creator_logger)

    lane_change_detector_runner = LaneChangeDetectorRunner(
        lane_change_detector_runner_logger, DDAD_PATH, gt_finder
    )

    parquet_creator = ParquetCreator(
        GT_BASE_PATH,
        username,
        password,
        gt_finder,
        parquet_creator_logger,
    )

    main_logger.info("Fetching GT JSON files")
    #json_fetcher.run()
    main_logger.info("Removing poses.json files")
    remove_poses_json(GT_BASE_PATH)
    main_logger.info("Running lane change detector")
    lane_change_detector_runner.run()
    # main_logger.info("Creating parquet files by triggering RAAS jobs")
    # TODO: Add parquet_creator_copy inside the run method
    # parquet_creator.run()
    # main_logger.info("Ground truth pipeline completed")
