import logging
from pathlib import Path
import pandas as pd
from typing import List, Tuple

# Hardcoded base directory
# BASE_PATH = Path("/home/sc62291/stla/gt_to_explore_23_04_2025")  # <-- Change this to your desired root directory
BASE_PATH = Path("/home/sc62291/stla/gt_to_explore_28_02_2025")
TARGET_FILENAME = "lane_change_included_gt_data.csv"
LOG_FILE = "gt_file_report.log"

# Configure logger to write to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def find_and_count_gt_first_level(
    base_path: Path,
    target_filename: str
) -> Tuple[List[Tuple[Path, int, int]], List[Path]]:
    """
    Check only the first-level subdirectories under `base_path`, categorize them
    by presence of `target_filename`, and count left/right lane changes.
    Returns two lists:
      - containing_info: list of tuples (dir, right_count, left_count)
      - missing_dirs: list of dirs missing the file
    """
    containing_info: List[Tuple[Path, int, int]] = []
    missing_dirs: List[Path] = []

    for entry in base_path.iterdir():
        if entry.is_dir():
            file_path = entry / target_filename
            if file_path.exists():
                right_count = 0
                left_count = 0
                try:
                    df = pd.read_csv(file_path)
                    if 'is_right_lane_change' in df.columns:
                        right_count = int((df['is_right_lane_change'] == True).sum())
                    else:
                        logger.warning(f"'is_right_lane_change' column not found in {file_path}")
                    if 'is_left_lane_change' in df.columns:
                        left_count = int((df['is_left_lane_change'] == True).sum())
                    else:
                        logger.warning(f"'is_left_lane_change' column not found in {file_path}")
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
                containing_info.append((entry, right_count, left_count))
            else:
                missing_dirs.append(entry)
    return containing_info, missing_dirs


def main() -> None:
    # Validate base path
    if not BASE_PATH.exists() or not BASE_PATH.is_dir():
        logger.error(f"BASE_PATH '{BASE_PATH}' is not a valid directory.")
        return

    logger.info(f"Scanning first-level subdirectories of '{BASE_PATH}' for '{TARGET_FILENAME}'...")
    containing_info, missing = find_and_count_gt_first_level(BASE_PATH, TARGET_FILENAME)

    logger.info(f"Found {len(containing_info)} first-level directories containing the file.")
    logger.info(f"Found {len(missing)} first-level directories missing the file.")

    logger.info("=== First-Level Directories Containing File with Counts ===")
    total_right_lc = 0
    total_left_lc = 0
    for d, right, left in containing_info:
        total_right_lc += right
        total_left_lc += left
        logger.info(f"{d} - Right changes: {right}, Left changes: {left}")
    logger.info(f"Total right lane changes: {total_right_lc}")
    logger.info(f"Total left lane changes: {total_left_lc}")
    
    logger.info("=== First-Level Directories Missing File ===")
    for d in missing:
        logger.info(d)

if __name__ == "__main__":
    main()
