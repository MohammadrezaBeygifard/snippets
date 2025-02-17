from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def remove_poses_json(base_path: str):
    base_dir = Path(base_path)
    if not base_dir.is_dir():
        logging.error(f"The provided path {base_path} is not a directory.")
        return

    # Iterate over all files named 'poses.json' in the directory and subdirectories
    for poses_file in base_dir.rglob("poses.json"):
        try:
            poses_file.unlink()
            logging.info(f"Removed file: {poses_file}")
        except Exception as e:
            logging.error(f"Error removing file {poses_file}: {e}")


if __name__ == "__main__":
    # Specify the base path here
    base_path = "/home/sc62291/stla/gt_to_explore"
    remove_poses_json(base_path)
