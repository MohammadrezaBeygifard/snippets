from pathlib import Path
import logging

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
