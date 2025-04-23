import json
from pathlib import Path

def get_session_ids(directory: Path):
    """Extracts session_ids from all JSON files in a given directory and its subdirectories."""
    session_ids = set()
    # Using '**/*.json' to recursively find all JSON files in the directory
    for json_file in directory.glob("**/*.json"):
        try:
            with json_file.open(encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict) and "metadata" in data and "session_id" in data["metadata"]:
                    session_ids.add(data["metadata"]["session_id"])
        except Exception as e:
            print(f"Error processing file {json_file}: {e}")
    return session_ids

def main():
    # Update these paths to your specific directories containing JSON files
    directory1 = Path("/home/sc62291/stla/gt_to_explore_28_02_2025")
    directory2 = Path("/home/sc62291/stla/gt_to_explore_perception_with_lane_change_06_03_2025")

    # Retrieve session_ids from each directory
    session_ids_dir1 = get_session_ids(directory1)
    session_ids_dir2 = get_session_ids(directory2)

    # Find common session_ids between the two sets
    common_session_ids = session_ids_dir1.intersection(session_ids_dir2)

    if common_session_ids:
        print("Common session_ids found:")
        for session_id in common_session_ids:
            print(session_id)
    else:
        print("No common session_ids found.")

if __name__ == "__main__":
    main()
