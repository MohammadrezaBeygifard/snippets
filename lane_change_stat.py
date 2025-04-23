import os
import pandas as pd

def count_lane_changes(directory):
    right_count = 0
    left_count = 0

    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file == "lane_change_included_gt_data.csv":
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(file_path)
                    
                    # Ensure the expected columns exist
                    if 'is_right_lane_change' in df.columns:
                        # Count occurrences where is_right_lane_change is True
                        right_count += (df['is_right_lane_change'] == True).sum()
                    else:
                        print(f"Warning: 'is_right_lane_change' column not found in {file_path}")
                    
                    if 'is_left_lane_change' in df.columns:
                        # Count occurrences where is_left_lane_change is True
                        left_count += (df['is_left_lane_change'] == True).sum()
                    else:
                        print(f"Warning: 'is_left_lane_change' column not found in {file_path}")
                
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    return right_count, left_count

if __name__ == "__main__":
    # Set the directory you want to search; using current directory here.
    directory = "/home/sc62291/stla/gt_to_explore_23_04_2025"
    #directory = "/home/sc62291/stla/gt_to_explore_28_02_2025"
    right, left = count_lane_changes(directory)
    
    print(f"Total 'is_right_lane_change' True count: {right}")
    print(f"Total 'is_left_lane_change' True count: {left}")
