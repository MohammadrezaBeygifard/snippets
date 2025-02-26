# snippets

# GroundTruth Pipeline

To run this pipeline, you need to copy and paste the `object_prediction_gt_example.py` from this repo to ddad. This is necessary as the example file in this repo does not need the parquet file, and saves the csv file in the location of the Ground truth JSON.

This is the command to run the ground truth pipeline.

```bash
python groundtruth_pipeline.py --password <stla-password>
```

The command will fetch the JSONs for which there task id is mentioned in the `task_id_list.py` file.

Moreover, you need to ensure that the API_Key of Scale is provided.

The pipeline, fetches the json of GT, copies it in a directory that you mention in the `GT_BASE_PATH`. Then runs the example code, object_prediction_gt_example.py, to generate the labels.

If you want you can uncomment the `parquet_creator.run()` to create parquet data for the GT data by triggering the raas.

If you re-run the pipeline, it will remove the previously fetched Jsons, so if you want to keep them, make a backup of the `gt_to_explore` directory.

# Comment code to perform a specific task

If you want the pipeline to perform a specific task, you can comment other task commands. Here is a list of task commands and a brief introduction on what they do:

`json_fetcher.run()`
It fetches the SCALE Jsons and remove the old JSONs if any is present

`remove_poses_json(GT_BASE_PATH)`
It removes the pose.json file fetched from the Scale API, as our team, prediction does not need them at this stage we remove them, but feel free to comment this task to avoid the removal.

`lane_change_detector_runner.run()`
It runs the `object_prediction_gt_example.py`, take a look at the beginning of this README, to generate the lane change labels. Comment it if you are not interested in this feature.

`parquet_creator.run()`
This task is in development, ideally it will trigger RAAS for each JSON to generate a parquet file. Keep it commented until the development of this task finishes.

# Constants
`GT_BASE_PATH`: The path to which you want to have the JSONs saved
`SCALE_AI_SCRIPT_PATH`: Path to the Scale Collaboration Script
`DDAD_PATH`: Path to the DDAD repository.