import pandas as pd
from pathlib import Path
from application.adp_fca.tools.eval.associations.object_association import ObjectAssociation
from application.adp_fca.tools.eval.codecs.localization.scaleai_localization_adjustment_codec import (
    ScaleAILocalizationAdjustmentCodec,
)
from application.adp_fca.tools.eval.codecs.objects.predicted_object_codec import PredictedObjectCodec
from application.adp_fca.tools.eval.codecs.objects.scaleai_object_codec import ScaleAIObjectCodec
from application.adp_fca.tools.eval.codecs.utils import get_transformation_matrices_from_ego_position
from application.adp_fca.tools.eval.eval_toolkit import EvalToolkit
from application.adp_fca.tools.eval.examples.examples_common import compute_prediction_metrics, parse_arguments
from application.adp_fca.tools.eval.codecs.roads.scaleai_road_codec import ScaleAIRoadCodec
from application.adp_fca.tools.eval.lane_change_detection.lane_change_detector import LaneChangeDetector
import argparse

pd.set_option("display.max_colwidth", None)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Object prediction ground truth example")
    parser.add_argument(
        "--json",
        type=str,
        help="Path to the JSON file containing the ground truth data",
        required=True,
    )
    parser.add_argument(
        "--parquet",
        type=str,
        help="Path to the Parquet file containing the predicted objects",
        required=False,
        default=None,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    json_file = args.json
    output_dir: Path = Path(args.json).parent
    parquet_file = args.parquet

    json_data = EvalToolkit.read_json(json_file)

    # Extract data from JSON and Parquet using the desired data class
    original_gt_data = EvalToolkit.extract(json_data, ScaleAIObjectCodec())

    road_gt = EvalToolkit.extract(json_data, ScaleAIRoadCodec())
    lanes = road_gt["lanes"]
    lanes.to_csv("lane_gt_data.csv", index=False)

    lane_change_detector = LaneChangeDetector()
    lane_change_included_gt_data = lane_change_detector.extract(original_gt_data, lane_data=road_gt["lanes"])

    csv_output_dir = output_dir / "lane_change_included_gt_data.csv"
    print("Copy csv file in %s", csv_output_dir)
    lane_change_included_gt_data.to_csv(csv_output_dir, index=False)

    if parquet_file:
        parquet_data = EvalToolkit.read_parquet(parquet_file)
        predicted_objects = EvalToolkit.extract(parquet_data, PredictedObjectCodec())

        localization_adjustment_codec = ScaleAILocalizationAdjustmentCodec()
        ego_positions = EvalToolkit.extract(json_data, localization_adjustment_codec)

        transformations_to_the_ego_frames = get_transformation_matrices_from_ego_position(ego_positions)

        print("Ground Truth Data")
        print(original_gt_data.head(10))
        print("Predicted Objects")
        print(predicted_objects.head(10))
        print("Transformation matrices")
        print(transformations_to_the_ego_frames)

        object_associations = ObjectAssociation.associate_objects(original_gt_data, predicted_objects)
        print("Object Associations")
        print(object_associations)

        print(object_associations["ID_HF"].values)

        results = compute_prediction_metrics(
            object_associations,
            original_gt_data,
            predicted_objects,
            transformations_to_the_ego_frames,
        )
        print("Results with error:")
        print(results[results["kpi_error_status"].eq(True)])
        print("All results: ")
        print(results)


if __name__ == "__main__":
    main()
