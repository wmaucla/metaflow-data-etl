import csv
import logging
from datetime import datetime

import pandas as pd
from helpers import (
    calculate_dimensional_values,
    calculate_norm,
    calculate_summary_stats,
    convert_timestamp,
    convert_wide_table,
    fill_data,
    postprocess_data,
)
from metaflow import Flow, FlowSpec, Parameter, get_metadata, namespace, step, timeout

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

namespace(None)  # namespace for metaflow - make it global

# Ignore mypy due to metaflow
# mypy: ignore-errors


class DataPipelineFlow(FlowSpec):
    """
    Creates an end-to-end data pipeline process in preparation for model training.

    Parameters:
        dataset_name (str): Dataset used for training
    """

    file_path_loc = Parameter(
        "file_path_loc", help="Absolute reference to file paths", default=""
    )

    dataset_name = Parameter(
        "dataset_name", help="Reference to data step", default="data/sample.parquet"
    )

    robots = Parameter(
        "robots",
        help="Active robot IDs",
        default=[1, 2],
    )

    @step
    def start(self) -> None:
        """
        Use the Metaflow client to retrieve the latest successful run from our
        DataPipelineFlow and assign them as data artifacts in this flow.
        """
        logger.info("Start data pipeline.")

        # Print metadata provider
        logger.info("Using metadata provider: %s", get_metadata())

        # Load the analysis from the Flow.
        run = Flow("DataPipelineFlow").latest_successful_run
        logger.info("Using analysis from '%s'", str(run))

        self.next(self.retrieve_data)

    @step
    def retrieve_data(self) -> None:
        """
        Depends on data source. Here we're choosing to read from disk, however
        could easily be a data stream
        """

        logger.info("Fetching data from source")
        df = pd.read_parquet(self.dataset_name)

        self.df = df

        self.next(self.find_uuids)

    @timeout(seconds=100)
    @step
    def find_uuids(self) -> None:
        """
        Find each ID to distribute the work
        """

        self.run_uuids = list(self.df["run_uuid"].unique())
        self.next(self.data_preprocessing, foreach="run_uuids")

    @step
    def data_preprocessing(self) -> None:
        """
        Preprocess data. For each unique run, filter out the data and flatten
        """

        self.uuid = self.input

        df = self.df
        sample_df = df[df["run_uuid"] == self.uuid]

        df_pivot = convert_wide_table(sample_df)
        df_pivot = convert_timestamp(df_pivot)
        df_fill = fill_data(df_pivot)

        self.df = df_fill
        self.next(self.data_postprocessing)

    @step
    def data_postprocessing(self) -> None:
        """
        Do additional postprocessing, including generate relevant features
        and write out accordingly
        """
        df = self.df

        for robot in self.robots:
            calculate_dim = calculate_dimensional_values(df, robot=robot)
            output = calculate_norm(calculate_dim, robot=robot)
            output = postprocess_data(output)

        data = calculate_summary_stats(output, self.robots)

        # File path to write text data
        file_path = f"{self.file_path_loc}data/summary_{self.uuid}.txt"

        # Write the tuple to a text file - note could be a DB too instead
        with open(file_path, "w") as file:
            file.write(",".join(data))

        self.next(self.data_summary)

    @step
    def data_summary(self, inputs) -> None:
        """
        Create a dataset summary for each dataframe
        """

        output = [
            [
                "run_start_time",
                "run_stop_time",
                "total_runtime",
                "total_distance_traveled",
                "run_uuid",
            ]
        ]

        # Read the contents of the file and extend with the run uuid
        for row in inputs:
            with open(f"{self.file_path_loc}data/summary_{row.input}.txt", "r") as file:
                for line in file:
                    values = [value.strip() for value in line.strip().split(",")]
                values.append(row.input)
                output.append(values)

        logger.info("Aggregation input complete")

        # All file outputs should have a timestamp
        cur_time = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
        file_path = f"{self.file_path_loc}data/summary_run_{cur_time}.csv"
        with open(file_path, "w", newline="") as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(output)

        self.next(self.end)

    @step
    def end(self) -> None:
        """
        Log that the ETL has finished.
        """
        logger.info("Data ETL pipeline completed.")


if __name__ == "__main__":
    DataPipelineFlow()
