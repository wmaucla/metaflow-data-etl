from datetime import datetime
from typing import List, Tuple

import pandas as pd


def convert_wide_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a tall table to a wide table based on time.

    Args:
        df (pd.DataFrame): The input DataFrame containing data in wide format.

    Returns:
        pd.DataFrame: DataFrame with data flattened based on time.
    """

    df_pivot = df.pivot_table(
        index="time", columns=["field", "robot_id"], values="value"
    )
    df_pivot.columns = pd.Index([f"{col[0]}_{col[1]}" for col in df_pivot.columns])
    return df_pivot


def convert_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function converts timestamps from the default ISOformat into epoch seconds.
    Epoch seconds are used to facilitate the calculation of features.

    Args:
        df (pd.DataFrame): The input DataFrame containing timestamps in ISOformat.

    Returns:
        pd.DataFrame: DataFrame with timestamps converted to epoch seconds.
    """
    df["dt"] = (
        pd.to_datetime(df.index, format="ISO8601").astype(int) / 10**9
    )  # cast to seconds
    df = df.sort_values(by="dt")
    return df


def fill_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing data points in the DataFrame using a defined strategy.

    Strategy:
        1. Interpolate between existing data points to smooth out the data.
        2. Backfill any remaining missing data points.

    Args:
        df (pd.DataFrame): The input DataFrame containing potentially missing data points.

    Returns:
        pd.DataFrame: DataFrame with missing data points filled according to the defined strategy.
    """

    return df.interpolate().bfill()


def calculate_dimensional_values(
    df: pd.DataFrame,
    robot: str,
    dims: List[str] = ["x", "y", "z"],
) -> pd.DataFrame:
    """
    Calculate velocity and acceleration.

    Args:
        df (pd.DataFrame): The input DataFrame containing dimensional values.
        robot (str): The identifier of the robot.
        dims (List[str]): The dimensions to calculate values for (default is ["x", "y", "z"]).

    Returns:
        pd.DataFrame: DataFrame with added columns for distance, velocity and acceleration.
    """

    # calculate deltas
    diffed = df.diff()

    # calculate change in time and assign back to dataframe
    df["dt"] = diffed["dt"]

    for col in dims:
        if f"{col}_{robot}" in df.columns:  # possible that data not available
            df[f"d{col}_{robot}"] = diffed[
                f"{col}_{robot}"
            ]  # calculate distance traveled as diff of coordinates
            df[f"v{col}_{robot}"] = (
                df[f"d{col}_{robot}"] / diffed["dt"]
            )  # calculate velocity as distance / time
            df[f"a{col}_{robot}"] = (
                df[f"v{col}_{robot}"].diff() / diffed["dt"]
            )  # take diff of velocity

    return df


def calculate_norm(
    df: pd.DataFrame, robot: str, calculations: List[str] = ["f", "a", "v", "d"]
) -> pd.DataFrame:
    """
    Calculate the total value for each specified prefix.

    Args:
        df (pd.DataFrame): The input DataFrame containing dimensional values.
        robot (str): The identifier of the robot.
        calculations (List[str]): The prefixes for which to calculate total values
            (default is ["f", "a", "v", "d"]). Possible values are "f" (force), "a" (acceleration),
            "v" (velocity), and "d" (distance).

    Returns:
        pd.DataFrame: DataFrame with added columns for the total values of specified prefixes.
    """

    for prefix in calculations:
        df[f"{prefix}{robot}"] = (
            sum(
                df[f"{prefix}{coord}_{robot}"] ** 2
                for coord in ["x", "y", "z"]
                if f"{prefix}{coord}_{robot}" in df.columns
            )
            ** 0.5
        )

    return df


def postprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Backfill missing data with zeros after calculating features.

    Args:
        df (pd.DataFrame): The input DataFrame containing calculated features.

    Returns:
        pd.DataFrame: DataFrame with missing data backfilled with zeros.
    """

    return df.fillna(0)


def calculate_summary_stats(
    df: pd.DataFrame, robots: List[int]
) -> Tuple[str, str, str, str]:
    """
    Calculate summary statistics for run start time, stop time, runtime, and total distance traveled.

    Args:
        df (pd.DataFrame): The input DataFrame containing data for multiple robots.
        robots (List[int]): A list of identifiers for which to calculate summary statistics.

    Returns:
        Tuple[str, str, str, str]: A tuple containing summary statistics for run start time,
            stop time, runtime, and total distance traveled.
    """

    start_time = df.index.min()
    stop_time = df.index.max()

    run_time = datetime.fromisoformat(stop_time) - datetime.fromisoformat(start_time)

    distance_traveled = sum(df[f"d{robot}"] for robot in robots).sum()

    return str(start_time), str(stop_time), str(run_time), str(distance_traveled)
