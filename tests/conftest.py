import random

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def base_date() -> str:
    # Serves as base date w/o microseconds - based on data sampled
    return "2022-11-23T20:40:00."


@pytest.fixture
def fake_df_base(base_date: str) -> pd.DataFrame:
    # Determine size of random timestamps
    n = 10

    random_times = []

    base_val = 0  # start from 0 microseconds
    # Generate n random intervals and add them to the start timestamp
    for _ in range(n):
        random_interval = int(random.uniform(0, 10))
        base_val += random_interval
        random_times.append(base_date + str(base_val).zfill(3) + "Z")

    random_values = np.random.randint(
        -1000, 3000, size=n
    )  # chosen based on values from EDA
    df = pd.DataFrame({"time": random_times, "value": random_values})

    return df.sample(frac=1)  # randomize data


@pytest.fixture
def fake_df_flat(fake_df_base: pd.DataFrame) -> pd.DataFrame:
    fake_len = len(fake_df_base)
    choice_robot = np.array([1, 2])
    choice_field = np.array(["x", "y"])

    # Use the indices to select random values from the choices
    fake_df_base["field"] = choice_field[
        np.random.randint(0, len(choice_field), size=fake_len)
    ]
    fake_df_base["robot_id"] = choice_robot[
        np.random.randint(0, len(choice_robot), size=fake_len)
    ]

    # Make an exception for one of the rows

    # Duplicate values in last row
    fake_row = fake_df_base.iloc[-1]
    fake_row["field"], fake_row["value"], fake_row["robot_id"] = "z", -2000, 3
    fake_df_base.loc[fake_len] = fake_row
    return fake_df_base


@pytest.fixture
def norm_df() -> pd.DataFrame:
    # Creates a dataframe that forms pythagorean quadruple
    return pd.DataFrame(
        {
            "fx_1": [1, 2, 4],
            "fy_1": [2, 3, 4],
            "fz_1": [2, 6, 7],
            "ax_1": [3, 5, 7],
            "ay_1": [4, 12, 24],
            "vx_1": [1, 2, 3],
            "dx_1": [1, 2, 3],
            "dy_2": [3, 5, 7],
            "dz_2": [4, 12, 24],
        }
    )


@pytest.fixture
def dim_df() -> pd.DataFrame:
    # Creates a dataframe for testing distance, velocity, acceleration
    return pd.DataFrame(
        {
            "dt": [1, 2, 3, 5],
            "x_1": [5, 10, 15, 20],
            "y_1": [2, 4, 6, 8],
        }
    )
