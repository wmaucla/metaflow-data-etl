import numpy as np
import pandas as pd

from core.helpers import (
    calculate_dimensional_values,
    calculate_norm,
    convert_timestamp,
    convert_wide_table,
)


def test_convert_timestamp(fake_df_base: pd.DataFrame) -> None:
    """
    Check that ISO format is correctly picked up and sorts the dataframe accordingly
    """
    fake_df_base.index = pd.Index(fake_df_base["time"])

    test_timestamp = convert_timestamp(fake_df_base)

    # Check that the test_timestamp is properly sorted
    assert test_timestamp["dt"].iloc[0] < test_timestamp["dt"].iloc[-1]


def test_widen_table(fake_df_flat: pd.DataFrame) -> None:
    """
    Check that assumptions about how to flatten a table are valid
    """
    test_df = convert_wide_table(fake_df_flat)

    # Check that there is a new column created due to the flattening
    assert "z_3" in test_df.columns

    # Check for assumed value
    assert test_df["z_3"].sum() == -2000

    # Check that it flattened against timestamp correctly w/ sum on that row
    test_df[~test_df["z_3"].isna()].sum().sum() != -2000


def test_calculate_norm(norm_df: pd.DataFrame) -> None:
    """
    Test the way to calculate total force, acceleration, velocity, and distance

    Tests happening:
    1. Test if a column is missing
    2. Test calculations are correct
    """

    # Define expected result
    expected_result = pd.DataFrame(
        {
            "f1": [3, 7, 9],  # test quadruples
            "a1": [5, 13, 25],  # squares only
            "v1": [1, 2, 3],  # no squares
            "d1": [1, 2, 3],  # test singles
        }
    ).astype(float)

    # Apply the function to the sample DataFrame
    result = calculate_norm(norm_df, "1")

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result[["f1", "a1", "v1", "d1"]], expected_result)

    # Now calculate for a different robot number
    expected_result = pd.DataFrame(
        {
            "d2": [5, 13, 25],  # test with two different robots
        }
    ).astype(float)

    result = calculate_norm(norm_df, "2")

    # Check if the result matches the expected result
    pd.testing.assert_frame_equal(result[["d2"]], expected_result)


def test_calculate_dimension_values(dim_df: pd.DataFrame) -> None:
    """
    Test the way to calculate distance, velocity, acceleration

    Tests happening:
    1. Ignores empty column (column "z")
    """

    # Apply the function to the sample DataFrame
    result = calculate_dimensional_values(dim_df, "1")

    # Check to make sure timestamp is calculated correctly
    expected_result = pd.Series([np.nan, 1.0, 1.0, 2.0], name="dt")

    pd.testing.assert_series_equal(result["dt"], expected_result)

    # Check to make sure z is ignored
    assert "dz_1" not in result.columns

    # Check calculations
    for dim in ["x", "y"]:

        # Count that there are 2 NaNs in acceleration due to second order
        assert result[f"a{dim}_1"].isna().sum() == 2

        # Count 1 NaNs for velocity
        assert result[f"v{dim}_1"].isna().sum() == 1

        # Count 1 NaNs for distance traveled
        assert result[f"d{dim}_1"].isna().sum() == 1
