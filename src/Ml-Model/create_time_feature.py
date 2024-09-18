import pandas as pd


def create_time_features(
    df: pd.DataFrame, prediction_offset_in_hours: int = 1
) -> pd.DataFrame:
    """
    Adds time-related features to the input DataFrame by converting time columns to Unix timestamps,
    calculating duration, and creating a time to predict (future time) based on an offset in hours.

    Args:
        df (pd.DataFrame): The input DataFrame containing time columns.
        prediction_offset_in_hours (int): The number of hours to offset for the prediction target time.

    Returns:
        pd.DataFrame: The DataFrame with added time-related features.
    """
    df["time_period_start_unix"] = (
        pd.to_datetime(df["time_period_start"]).astype(int) / 10**9
    )
    df["time_period_end_unix"] = (
        pd.to_datetime(df["time_period_end"]).astype(int) / 10**9
    )

    df["duration"] = (
        pd.to_datetime(df["time_period_end"]) - pd.to_datetime(df["time_period_start"])
    ).dt.total_seconds()

    prediction_offset_seconds = prediction_offset_in_hours * 3600
    df["time_to_predict_unix"] = df["time_period_end_unix"] + prediction_offset_seconds

    return df
