import pandas as pd
from datetime import datetime

def prepare_df_for_bigquery(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares a DataFrame for BigQuery by converting datetime columns and 
    Unix timestamp columns to BigQuery-friendly formats, and then converts the DataFrame to a dictionary.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
    
    Returns:
        dict: The DataFrame converted into a dictionary format ready for streaming to BigQuery.
    """
    df = df.copy()  # Make a copy to avoid modifying the original DataFrame
    
    # Step 1: Convert datetime64 columns to ISO 8601 format (which BigQuery accepts as TIMESTAMP)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    # Step 2: Convert Unix timestamp columns (detected by column names containing "unix") to ISO 8601 format
    for col in df.columns:
        if "unix" in col.lower():
            df[col] = df[col].apply(lambda x: datetime.utcfromtimestamp(x).isoformat() if pd.notnull(x) else None)



    return df
