from typing import Optional
from google.cloud import bigquery
import logging
import pandas as pd


def stream_df_to_bigquery(
    bigquery_client: bigquery.Client,
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> Optional[None]:
    """
    Streams data from a Pandas DataFrame to a BigQuery table.

    Args:
        bigquery_client (bigquery.Client): An authenticated BigQuery client.
        df (pd.DataFrame): The DataFrame containing the data to stream to BigQuery.
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        None
    """
    table = f"{project_id}.{dataset_id}.{table_id}"

    # Convert DataFrame to list of dictionaries (BigQuery format)
    data = df.to_dict(orient='records')

    # Ensure the DataFrame has data
    if df.empty:
        logging.info("Error: The DataFrame is empty.")
        return

    # Stream data to BigQuery
    errors = bigquery_client.insert_rows_json(table=table, json_rows=data)
    if errors:
        logging.info("Encountered errors while inserting rows:")
        for error in errors:
            logging.info(error)
    else:
        logging.info("Data successfully streamed to BigQuery.")