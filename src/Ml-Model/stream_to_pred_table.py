import logging
from typing import Optional
from google.cloud import bigquery
import pandas as pd


def stream_df_to_bigquery(
    bigquery_client: bigquery.Client,
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    timeout: int = 60,  # Adding a timeout to the function
) -> Optional[None]:
    """
    Streams data from a Pandas DataFrame to a BigQuery table.
    Includes detailed logging and timeout handling.

    Args:
        bigquery_client (bigquery.Client): An authenticated BigQuery client.
        df (pd.DataFrame): The DataFrame containing the data to stream to BigQuery.
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        timeout (int): Timeout for the request in seconds.

    Returns:
        None
    """
    table = f"{project_id}.{dataset_id}.{table_id}"

    # Convert DataFrame to list of dictionaries (BigQuery format)
    data = df.to_dict(orient="records")

    # Ensure the DataFrame has data
    if df.empty:
        logging.info("Error: The DataFrame is empty.")
        return

    logging.info(f"Preparing to stream {len(data)} rows to BigQuery table {table}")

    try:
        # Stream data to BigQuery
        errors = bigquery_client.insert_rows_json(
            table=table, json_rows=data, timeout=timeout
        )
        if errors:
            logging.info("Encountered errors while inserting rows:")
            for error in errors:
                logging.info(error)
        else:
            logging.info(f"Data successfully streamed to BigQuery table {table}.")
    except Exception as e:
        logging.error(f"Error streaming data to BigQuery: {e}")
