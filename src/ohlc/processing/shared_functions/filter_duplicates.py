from typing import List, Dict
from google.cloud import bigquery
from datetime import datetime
import logging
from shared_functions.datetime_helper import (
    convert_unix_timestamp_in_data,
    convert_datetime_to_string,
)


from datetime import datetime
import logging

def filter_duplicates_ohlc(
    bigquery_client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    raw_data: List[Dict],
) -> List[Dict]:
    """
    Retrieves the time_period_start timestamps from the clean table, checks which of the raw data rows are already in the clean table,
    and removes those rows from the raw data.

    Args:
        bigquery_client (bigquery.Client): The BigQuery client instance.
        dataset_id (str): The dataset ID in BigQuery.
        table_id (str): The table ID in BigQuery.
        raw_data (List[Dict]): The raw data list containing dictionaries.

    Returns:
        List[Dict]: A list of dictionaries containing only the new rows not present in the clean table.
    """

    # Query the existing timestamps from the clean table
    query = f"""
    SELECT time_period_start
    FROM `{bigquery_client.project}.{dataset_id}.{table_id}`
    """

    query_job = bigquery_client.query(query)
    results = query_job.result()

    # Create a set of existing timestamps for efficient lookup
    existing_data_modified = {row["time_period_start"].strftime("%Y-%m-%d %H:%M:%S UTC") for row in results}

    new_data = []
    for row in raw_data:
        data_modified = row.get("time_period_start")
        if data_modified:
            try:
                # Parse the timestamp while handling microseconds
                formatted_date = datetime.strptime(data_modified, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S UTC")
            except ValueError as e:
                logging.info(f"Failed to parse {data_modified}: {e}")
                continue  # Skip to the next iteration if parsing fails

            if formatted_date not in existing_data_modified:
                logging.info("Adding new data row")
                new_data.append(row)
            else:
                logging.info("Data row already exists, skipping.")

    # Add logic to handle the case when new_data is empty
    if not new_data:
        logging.info("No new data to insert after deduplication.")
        return []

    return new_data



def filter_duplicates_fear_greed(
    bigquery_client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    raw_data: List[Dict],
) -> List[Dict]:
    raw_data = convert_unix_timestamp_in_data(raw_data)

    # nosec
    query = f""" 
    SELECT timestamp
    FROM `{bigquery_client.project}.{dataset_id}.{table_id}`
    """  # nosec

    query_job = bigquery_client.query(query)
    results = query_job.result()

    existing_data_modified = {
        row["timestamp"] for row in results
    }  # A set of existing timestamps

    existing_data_modified = convert_datetime_to_string(existing_data_modified)
    new_data = [
        row for row in raw_data if row.get("timestamp") not in existing_data_modified
    ]

    return new_data
