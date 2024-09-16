from google.cloud import bigquery
import json
from typing import List, Dict
from datetime import datetime
import logging


def get_raw_data(
    bigquery_client: bigquery.Client, dataset_id: str, table_id: str
) -> List[Dict]:
    """
    Retrieves and parses the raw JSON data from a BigQuery table.

    Args:
        bigquery_client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to retrieve data from.

    Returns:
        List[Dict]: A list of dictionaries where each dictionary represents a row of data,
                    parsed from the 'raw_data' JSON string.
    """

    query = f"""
    SELECT data_modified, metadata_time, raw_data
    FROM `{bigquery_client.project}.{dataset_id}.{table_id}`
    """

    query_job = bigquery_client.query(query)
    results = query_job.result()

    raw_data_list = []

    for row in results:
        # Convert BigQuery Row object to dictionary
        row_dict = dict(row)

        # Parse the 'raw_data' JSON string into a dictionary
        if "raw_data" in row_dict and row_dict["raw_data"]:
            try:
                row_dict["raw_data"] = json.loads(row_dict["raw_data"])
            except json.JSONDecodeError as e:
                logging.error(
                    f"Error decoding JSON in 'raw_data' for row {row_dict}: {e}"
                )

        # Convert datetime fields to ISO format (optional, for serialization)
        for key, value in row_dict.items():
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()

        raw_data_list.append(row_dict)

    return raw_data_list
