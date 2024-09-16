import json
from typing import List, Dict
from google.cloud import bigquery
import logging


def get_raw_data_from_bigquery(
    bigquery_client: bigquery.Client, dataset_id: str, table_id: str
) -> List[Dict]:
    """
    Retrieves the latest data from a specified table in BigQuery as a list of dictionaries.

    This function ensures that for each `data_modified` timestamp, only the latest row
    based on `metadata_time` is retrieved.

    Args:
        bigquery_client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to retrieve data from.

    Returns:
        List[Dict]: A list of dictionaries where each dictionary represents a row of data,
                    parsed from the 'raw_data' JSON string in the table.
    """

    # Define the query to select the latest row for each `data_modified` based on `metadata_time`
    query = f"""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY data_modified ORDER BY metadata_time DESC) as row_num
        FROM `{bigquery_client.project}.{dataset_id}.{table_id}`
    )
    WHERE row_num = 1
    """  # SQL query to get the latest row for each `data_modified` based on `metadata_time`

    try:
        # Execute the query
        query_job = bigquery_client.query(query)
        results = query_job.result()

        raw_data_list = []

        # Process the query results
        for row in results:
            try:
                # Parse the 'raw_data' JSON string into a dictionary
                raw_data_dict = json.loads(row["raw_data"])
                raw_data_list.append(raw_data_dict)
            except (TypeError, json.JSONDecodeError) as e:
                logging.error(f"Error parsing row: {e}, skipping row: {row}")

        return raw_data_list

    except Exception as e:
        logging.error(f"BigQuery error: {e}")
        return []  # Return an empty list in case of error
