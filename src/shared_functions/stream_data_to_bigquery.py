import logging
from typing import List, Dict, Union
from google.cloud import bigquery
from shared_functions.datetime_helper import convert_datetime_to_string


def stream_data_to_bigquery(
    client: bigquery.Client,
    data: Union[List[Dict], List[List[Dict]]],
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> None:
    """
    Streams data to a BigQuery table, handling nested lists and converting datetime objects.

    Args:
        client (bigquery.Client): An authenticated BigQuery client.
        data (Union[List[Dict], List[List[Dict]]]): The data to stream to BigQuery, either a list of dictionaries
                                                    or a list of lists of dictionaries.
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        None
    """
    table = f"{project_id}.{dataset_id}.{table_id}"

    # Flatten nested lists if present
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
        data = [item for sublist in data for item in sublist]

    # Ensure data is a list of dictionaries
    if not all(isinstance(item, dict) for item in data):
        logging.info("Error: All items in the data must be dictionaries.")
        return

    # Convert datetime objects to strings
    data = convert_datetime_to_string(data)

    # Stream data to BigQuery
    errors = client.insert_rows_json(table=table, json_rows=data)
    if errors:
        logging.info("Encountered errors while inserting rows:")
        for error in errors:
            logging.info(error)
    else:
        logging.info("Data successfully streamed to BigQuery.")
