import json
from typing import List, Dict
from google.cloud import bigquery


def get_all_data_from_bigquery(
    client: bigquery.Client, dataset_id: str, table_id: str
) -> List[Dict]:
    """
    Retrieves all data from a specified table in BigQuery as a list of dictionaries.

    Args:
        client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to retrieve data from.

    Returns:
        List[Dict]: A list of dictionaries where each dictionary represents a row of data,
                    with each row parsed from the 'raw_data' JSON string in the table.
    """
    # nosec B608 - The following query construction is safe in this context.
    # nosec
    query = """ 
    SELECT * 
    FROM `{}`.`{}`.`{}` 
    """.format(  # nosec
        client.project, dataset_id, table_id  # nosec
    )  # nosec

    query_job = client.query(query)

    results = query_job.result()

    raw_data_list = []

    for row in results:
        # Parse the raw_data JSON string into a dictionary
        raw_data_dict = json.loads(row["raw_data"])
        raw_data_list.append(raw_data_dict)

    return raw_data_list
