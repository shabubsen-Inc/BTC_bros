import json

def get_all_data_from_bigquery(client, dataset_id, table_id):
    """
    Retrieves all data from a specified table in BigQuery as a list of dictionaries.
    
    Args:
        client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to retrieve data from.

    Returns:
        list: A list of dictionaries where each dictionary represents a row of data.
    """
    query = f"""
    SELECT *
    FROM `{client.project}.{dataset_id}.{table_id}`
    """
   
    query_job = client.query(query)

    results = query_job.result()

    raw_data_list = []

    for row in results:
        # Parse the raw_data JSON string into a dictionary
        raw_data_dict = json.loads(row["raw_data"])
        raw_data_list.append(raw_data_dict)
    
    return raw_data_list