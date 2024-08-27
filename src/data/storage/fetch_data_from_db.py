import json
from google.cloud import bigquery

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

    data = []

    for row in results:
        row_dict = dict(row)
        
        if 'raw_data' in row_dict:
            json_data = json.loads(row_dict['raw_data'])
            row_dict.update(json_data)  # Merge the JSON data into the row dictionary
            del row_dict['raw_data']  # Remove the original JSON column
        
        data.append(row_dict)
    
    return data

client = bigquery.Client(project='shabubsinc')

data = get_all_data_from_bigquery(client=client,dataset_id='shabubsinc_db',table_id='raw_hourly_data')

print(data)