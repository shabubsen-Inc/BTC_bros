from google.cloud import bigquery
from google.api_core.exceptions import NotFound

import time

def ensure_bigquery_fear_greed_table(client, dataset_id, table_id="raw_fear_and_greed"):
    # Defining schema to use for Fear and Greed Index data
    desired_schema = [
        bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("value_classification", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        # Check if the table already exists
        table = client.get_table(table_ref)
        print(f"Table {table.project}.{table.dataset_id}.{table.table_id} exists.")
        
        # Check if the table has a schema
        if not table.schema:
            print("Table exists but has no schema. Updating the table schema.")
            table.schema = desired_schema
            client.update_table(table, ["schema"])
            print(f"Schema updated for table {table.project}.{table.dataset_id}.{table.table_id}")
        else:
            print("Table already has a schema. No need to create or update the table.")
    
    except NotFound:
        print(f"Table does not exist. Creating table: {dataset_id}.{table_id}")
        table = bigquery.Table(table_ref, schema=desired_schema)
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        time.sleep(10)