import logging
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
import pendulum


def bigquery_raw_data_table(client, dataset_id, table_id, api_data):

    desired_schema = [
        SchemaField("data_modified", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("metadata_time", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("raw_data", "STRING", mode="NULLABLE")
    ]

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        # Check if the table already exists
        table = client.get_table(table_ref)
        logging.info(f"Table {table.project}.{table.dataset_id}.{table.table_id} exists.")

        # Check if the table has a schema
        if not table.schema:
            logging.info("Table exists but has no schema. Updating the table schema.")
            table.schema = desired_schema
            client.update_table(table, ["schema"])
            logging.info(f"Schema updated for table {table.project}.{table.dataset_id}.{table.table_id}")
        else:
            logging.info("Table already has a schema. No need to create or update the table.")

    except NotFound:
        logging.info(f"Table does not exist. Creating table: {dataset_id}.{table_id}")
        table = bigquery.Table(table_ref, schema=desired_schema)
        table = client.create_table(table)
        logging.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        time.sleep(5)

    # Get the current timestamp for when the API call was made
    metadata_time = pendulum.now("Europe/Stockholm").to_iso8601_string()

    rows_to_insert = []

    # Flatten the nested list structure of api_data
    if isinstance(api_data, list) and len(api_data):
        #this is for coinapi response
        try:
            api_data = api_data
        except Exception as e:
            logging.error(f"An error occurred while processing API data: {e}")
    elif api_data:
        #this is for fear and gread response
        api_data = [item for item in api_data['data'] if isinstance(item, dict)]
    else:
        logging.warning("API data is empty.")

    for data_point in api_data:
        if 'time_period_start' in data_point:
              #this is for coinapi response
            if isinstance(data_point, dict):
                data_modified = data_point["time_period_start"]
                raw_data = json.dumps(data_point)
                row = {
                    "data_modified": data_modified,
                    "metadata_time": metadata_time,
                    "raw_data": raw_data
                }
                rows_to_insert.append(row)
            else:
                logging.info("Encountered a non-dictionary item in the list. Skipping.")

        elif 'timestamp' in data_point:
            #this is for fear and gread response
            if isinstance(data_point, dict):

                data_modified = data_point["timestamp"]
                raw_data = json.dumps(data_point)
                row = {
                    "data_modified": data_modified,
                    "metadata_time": metadata_time,
                    "raw_data": raw_data
                }
                rows_to_insert.append(row)
            else:
                logging.info("Encountered a non-dictionary item in the list. Skipping.")
        else:
            logging.warning(f"Warning: Neither 'timestamp' nor 'time_period_start' found in data point: {data_point}. Skipping this item.")

    if rows_to_insert:
        return rows_to_insert
    else:
        logging.info("No valid data to insert.")