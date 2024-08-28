from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound
import pendulum
import json
import time

client = bigquery.Client(project='shabubsinc')


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
        print(f"Table {table.project}.{
              table.dataset_id}.{table.table_id} exists.")

        # Check if the table has a schema
        if not table.schema:
            print("Table exists but has no schema. Updating the table schema.")
            table.schema = desired_schema
            client.update_table(table, ["schema"])
            print(f"Schema updated for table {table.project}.{
                  table.dataset_id}.{table.table_id}")
        else:
            print("Table already has a schema. No need to create or update the table.")

    except NotFound:
        print(f"Table does not exist. Creating table: {dataset_id}.{table_id}")
        table = bigquery.Table(table_ref, schema=desired_schema)
        table = client.create_table(table)
        print(f"Created table {table.project}.{
              table.dataset_id}.{table.table_id}")
        time.sleep(5)

    # Get the current timestamp for when the API call was made
    metadata_time = pendulum.now("Europe/Stockholm").to_iso8601_string()

    rows_to_insert = []

    # Flatten the nested list structure of api_data
    if isinstance(api_data, list) and len(api_data) > 0 and isinstance(api_data[0], list):
        api_data = [item for sublist in api_data for item in sublist]

    # Iterate over each item in the flattened api_data list
    for data_point in api_data:
        try:
            if isinstance(data_point, dict):
                data_modified = data_point.get("time_period_start", None)
                raw_data = json.dumps(data_point)
                row = {
                    "data_modified": data_modified,
                    "metadata_time": metadata_time,
                    "raw_data": raw_data
                }
                rows_to_insert.append(row)
            else:
                print("Encountered a non-dictionary item in the list. Skipping.")
        except Exception as e:
            print(f"Error processing data point: {e}")
            if isinstance(data_point, dict):
                data_modified = data_point.get("timestamp", None)
                raw_data = json.dumps(data_point)
                row = {
                    "data_modified": data_modified,
                    "metadata_time": metadata_time,
                    "raw_data": raw_data
                }
                rows_to_insert.append(row)
            else:
                print("Encountered a non-dictionary item in the list. Skipping.")

    if rows_to_insert:
        return rows_to_insert
    else:
        print("No valid data to insert.")


def stream_data_to_bigquery(client, data, project_id, dataset_id, table_id):

    table = f"{project_id}.{dataset_id}.{table_id}"

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
        data = [item for sublist in data for item in sublist]
     # Ensure data is a list of dictionaries
    if not all(isinstance(item, dict) for item in data):
        print("Error: All items in the data must be dictionaries.")
        return

    errors = client.insert_rows_json(table=table, json_rows=data)
    if errors:
        print("Encountered errors while inserting rows:")
        for error in errors:
            print(error)
    else:
        print("Data successfully streamed to BigQuery.")
