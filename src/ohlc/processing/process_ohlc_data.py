from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound
import logging


def ensure_bigquery_ohlc_table(bigquery_client: bigquery.Client, dataset_id:str, table_id:str):

    desired_schema = [
        SchemaField("time_period_start", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("time_period_end", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("time_open", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("time_close", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("price_open", "FLOAT", mode="NULLABLE"),
        SchemaField("price_high", "FLOAT", mode="NULLABLE"),
        SchemaField("price_low", "FLOAT", mode="NULLABLE"),
        SchemaField("price_close", "FLOAT", mode="NULLABLE"),
        SchemaField("volume_traded", "FLOAT", mode="NULLABLE"),
        SchemaField("trades_count", "INTEGER", mode="NULLABLE"),
    ]
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        print(1)
        # Check if the table already exists
        table = bigquery_client.get_table(table_ref)
        logging.info(
            f"Table {table.project}.{table.dataset_id}.{table.table_id} exists."
        )

        # Check if the table has a schema
        if not table.schema:
            print(2)
            logging.info("Table exists but has no schema. Updating the table schema.")
            table.schema = desired_schema
            bigquery_client.update_table(table, ["schema"])
            logging.info(
                f"Schema updated for table {table.project}.{table.dataset_id}.{table.table_id}"
            )
        else:
            print(3)
            logging.info(
                "Table already has a schema. No need to create or update the table."
            )

    except NotFound:

        try:
            logging.info(
                f"Table does not exist. Creating table: {dataset_id}.{table_id}"
            )
            table = bigquery.Table(table_ref, schema=desired_schema)
            table = bigquery_client.create_table(table)
            logging.info(
                f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
            )
            print('created a new table')
            

        except NotFound as e:
            print(5)
            logging.error(
                f"Failed to create table because the dataset or table was not found: {e}"
            )
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
