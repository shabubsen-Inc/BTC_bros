from google.cloud import bigquery
from typing import List
import logging
from google.cloud.exceptions import NotFound
from datetime import datetime
from typing import Dict


def check_and_create_partitioned_table_if_not_exists(
    bigquery_client: bigquery.Client, dataset_id: str, table_id: str
):
    # Define the schema with metadata_time
    schema = [
        bigquery.SchemaField("time_period_start", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("time_period_end", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("time_open", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("time_close", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("price_open", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("price_high", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("price_low", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("price_close", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("volume_traded", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("trades_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("metadata_time", "TIMESTAMP", mode="REQUIRED"),
    ]

    # Create the table reference
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    try:
        # Check if the table already exists
        bigquery_client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        # If the table does not exist, create it with partitioning on metadata_time
        logging.info(f"Table {table_id} not found. Creating new partitioned table.")
        table = bigquery.Table(table_ref, schema=schema)

        # Specify partitioning by metadata_time
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="metadata_time",  # Partitioning based on the metadata_time field
        )

        # Create the table
        bigquery_client.create_table(table)
        logging.info(f"Partitioned table {table_id} created successfully.")


def stream_data_to_staging(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    staging_table: str,
    data: List[Dict],
):
    """
    Inserts data into the staging table in BigQuery. Converts datetime fields to ISO format for JSON serialization.
    """

    # Convert datetime objects to strings in ISO format
    for i, row in enumerate(data):
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()  # Convert datetime to ISO format
                logging.info(f"Converted {key} to ISO format for row {i}.")

    table_ref = f"{project_id}.{dataset_id}.{staging_table}"

    # Insert rows into BigQuery (without data_modified, as it's not in staging)
    errors = bigquery_client.insert_rows_json(table_ref, data)

    if errors:
        logging.error(f"Errors occurred while inserting data into staging: {errors}")
        raise Exception("Errors occurred while inserting data into staging.")
    else:
        logging.info(f"Successfully inserted data into {staging_table}")
