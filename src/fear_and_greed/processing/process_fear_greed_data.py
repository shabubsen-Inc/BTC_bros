from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleCloudError
import time
import logging


def ensure_bigquery_fear_greed_table(client, dataset_id, table_id):
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
        logging.info(
            f"Table {table.project}.{table.dataset_id}.{table.table_id} exists."
        )

        # Check if the table has a schema
        if not table.schema:
            logging.info("Table exists but has no schema. Updating the table schema.")
            table.schema = desired_schema
            client.update_table(table, ["schema"])
            logging.info(
                f"Schema updated for table {table.project}.{table.dataset_id}.{table.table_id}"
            )
        else:
            logging.info(
                "Table already has a schema. No need to create or update the table."
            )

    except NotFound:

        try:
            logging.info(
                f"Table does not exist. Creating table: {dataset_id}.{table_id}"
            )
            table = bigquery.Table(table_ref, schema=desired_schema)
            table = client.create_table(table)
            logging.info(
                f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
            )
            time.sleep(10)

        except NotFound as e:
            logging.error(
                f"Failed to create table because the dataset or table was not found: {e}"
            )
        except GoogleCloudError as e:
            logging.error(f"Google Cloud error occurred: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")


def extract_required_fields(data):
    """
    Extracts the required fields (value, value_classification, timestamp) from the row column.

    Args:
        data (list): A list of dictionaries where each dictionary represents a row of data.

    Returns:
        list: A list of dictionaries with only the required fields.
    """
    extracted_data = []
    for row in data:

        if isinstance(row, dict):
            # Extract the desired fields
            value = row.get("value")
            value_classification = row.get("value_classification")
            timestamp = row.get("timestamp")

            # Check if all required fields are present
            if (
                value is not None
                and value_classification is not None
                and timestamp is not None
            ):
                extracted_data.append(
                    {
                        "value": value,
                        "value_classification": value_classification,
                        "timestamp": timestamp,
                    }
                )
            else:
                logging.info(f"Skipping row due to missing required fields: {row}")
        else:
            logging.info(f"Skipping row because row is not a dictionary: {row}")

    if not extracted_data:
        logging.error("No valid data extracted.")

    return extracted_data
