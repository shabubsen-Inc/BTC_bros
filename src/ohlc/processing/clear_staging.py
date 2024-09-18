from google.cloud import bigquery
import logging


def drop_staging_table(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    staging_table: str,
):
    """
    Drops the staging table after processing is complete.

    Args:
        bigquery_client (bigquery.Client): A BigQuery client instance.
        project_id (str): The ID of the GCP project.
        dataset_id (str): The ID of the dataset containing the staging table.
        staging_table (str): The ID of the staging table to be dropped.
    """
    table_ref = f"{project_id}.{dataset_id}.{staging_table}"

    try:
        bigquery_client.delete_table(table_ref)  # Drop the table
        logging.info(f"Staging table {staging_table} deleted successfully.")
    except Exception as e:
        logging.error(f"Failed to delete staging table {staging_table}: {e}")
        raise
