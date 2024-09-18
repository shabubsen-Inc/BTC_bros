from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO)


def get_consume_data(
    bigquery_client: bigquery.Client, dataset_id: str, view_id: str, training: bool
) -> pd.DataFrame:
    """
    Retrieves data from a BigQuery view and returns it as a Pandas DataFrame.

    Args:
        bigquery_client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the view.
        view_id (str): The ID of the view to retrieve data from.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the retrieved data.
    """
    # nosec
    if training:
        query = f"""
        SELECT *
        FROM `{bigquery_client.project}.{dataset_id}.{view_id}`
        ORDER BY time_period_start DESC LIMIT 1;
        """  # nosec
    elif not training:
        query = f"""
        SELECT *
        FROM `{bigquery_client.project}.{dataset_id}.{view_id}`
        WHERE time_period_start >= '2018-02-02'
        ORDER BY time_period_start
        """  # nosec

    try:
        logging.info(f"Running query on view: {view_id} in dataset: {dataset_id}")

        query_job = bigquery_client.query(query)

        results = query_job.result()

        rows = [dict(row) for row in results]
        if not rows:
            logging.warning(f"No data found in view {view_id}")
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = df.sort_values(by="time_period_start", ascending=False)

        logging.info(f"Retrieved {len(df)} rows from view {view_id}")
        return df

    except GoogleCloudError as e:
        logging.error(f"An error occurred while querying the view {view_id}: {e}")
        raise

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise
