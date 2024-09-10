from google.cloud import bigquery
import logging
from google.cloud.exceptions import NotFound, GoogleCloudError


def create_or_refresh_materialized_view(
    bigquery_client: bigquery.Client, project_id: str, dataset_id: str, view_id: str
):
    view_query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{project_id}.{dataset_id}.{view_id}` AS
    SELECT
        ohlc.time_period_start,
        ohlc.time_period_end,
        ohlc.time_open,
        ohlc.time_close,
        ohlc.price_open,
        ohlc.price_high,
        ohlc.price_low,
        ohlc.price_close,
        ohlc.volume_traded,
        ohlc.trades_count,
        COALESCE(CAST(fng.value AS INT64), 0) AS fear_greed_value,
        COALESCE(fng.value_classification, 'Unknown') AS fear_greed_classification
    FROM `{project_id}.{dataset_id}.clean_hourly_ohlc_data` AS ohlc
    LEFT JOIN `{project_id}.{dataset_id}.clean_daily_fear_greed_data` AS fng
    ON DATE(ohlc.time_period_start) = DATE(fng.timestamp)
    """

    try:
        query_job = bigquery_client.query(view_query)
        query_job.result() 
        logging.info(f"Materialized view {project_id}.{dataset_id}.{view_id} created or refreshed successfully.")

    except GoogleCloudError as e:
        logging.error(
            f"Error creating or refreshing the materialized view: {e}")
        raise

if __name__ == "__main__":
    create_or_refresh_materialized_view()
