from google.cloud import bigquery
import logging


def merge_staging_into_clean(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    staging_table: str,
    clean_table: str,
):
    """
    Merges data from the staging table into the clean table, ensuring deduplication by keeping the latest metadata_time for each time_period_start.
    """

    merge_query = f"""
    MERGE `{project_id}.{dataset_id}.{clean_table}` T
    USING (
        SELECT time_period_start, time_period_end, time_open, time_close, 
               price_open, price_high, price_low, price_close, 
               volume_traded, trades_count, metadata_time
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY time_period_start ORDER BY metadata_time DESC) as row_num
            FROM `{project_id}.{dataset_id}.{staging_table}`
        )
        WHERE row_num = 1
    ) S
    ON T.time_period_start = S.time_period_start
    WHEN MATCHED THEN
      UPDATE SET 
        T.time_period_end = S.time_period_end,
        T.time_open = S.time_open,
        T.time_close = S.time_close,
        T.price_open = S.price_open,
        T.price_high = S.price_high,
        T.price_low = S.price_low,
        T.price_close = S.price_close,
        T.volume_traded = S.volume_traded,
        T.trades_count = S.trades_count,
        T.metadata_time = S.metadata_time
    WHEN NOT MATCHED THEN
      INSERT (
        time_period_start, time_period_end, time_open, time_close, 
        price_open, price_high, price_low, price_close, 
        volume_traded, trades_count, metadata_time
      )
      VALUES (
        S.time_period_start, S.time_period_end, S.time_open, S.time_close, 
        S.price_open, S.price_high, S.price_low, S.price_close, 
        S.volume_traded, S.trades_count, S.metadata_time
      );
    """

    query_job = bigquery_client.query(merge_query)
    query_job.result()  # Wait for the query to complete
    logging.info(f"Merged data from {staging_table} to {clean_table}.")
