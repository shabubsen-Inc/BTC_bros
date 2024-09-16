from google.cloud import bigquery


def get_data_in_batches(
    bigquery_client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    batch_size: int = 10000,
):
    """
    Retrieves data in batches from a specified table in BigQuery, with support for JSON extraction.
    This function excludes `data_modified` but includes `metadata_time`.

    Args:
        bigquery_client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to retrieve data from.
        batch_size (int): The number of rows per batch.

    Yields:
        List[Dict]: A list of dictionaries where each dictionary represents a row of data.
    """

    query = f"""
    SELECT
        JSON_EXTRACT_SCALAR(raw_data, '$.time_period_start') AS time_period_start,
        JSON_EXTRACT_SCALAR(raw_data, '$.time_period_end') AS time_period_end,
        JSON_EXTRACT_SCALAR(raw_data, '$.time_open') AS time_open,
        JSON_EXTRACT_SCALAR(raw_data, '$.time_close') AS time_close,
        JSON_EXTRACT_SCALAR(raw_data, '$.price_open') AS price_open,
        JSON_EXTRACT_SCALAR(raw_data, '$.price_high') AS price_high,
        JSON_EXTRACT_SCALAR(raw_data, '$.price_low') AS price_low,
        JSON_EXTRACT_SCALAR(raw_data, '$.price_close') AS price_close,
        JSON_EXTRACT_SCALAR(raw_data, '$.volume_traded') AS volume_traded,
        JSON_EXTRACT_SCALAR(raw_data, '$.trades_count') AS trades_count,
        metadata_time   -- Only include metadata_time for partitioning
    FROM `{bigquery_client.project}.{dataset_id}.{table_id}`
    """

    query_job = bigquery_client.query(query)
    rows = query_job.result(page_size=batch_size)

    for page in rows.pages:
        batch_data = []
        for row in page:
            # Build a dictionary for each row, converting the necessary fields
            row_dict = {
                "time_period_start": row["time_period_start"],
                "time_period_end": row["time_period_end"],
                "time_open": row["time_open"],
                "time_close": row["time_close"],
                "price_open": float(row["price_open"]) if row["price_open"] else None,
                "price_high": float(row["price_high"]) if row["price_high"] else None,
                "price_low": float(row["price_low"]) if row["price_low"] else None,
                "price_close": (
                    float(row["price_close"]) if row["price_close"] else None
                ),
                "volume_traded": (
                    float(row["volume_traded"]) if row["volume_traded"] else None
                ),
                "trades_count": (
                    int(row["trades_count"]) if row["trades_count"] else None
                ),
                "metadata_time": row[
                    "metadata_time"
                ],  # Include metadata_time for partitioning
            }
            batch_data.append(row_dict)

        yield batch_data  # Yield each batch
