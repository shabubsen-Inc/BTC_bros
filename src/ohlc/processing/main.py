from fastapi import FastAPI, HTTPException
from bigquery_utils import (
    check_and_create_partitioned_table_if_not_exists,
    stream_data_to_staging,
)
from shared_functions.logger_setup import setup_logger
from staging_utils import merge_staging_into_clean
from shared_functions import bigquery_client
import uvicorn
from batch_data import get_data_in_batches
from process_ohlc_data import ensure_bigquery_ohlc_table
from clear_staging import (
    drop_staging_table,
)  # Or delete_old_rows_from_staging based on your use case
# Set up the logger from shared_functions
logger = setup_logger()

app = FastAPI()


@app.post("/")
def process_ohlc_data():
    # BigQuery client and table details
    bq_client = bigquery_client
    project_id = "shabubsinc"
    dataset_id = "shabubsinc_db"
    staging_table = "staging_hourly_ohlc_data"
    clean_table = "new_clean_hourly_ohlc_data"
    raw_table = "raw_hourly_ohlc_data"

    logger.info("Starting OHLC data processing...")

    try:
        # Step 1: Ensure the staging table exists
        logger.info(f"Checking or creating staging table: {staging_table}.")
        check_and_create_partitioned_table_if_not_exists(
            bigquery_client=bq_client, dataset_id=dataset_id, table_id=staging_table
        )

        # Step 2: Ensure the clean table exists
        logger.info(f"Checking or creating clean table: {clean_table}.")
        ensure_bigquery_ohlc_table(
            bigquery_client=bq_client, dataset_id=dataset_id, table_id=clean_table
        )

        # Step 3: Process raw data in batches
        batch_size = 10000  # Adjust as needed for optimal performance
        for batch_data in get_data_in_batches(
            bq_client, dataset_id, raw_table, batch_size
        ):
            if not batch_data:
                logger.info(f"No data in batch from {raw_table}.")
                continue

            logger.info(
                f"Processing batch with {len(batch_data)} rows from {raw_table}."
            )

            # Step 4: Insert batch data into the staging table
            stream_data_to_staging(
                bq_client, project_id, dataset_id, staging_table, batch_data
            )
            logger.info(
                f"Inserted {len(batch_data)} rows into {staging_table}.")

            # Step 5: Merge data from staging into the clean table
            merge_staging_into_clean(
                bq_client, project_id, dataset_id, staging_table, clean_table
            )
            logger.info(
                f"Merged batch from {staging_table} into {clean_table}.")

        drop_staging_table(bq_client, project_id, dataset_id, staging_table)

        logger.info("Data processing and merging completed successfully.")
        return {
            "status": "success",
            "message": "Data processed and merged successfully",
        }

    except Exception as e:
        logger.error(f"Error during OHLC data processing: {e}")
        raise HTTPException(status_code=500, detail="Data processing failed")


if __name__ == "__main__":
    # nosec
    uvicorn.run(app, host="0.0.0.0", port=8080)  # nosec
