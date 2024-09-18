from shared_functions import (
    stream_data_to_bigquery,
    bigquery_client,
    get_raw_data_from_bigquery,
    filter_duplicates_fear_greed,
)
from process_fear_greed_data import (
    ensure_bigquery_fear_greed_table,
    extract_required_fields,
)
from fastapi import FastAPI, HTTPException
import logging

from shared_functions.logger_setup import setup_logger

# Set up the logger from shared_functions
logger = setup_logger()

app = FastAPI()


@app.post("/")
def ingest_fear_greed_clean():
    try:
        # Fetch raw data
        logger.info("Fetching raw data from BigQuery.")
        fear_greed_raw_data = get_raw_data_from_bigquery(
            bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id="raw_daily_fear_greed_data",
        )

        # Ensure clean table exists
        logger.info("Ensuring clean table exists.")
        ensure_bigquery_fear_greed_table(
            bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id="clean_daily_fear_greed_data",
        )

        # Process raw data and remove duplicates
        logger.info("Processing raw data and removing duplicates.")
        structured_data = extract_required_fields(fear_greed_raw_data)
        clean_data = filter_duplicates_fear_greed(
            bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id="clean_daily_fear_greed_data",
            raw_data=structured_data,
        )

        # If clean_data is empty, skip insertion
        if not clean_data:
            logger.info("No new data to insert after deduplication.")
            return {"status": "success", "message": "No new data to process"}

        # Insert clean data into BigQuery
        logger.info("Inserting clean data into BigQuery.")
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=clean_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="clean_daily_fear_greed_data",
        )
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Failed to stream fear and greed data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data processing failed")
