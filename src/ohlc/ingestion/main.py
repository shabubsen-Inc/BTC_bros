from fetch_ohlc_hourly_data import (
    fetch_ohlc_data_from_api,
    access_secret_version,
)
from shared_functions import (
    bigquery_client,
    bigquery_raw_data_table,
    stream_data_to_bigquery,
)
from shared_functions.logger_setup import (
    setup_logger,
)  # Importing the custom logger setup
from google.cloud.workflows.executions_v1 import ExecutionsClient
from fastapi import FastAPI, HTTPException


# Set up the logger from shared_functions
logger = setup_logger()


# Get API key for the headers
api_key = access_secret_version("shabubsinc", "coinapi-key")
headers = {"X-CoinAPI-Key": api_key}

# Initialize FastAPI app
app = FastAPI()

# Initialize the Workflows Execution client
executions_client = ExecutionsClient()


@app.post("/")
def ingest_ohlc_raw():
    try:
        logger.info("Starting data ingestion process...")

        # Fetch OHLC data from the API
        ohlc_data = fetch_ohlc_data_from_api(headers=headers)
        if not ohlc_data:
            logger.error("OHLC data is None or empty.")
            raise HTTPException(status_code=500, detail="Failed to fetch OHLC data")
        logger.info(f"OHLC data fetched: {ohlc_data}")

        # Prepare raw OHLC data for BigQuery
        raw_ohlc_data = bigquery_raw_data_table(
            bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id="raw_hourly_ohlc_data",
            api_data=ohlc_data,
        )
        if not raw_ohlc_data:
            logger.error("Raw OHLC data preparation failed.")
            raise HTTPException(
                status_code=500, detail="Failed to prepare raw OHLC data"
            )
        logger.info("Prepared raw OHLC data for BigQuery")

        # Stream data to BigQuery
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=raw_ohlc_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="raw_hourly_ohlc_data",
        )
        logger.info("Data streamed to BigQuery successfully")

        return {"status": "success"}

    except Exception as e:
        logger.exception(f"Failed to process OHLC data: {e}")
        raise HTTPException(
            status_code=500, detail=f"Data ingestion and processing failed: {str(e)}"
        )


# nosec
# Ensure FastAPI runs with Uvicorn in Cloud Run
if __name__ == "__main__":
    ingest_ohlc_raw()
    # Use uvicorn to serve the FastAPI app
    # uvicorn.run(app, host="0.0.0.0", port=8080)  # nosec
