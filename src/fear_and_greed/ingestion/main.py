from fetch_fear_greed_data import fetch_fear_greed_data
from shared_functions import (
    bigquery_raw_data_table,
    stream_data_to_bigquery,
    bigquery_client,
)
from fastapi import FastAPI, HTTPException
import logging

# API URL for fetching Fear & Greed data
api_url = "https://api.alternative.me/fng/?limit=1"

# Initialize FastAPI app
app = FastAPI()

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.post("/")
def ingest_fear_greed_raw():
    try:
        # Fetch Fear & Greed data from the API
        fear_greed_data = fetch_fear_greed_data(api_url)

        # Prepare raw data for BigQuery insertion
        raw_fear_greed_data = bigquery_raw_data_table(
            bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id="raw_daily_fear_greed_data",
            api_data=fear_greed_data,
        )

        # Stream raw data to BigQuery
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=raw_fear_greed_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="raw_daily_fear_greed_data",
        )
        logger.info("Fear & Greed data streamed successfully to BigQuery.")

        return {"status": "success"}

    except Exception as e:
        logger.error(f"Failed to stream data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data ingestion failed")
