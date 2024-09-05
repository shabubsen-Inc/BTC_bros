from google.cloud import pubsub_v1
from ingestion.fetch_ohlc_hourly_data import (
    fetch_ohlc_data_from_api,
    access_secret_version,
)
from shared_functions import (
    bigquery_client,
    bigquery_raw_data_table,
    stream_data_to_bigquery,
)
import logging
from fastapi import FastAPI, HTTPException

max_calls_per_minute = 30
call_interval = 60 / max_calls_per_minute

api_key = access_secret_version("shabubsinc", "coinapi-key")
headers = {"X-CoinAPI-Key": api_key}

app = FastAPI()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("shabubsinc", "trigger-clean-ohlc")


@app.post("/ingest/ohlc/raw")
def ingest_ohlc_raw():
    ohlc_data = fetch_ohlc_data_from_api(headers)

    raw_ohlc_data = bigquery_raw_data_table(
        bigquery_client=bigquery_client,
        dataset_id="shabubsinc_db",
        table_id="raw_hourly_ohlc_data",
        api_data=ohlc_data,
    )

    try:
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=raw_ohlc_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="raw_hourly_ohlc_data",
        )

        publisher.publish(topic_path, b"Trigger clean processing for ohlc")

        return {"status": "success"}

    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data ingestion failed")


if __name__ == "__main__":
    ingest_ohlc_raw()
