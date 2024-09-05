import json
from google.cloud import pubsub_v1
from fear_and_greed.ingestion.fetch_fear_greed_data import fetch_fear_greed_data
from shared_functions import (
    bigquery_raw_data_table,
    stream_data_to_bigquery,
    bigquery_client,
)
from fastapi import FastAPI, HTTPException
import logging

api_url = "https://api.alternative.me/fng/?limit=1"

app = FastAPI()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("shabubsinc", "trigger-clean-fear-greed")


@app.post("/ingest/fear-greed/raw")
def ingest_fear_greed_raw():
    fear_greed_data = fetch_fear_greed_data(api_url)

    raw_fear_greed_data = bigquery_raw_data_table(
        bigquery_client=bigquery_client,
        dataset_id="shabubsinc_db",
        table_id="raw_daily_fear_greed_data",
        api_data=fear_greed_data,
    )

    try:
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=raw_fear_greed_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="raw_daily_fear_greed_data",
        )

        message = json.dumps({
            "status": "completed",
            "source": "fear_greed_raw",
            "table": "raw_daily_fear_greed_data"
        }).encode("utf-8")

        publisher.publish(topic_path, message)
        publisher.result()

        logging.info("Published message to trigger fear and greed clean.")

        return {"status": "success"}

    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data ingestion failed")


if __name__ == "__main__":
    ingest_fear_greed_raw()
