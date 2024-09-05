import json
from shared_functions import (
    stream_data_to_bigquery,
    bigquery_client,
    get_raw_data_from_bigquery,
    filter_duplicates_ohlc,
)
from ohlc.processing.process_ohlc_data import ensure_bigquery_ohlc_table
from google.cloud import pubsub_v1
import logging
from fastapi import FastAPI, HTTPException


app = FastAPI()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("shabubsinc","trigger-consume")

@app.post("/ingest/ohlc/clean")
def ingest_ohlc_clean():

    ohlc_raw_data = get_raw_data_from_bigquery(
        bigquery_client=bigquery_client,
        dataset_id="shabubsinc_db",
        table_id="raw_hourly_ohlc_data",
    )

    ensure_bigquery_ohlc_table(
        bigquery_client=bigquery_client,
        dataset_id="shabubsinc_db",
        table_id="clean_hourly_ohlc_data",
    )
    clean_data = filter_duplicates_ohlc(
        bigquery_client=bigquery_client,
        dataset_id="shabubsinc_db",
        table_id="clean_hourly_ohlc_data",
        raw_data=ohlc_raw_data,
    )
    try:
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=clean_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="clean_hourly_ohlc_data",
        )

        message = json.dumps({
            "status": "completed",
            "source": "ohlc_clean",
            "table": "clean_hourly_ohlc_data"
        }).encode("utf-8")
        
        publisher.publish(topic_path, message)
        logging.info("Published message to trigger consumption process.")


        return {"status": "success"}

    except Exception as e:
        logging.error(f"Failed to stream hourly ohlc data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data ingestion failed")


if __name__ == "__main__":
    ingest_ohlc_clean()
