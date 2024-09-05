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
from starlette.requests import Request

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("shabubsinc", "trigger-consume")


@app.post("/ingest/ohlc/clean")
async def ingest_ohlc_clean(request: Request):
    try:
        request_body = await request.json()
        message_data = json.loads(request_body["message"]["data"])

        source = message_data.get("source")
        table = message_data.get("table")

        if source == "ohlc_raw":
            logging.info(f"Processing clean data for {source}, table: {table}")

            ohlc_raw_data = get_raw_data_from_bigquery(
                bigquery_client=bigquery_client,
                dataset_id="shabubsinc_db",
                table_id=table,
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
            stream_data_to_bigquery(
                bigquery_client=bigquery_client,
                data=clean_data,
                project_id="shabubsinc",
                dataset_id="shabubsinc_db",
                table_id="clean_hourly_ohlc_data",
            )
            logging.info("OHLC cleaned and streamed successfully.")

            message = json.dumps(
                {
                    "status": "completed",
                    "source": "ohlc_clean",
                    "table": "clean_hourly_ohlc_data",
                }
            ).encode("utf-8")

            future = publisher.publish(topic_path, message)
            await future.result()
            logging.info("Published message to trigger consumption process.")

            return {"status": "success"}
        else:
            logging.warning(f"Message source {source} is not relevant, ignoring.")
            return {"status": "ignored"}

    except Exception as e:
        logging.error(f"Failed to process OHLC data: {e}")
        raise HTTPException(status_code=500, detail="Data ingestion failed")


if __name__ == "__main__":
    ingest_ohlc_clean()
