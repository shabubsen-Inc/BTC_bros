from shared_functions import (
    stream_data_to_bigquery,
    bigquery_client,
    get_raw_data_from_bigquery,
    filter_duplicates_fear_greed,
)
from fear_and_greed.processing.process_fear_greed_data import (
    ensure_bigquery_fear_greed_table,
    extract_required_fields,
)
import json
from google.cloud import pubsub_v1
from fastapi import FastAPI, HTTPException
import logging
from starlette.requests import Request

app = FastAPI()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("shabubsinc", "trigger-consume")


@app.post("/ingest/fear-greed/clean")
async def ingest_fear_greed_clean(request: Request):
    try:
        request_body = await request.json()
        message_data = json.loads(request_body["message"]["data"])

        source = message_data.get("source")
        table = message_data.get("table")

        if source == "fear_greed_raw":
            logging.info(f"Processing clean data for {source}, table: {table}")

            fear_greed_raw_data = get_raw_data_from_bigquery(
                bigquery_client=bigquery_client,
                dataset_id="shabubsinc_db",
                table_id=table,
            )
            ensure_bigquery_fear_greed_table(
                bigquery_client=bigquery_client,
                dataset_id="shabubsinc_db",
                table_id="clean_daily_fear_greed_data",
            )
            structured_data = extract_required_fields(fear_greed_raw_data)

            clean_data = filter_duplicates_fear_greed(
                bigquery_client=bigquery_client,
                dataset_id="shabubsinc_db",
                table_id="clean_daily_fear_greed_data",
                raw_data=structured_data,
            )

            stream_data_to_bigquery(
                bigquery_client=bigquery_client,
                data=clean_data,
                project_id="shabubsinc",
                dataset_id="shabubsinc_db",
                table_id="clean_daily_fear_greed_data",
            )
            logging.info("Fear & Greed data cleaned and streamed successfully.")

            message = json.dumps(
                {
                    "status": "completed",
                    "source": "fear_greed_clean",
                    "table": "clean_daily_fear_greed_data",
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
        logging.error(f"Failed to process Fear & Greed data: {e}")
        raise HTTPException(status_code=500, detail="Data processing failed")


if __name__ == "__main__":
    ingest_fear_greed_clean()
