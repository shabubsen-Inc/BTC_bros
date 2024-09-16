from shared_functions import (
    stream_data_to_bigquery,
    bigquery_client,
    get_raw_data_from_bigquery,
    filter_duplicates_ohlc,
)
from process_ohlc_data import ensure_bigquery_ohlc_table
import logging
from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI()


@app.post("/")
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

    # Check if clean_data is empty after deduplication
    if not clean_data:
        logging.info("No new data to insert after deduplication.")
        return {"status": "success", "message": "No new data to process"}

    try:
        # Stream new data to BigQuery only if clean_data is not empty
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=clean_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="clean_hourly_ohlc_data",
        )
        return {"status": "success"}

    except Exception as e:
        logging.error(f"Failed to stream hourly ohlc data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data ingestion failed")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
