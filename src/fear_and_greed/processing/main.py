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
from fastapi import FastAPI, HTTPException
import logging

app = FastAPI()


@app.post("/ingest/fear-greed/clean")
def ingest_fear_greed_clean():

    fear_greed_raw_data = get_raw_data_from_bigquery(
        bigquery_client=bigquery_client,
        dataset_id="shabubsinc_db",
        table_id="raw_daily_fear_greed_data",
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

    try:
        stream_data_to_bigquery(
            bigquery_client=bigquery_client,
            data=clean_data,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="clean_daily_fear_greed_data",
        )
        return {"status": "success"}

    except Exception as e:
        logging.error(f"Failed to stream fear and greed data to BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Data processing failed")


if __name__ == "__main__":
    ingest_fear_greed_clean()
