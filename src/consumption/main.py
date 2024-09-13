from fastapi import FastAPI, HTTPException
from shared_functions import bigquery_client
import logging

from create_or_refresh_materialized_view import create_or_refresh_materialized_view

app = FastAPI()


@app.post("/")
def consume():
    try:

        logging.info(f"Triggered consumption to refresh materialized view.")

        create_or_refresh_materialized_view(
            bigquery_client, "shabubsinc", "shabubsinc_db", "mview_consume"
        )
        logging.info("Materialized view successfully created or refreshed.")

        return {"status": "success", "message": "Materialized view created/refreshed."}

    except Exception as e:
        logging.error(f"Error during consumption process: {e}")
        raise HTTPException(status_code=500, detail="Data consumption failed")


if __name__ == "__main__":
    consume()
