import joblib
from get_consume_data import get_consume_data
from shared_functions.logger_setup import setup_logger
from tranfomer import tranfomer
import logging
from shared_functions import bigquery_client
from create_time_feature import create_time_features
from stream_to_pred_table import stream_df_to_bigquery
from process_pred_data import ensure_pred_table
from prepare_df_for_bigquery import prepare_df_for_bigquery
from fastapi import FastAPI, HTTPException


# Set up the logger from shared_functions
logger = setup_logger()

app = FastAPI()

dataset_id = "shabubsinc_db"
view_id = "mview_consume"


@app.post("/")
def main():
    try:
        logger.info("Fetching consumption data.")
        df = get_consume_data(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            view_id=view_id,
            training=False,
        )

        logger.info("Creating time features.")
        X = create_time_features(df)

        logger.info("Transforming data.")
        X = tranfomer(X)

        logger.info("Loading the model.")
        model = joblib.load("random_forest_model.pkl")

        logger.info("Making predictions.")
        predicted_price = model.predict(X)
        df["predicted_price"] = predicted_price

        df = prepare_df_for_bigquery(df)

        logger.info("Ensuring the prediction table exists.")
        ensure_pred_table(
            bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id="pred_table",
        )

        logger.info("Streaming data to BigQuery.")
        stream_df_to_bigquery(
            bigquery_client=bigquery_client,
            df=df,
            project_id="shabubsinc",
            dataset_id="shabubsinc_db",
            table_id="pred_table",
        )

        return {"status": "success", "predictions": predicted_price.tolist()}

    except FileNotFoundError as fnf_error:
        logger.error(f"Model file not found: {fnf_error}")
        raise HTTPException(status_code=500, detail="Model file not found.")

    except ValueError as val_error:
        logger.error(f"Error during model prediction: {val_error}")
        raise HTTPException(status_code=500, detail="Prediction error.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise HTTPException(
            status_code=500, detail="An unexpected error occurred.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
