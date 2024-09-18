from sklearn.model_selection import train_test_split
import pandas as pd
import pendulum
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException
import uvicorn
import xgboost
import joblib
from get_consume_data import get_raw_data
import logging
from shared_functions import bigquery_client

logging.basicConfig(level=logging.INFO)

dataset_id = "shabubsinc_db"
view_id = "mview_consume"


def main():

    df = get_raw_data(
        bigquery_client=bigquery_client, dataset_id=dataset_id, view_id=view_id
    )

    print(df.head())


if __name__ == "__main__":
    main()
