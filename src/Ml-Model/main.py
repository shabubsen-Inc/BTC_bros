import joblib
from get_consume_data import get_consume_data
from tranfomer import tranfomer
import logging
from shared_functions import bigquery_client
from create_time_feature import create_time_features
from stream_to_pred_table import stream_df_to_bigquery
from process_pred_data import ensure_pred_table
from prepare_df_for_bigquery import prepare_df_for_bigquery

logging.basicConfig(level=logging.INFO)

dataset_id = "shabubsinc_db"
view_id = "mview_consume"


def main():

    df = get_consume_data(
        bigquery_client=bigquery_client, dataset_id=dataset_id, view_id=view_id, training=False
    )
    X = create_time_features(df)

    X  = tranfomer(X)

    model = joblib.load("random_forest_model.pkl")
    predicted_price = model.predict(X)
    df["predicted_price"] = predicted_price

    df = prepare_df_for_bigquery(df)

    ensure_pred_table(bigquery_client=bigquery_client,
            dataset_id="shabubsinc_db",
            table_id='pred_table')

    stream_df_to_bigquery(bigquery_client=bigquery_client,
                df=df,
                project_id="shabubsinc",
                dataset_id="shabubsinc_db",
                table_id='pred_table')
   
if __name__ == "__main__":
    main()