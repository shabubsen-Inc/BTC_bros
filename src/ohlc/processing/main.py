from shared_functions import stream_data_to_bigquery, bigquery_client, get_raw_data_from_bigquery
from ohlc.processing.process_ohlc_data import ensure_bigquery_ohlc_table
import logging

def ohlc(): 

    ohlc_raw_data = get_raw_data_from_bigquery(bigquery_client=bigquery_client,dataset_id='shabubsinc_db',table_id='raw_hourly_ohlc_data')

    ensure_bigquery_ohlc_table(bigquery_client=bigquery_client, dataset_id='shabubsinc_db',table_id='clean_hourly_ohlc_data')

    try:
        stream_data_to_bigquery(bigquery_client=bigquery_client, data=ohlc_raw_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='clean_hourly_ohlc_data')
    except Exception as e:
        logging.error(f"Failed to stream hourly ohlc data to BigQuery: {e}")

if __name__ == "__main__":
    ohlc()
