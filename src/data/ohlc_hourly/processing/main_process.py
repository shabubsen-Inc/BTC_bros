from data.storage.fetch_data_from_db import get_all_data_from_bigquery
from data.storage.store_in_db import stream_data_to_bigquery, client
from data.ohlc_hourly.processing.process_ohlc_data import ensure_bigquery_ohlc_table
import logging

def ohlc(): 

    ohlc_raw_data = get_all_data_from_bigquery(client=client,dataset_id='shabubsinc_db',table_id='raw_hourly_data')

    ensure_bigquery_ohlc_table(client=client, dataset_id='shabubsinc_db',table_id='processed_hourly_data')

    try:
        stream_data_to_bigquery(client=client, data=ohlc_raw_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='processed_hourly_data')
    except Exception as e:
        logging.error(f"Failed to stream hourly ohlc data to BigQuery: {e}")

if __name__ == "__main__":
    ohlc()
