from data.config import api_url
from data.ingestion.fetch_fear_greed_data import fetch_fear_greed_data
from data.processing.process_fear_greed_data import process_fear_greed_data
from data.ingestion.fetch_ohlc_hourly_data import fetch_ohlc_data_from_api, headers, dates 
from data.storage.store_in_db import ensure_bigquery_ohlc_table, stream_data_to_bigquery, ensure_bigquery_fear_greed_table, client
import logging


def main():
    # Step 1: Ingest data
    raw_data = fetch_fear_greed_data(api_url)

    # Step 2: Process data
    if raw_data:
        processed_data = process_fear_greed_data(raw_data)
    else:
        print("Failed to fetch data.")
    
    ensure_bigquery_fear_greed_table(client=client, dataset_id='shabubsinc_db', table_id='raw_fearNgread_data')

    try:
        stream_data_to_bigquery(client=client, data=processed_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='fear_and_gread')
    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")
    

    ohlc_data = fetch_ohlc_data_from_api(dates,headers)

    ensure_bigquery_ohlc_table(client=client, dataset_id='shabubsinc_db', table_id='raw_hourly_data')
    
    try:
        stream_data_to_bigquery(client=client, data=ohlc_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='raw_hourly_data')
    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")

if __name__ == "__main__":
    main()