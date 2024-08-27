from data.config import api_url
from data.ingestion.fetch_fear_greed_data import fetch_fear_greed_data
from data.processing.process_fear_greed_data import process_fear_greed_data
from data.ingestion.fetch_ohlc_hourly_data import fetch_ohlc_data_from_api,access_secret_version,API_call_limiter, dates 
from data.storage.store_in_db import bigquery_raw_data_table, stream_data_to_bigquery, client
import logging

max_calls_per_minute = 30
call_interval = 60/max_calls_per_minute

api_key = access_secret_version('shabubsinc', "coinapi-key")
headers = {'X-CoinAPI-Key': api_key}

def main():
   #  Step 1: Ingest data
    fear_greed_data = fetch_fear_greed_data(api_url)

    
    raw_fear_greed_data = bigquery_raw_data_table(client=client, dataset_id='shabubsinc_db', table_id='raw_fear_greed_data',api_data=fear_greed_data)

    try:
        stream_data_to_bigquery(client=client, data=raw_fear_greed_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='raw_fear_and_gread')
    
    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")
   
    call_count = 0
    all_call_counts = 0
    for date in dates:
        ohlc_data = fetch_ohlc_data_from_api(date,headers)
        all_call_counts =+1 
        print(all_call_counts)
        call_count = API_call_limiter(call_count, max_calls_per_minute, call_interval)

        raw_ohlc_data = bigquery_raw_data_table(client=client, dataset_id='shabubsinc_db', table_id='raw_hourly_data',api_data=ohlc_data)
    
        
        try:
            stream_data_to_bigquery(client=client, data=raw_ohlc_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='raw_hourly_data')
        except Exception as e:
            logging.error(f"Failed to stream data to BigQuery: {e}")

if __name__ == "__main__":
    main()