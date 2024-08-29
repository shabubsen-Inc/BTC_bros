from fear_and_greed.ingestion_fg.fetch_fear_greed_data import fetch_fear_greed_data
from ohlc_hourly.ingestion_ohlc.fetch_ohlc_hourly_data import fetch_ohlc_data_from_api, headers, dates 
from storage.store_in_db import stream_data_to_bigquery, client, bigquery_raw_data_table
import logging


def main():
    # Step 1: Ingest data
#    raw_data = fetch_fear_greed_data(api_url)

    # Step 2: Process data
 #   if raw_data:
  #      processed_data = process_fear_greed_data(raw_data)
   # else:
    #    print("Failed to fetch ")
    
    #ensure_bigquery_fear_greed_table(client=client, dataset_id='shabubsinc_db', table_id='raw_fearNgread_data')

#    try:
 #       stream_data_to_bigquery(client=client, data=processed_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='fear_and_gread')
  #  except Exception as e:
   #     logging.error(f"Failed to stream data to BigQuery: {e}")
    

    api_data = fetch_ohlc_data_from_api(dates,headers)

    ohlc_data = bigquery_raw_data_table(client=client, dataset_id='shabubsinc_db', table_id='raw_hourly_ohlc_data',api_data=api_data)
    
    try:
        stream_data_to_bigquery(client=client, data=ohlc_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='raw_hourly_ohlc_data')
    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")

if __name__ == "__main__":
    main()