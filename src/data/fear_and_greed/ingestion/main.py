from data.config import api_url
from data.fear_and_greed.ingestion_fg.fetch_fear_greed_data import fetch_fear_greed_data
from data.storage.store_in_db import bigquery_raw_data_table, stream_data_to_bigquery, client
import logging


def main():
    fear_greed_data = fetch_fear_greed_data(api_url)
    print(fear_greed_data)
    
    raw_fear_greed_data = bigquery_raw_data_table(client=client, dataset_id='shabubsinc_db', table_id='raw_daily_fear_greed_data',api_data=fear_greed_data)

    try:
        stream_data_to_bigquery(client=client, data=raw_fear_greed_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='raw_daily_fear_greed_data')
    
    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")


if __name__ == "__main__":
    main()