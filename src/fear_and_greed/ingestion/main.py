from fear_and_greed.ingestion.fetch_fear_greed_data import fetch_fear_greed_data
from shared_functions import bigquery_raw_data_table, stream_data_to_bigquery, bigquery_client
import logging


def main():
    api_url = "https://api.alternative.me/fng/?limit=10"
    fear_greed_data = fetch_fear_greed_data(api_url)
    
    raw_fear_greed_data = bigquery_raw_data_table(bigquery_client=bigquery_client, dataset_id='shabubsinc_db', table_id='raw_daily_fear_greed_data',api_data=fear_greed_data)

    try:
        stream_data_to_bigquery(bigquery_client=bigquery_client, data=raw_fear_greed_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='raw_daily_fear_greed_data')
    
    except Exception as e:
        logging.error(f"Failed to stream data to BigQuery: {e}")


if __name__ == "__main__":
    main()