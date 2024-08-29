from data.storage.fetch_data_from_db import get_all_data_from_bigquery
from data.storage.store_in_db import stream_data_to_bigquery, client
from data.fear_and_greed.processing_fg.process_fear_greed_data import ensure_bigquery_fear_greed_table, extract_required_fields
import logging

def fear_greed(): 

    fear_greed_raw_data = get_all_data_from_bigquery(client=client,dataset_id='shabubsinc_db',table_id='raw_daily_fear_greed_data')
    ensure_bigquery_fear_greed_table(client=client, dataset_id='shabubsinc_db',table_id='clean_daily_fear_greed_data')
   
    structured_data = extract_required_fields(fear_greed_raw_data)
    
    try:
        stream_data_to_bigquery(client=client, data=structured_data, project_id='shabubsinc', dataset_id='shabubsinc_db', table_id='clean_daily_fear_greed_data')
    except Exception as e:
        logging.error(f"Failed to stream fear and greed data to BigQuery: {e}")

if __name__ == "__main__":
    fear_greed()