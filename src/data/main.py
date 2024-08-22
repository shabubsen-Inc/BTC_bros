from data.config import api_url
from data.ingestion.fetch_fear_greed_data import fetch_fear_greed_data
from data.processing.process_fear_greed_data import process_fear_greed_data


def main():
    # Step 1: Ingest data
    raw_data = fetch_fear_greed_data(api_url)

    # Step 2: Process data
    if raw_data:
        processed_data = process_fear_greed_data(raw_data)
    else:
        print("Failed to fetch data.")

    #! TODO: upload to google cloud


if __name__ == "__main__":
    main()
