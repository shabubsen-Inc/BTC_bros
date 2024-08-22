
def process_fear_greed_data(raw_data: dict) -> dict:
    if raw_data:

        data_entries = raw_data.get("data", [])
      
        data_length = len(data_entries)
      
        print(f"Number of entries in data: {data_length}")

        return data_entries
    
    else:
        print("No data to process.")
        return None
