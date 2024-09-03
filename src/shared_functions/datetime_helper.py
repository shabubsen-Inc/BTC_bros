from datetime import datetime, timezone

def convert_unix_timestamp_in_data(data:dict)->dict:
    """
    Converts Unix timestamps in the 'timestamp' fields to a formatted string 'YYYY-MM-DD HH:MM:SS UTC'.

    Args:
        data (dict, list): The data structure containing 'timestamp' fields to convert.

    Returns:
        dict, list: The data structure with 'timestamp' fields converted to formatted strings.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "timestamp" and isinstance(value, str):  # Convert only if the key is 'timestamp'
                try:
                    unix_timestamp = int(value)
                    data[key] = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                except ValueError:
                    continue  # If the conversion fails, skip this field
            else:
                data[key] = convert_unix_timestamp_in_data(value)
    elif isinstance(data, list):
        return [convert_unix_timestamp_in_data(item) for item in data]
    
    return data


def convert_datetime_to_string(data: datetime) -> str:
    formatted_dates = {dt.strftime("%Y-%m-%d %H:%M:%S UTC") for dt in data}
    return formatted_dates