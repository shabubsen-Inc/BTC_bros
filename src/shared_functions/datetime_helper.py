from datetime import datetime


def convert_datetime_to_string(data):
    """
    Recursively converts datetime objects in the data to ISO 8601 formatted strings.

    Args:
        data (dict, list, datetime): The data structure to convert.

    Returns:
        dict, list, or str: The data structure with datetime objects converted to strings.
    """
    if isinstance(data, dict):
        return {key: convert_datetime_to_string(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_datetime_to_string(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data
