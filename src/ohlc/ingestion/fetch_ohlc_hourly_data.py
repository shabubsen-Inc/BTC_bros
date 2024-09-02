import requests
from datetime import datetime, timedelta
from google.cloud import secretmanager
import logging
import time

PROJECT_ID = "shabubsinc"

start_date_str = "2018-02-05T00:00:00"
end_date_str = "2019-08-24T00:00:00"

start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S")


def API_call_limiter(call_count, max_calls_per_minute, call_interval):
    """
    Rate limiter to ensure the API calls do not exceed the allowed limit.

    Args:
        call_count (int): The current count of API calls made. set a call count to 0 outside of the api call loop
        max_calls_per_minute (int): The maximum number of API calls allowed per minute.
        call_interval (float): The interval between API calls in seconds. this should be 60/max_calls_per_minute

        imploment like this  call_count = minute_request_limiter(call_count, CALLS_PER_MINUTE, CALL_INTERVAL)
    Returns:
        int: Updated call count after rate limiting.
    """
    if call_count >= max_calls_per_minute:
        logging.info("Reached call limit. Waiting for 60 seconds...")
        time.sleep(60)
        call_count = 0
    else:
        logging.info(f"current call count is {call_count}/ 30 before 60 sec pause")
        time.sleep(call_interval)

    return call_count + 1


def access_secret_version(project_id: str, secret_id: str, version_id="latest"):
    """this function fetches keys from google secret manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


api_key = access_secret_version(PROJECT_ID, "coinapi-key")

headers = {"X-CoinAPI-Key": api_key}

dates = [
    (start_date + timedelta(days=i)).strftime("%Y-%m-%dT00:00:00")
    for i in range((end_date - start_date).days + 1)
]


def fetch_ohlc_data_from_api(headers):
    """this function fetches ohlc data from coinapi.
    the function requires a date of which you want to fetch data
    and also a header containing a key value pair od the api destination and key.
    this function returnes raw formated data"""

    url = "https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/latest?period_id=1HRS&limit=1"

    try:
        response = requests.get(url, headers=headers,timeout=10)

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Request failed: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Error: {response.status_code} - {response.text}")

        return data
