import requests
from datetime import datetime, timedelta
from google.cloud import secretmanager
from typing import List, Dict, Optional, Union
import logging
import time

PROJECT_ID = "shabubsinc"


start_date_str = "2024-09-03T00:00:00"
end_date_str = "2019-09-13T00:00:00"

start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S")


def API_call_limiter(
    call_count: int, max_calls_per_minute: int, call_interval: int
) -> int:
    """
    Rate limiter to ensure the API calls do not exceed the allowed limit.

    Args:
        call_count (int): The current count of API calls made. set a call count to 0 outside of the api call loop
        max_calls_per_minute (int): The maximum number of API calls allowed per minute.
        call_interval (float): The interval between API calls in seconds. this should be 60/max_calls_per_minute

        Implement like this  call_count = minute_request_limiter(call_count, CALLS_PER_MINUTE, CALL_INTERVAL)
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
    secret_manager_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = secret_manager_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


api_key = access_secret_version(PROJECT_ID, "coinapi-key")

headers = {"X-CoinAPI-Key": api_key}

dates = [
    (start_date + timedelta(days=i)).strftime("%Y-%m-%dT00:00:00")
    for i in range((end_date - start_date).days + 1)
]


def fetch_ohlc_data_from_api(
    headers: Dict[str, str], dates: Optional[List[str]] = None
) -> Union[List[Dict], Dict]:
    response = None
    if dates:
        data_list = []
        for date in dates:
            url = f"https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?period_id=1HRS&time_start={date}&limit=24"
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                data = response.json()

                # Unpack the response if it's a list of dictionaries
                if isinstance(data, list):
                    for data_point in data:
                        # Append each dictionary directly
                        data_list.append(data_point)
                elif isinstance(data, dict):
                    data_list.append(
                        data
                    )  # In case it's a single dictionary, append directly

                logging.info(f"Successfully fetched data for {date}.")

            except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as err:
                logging.error(f"Request failed: {err}")
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")

            if response.status_code == 200:
                data = response.json()
                logging.info(f"Error: {response.status_code} - {response.text}")

            else:
                logging.error("FAILED TO ACHIVE 200 status code")

        return data_list

    else:
        url = "https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/latest?period_id=1HRS&limit=1"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as err:
            logging.error(f"Request failed: {err}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

        # Add a check to ensure response is not None
        if response and response.status_code == 200:
            data = response.json()
            logging.info(f"Error: {response.status_code} - {response.text}")
            return data
        else:
            logging.error("FAILED TO ACHIEVE 200 status code")
