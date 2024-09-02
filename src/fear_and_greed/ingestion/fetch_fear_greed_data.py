from typing import Optional, Dict
import requests
import logging


def fetch_fear_greed_data(api_uri: str) -> Optional[Dict]:
    try:
        response = requests.get(api_uri, timeout=10)

        response.raise_for_status()

        data = response.json()

        return data

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")

    return None
