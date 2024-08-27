import requests
from datetime import datetime, timedelta
from google.cloud import secretmanager
import logging


PROJECT_ID = 'shabubsinc'

def access_secret_version(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    #this does not work becaus the service account doesnt have permissions to read secret manager.. hussein will fix.
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


api_key = access_secret_version(PROJECT_ID, "coinapi-key")


headers = {'X-CoinAPI-Key': api_key}

start_date_str = '2017-02-01T00:00:00'
end_date_str   = '2017-02-02T00:00:00'

start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
end_date   = datetime.strptime(end_date_str,   "%Y-%m-%dT%H:%M:%S")


dates = [
    (start_date + timedelta(days=i)).strftime("%Y-%m-%dT00:00:00")
    for i in range((end_date - start_date).days + 1)
]

def access_secret_version(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


api_key = access_secret_version(PROJECT_ID, "COINAPI_KEY")


def fetch_ohlc_data_from_api(dates, headers):
    all_data = []
    for date in dates:
        url = f'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?period_id=1HRS&time_start={date}&limit=24'

        try:
            response = requests.get(url, headers=headers)
        
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as err:
            logging.error(f"Request failed: {err}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

        if response.status_code == 200:
            data = response.json()
            all_data.append(data)
            print(f"Error: {response.status_code} - {response.text}")
        return all_data