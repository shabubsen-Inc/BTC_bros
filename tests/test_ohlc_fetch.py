import unittest
from unittest.mock import patch, MagicMock
from src.ohlc.ingestion.fetch_ohlc_hourly_data import fetch_ohlc_data_from_api, access_secret_version
import requests


class TestOHLCFetch(unittest.TestCase):

    @patch('requests.get')
    def test_fetch_ohlc_data_success(self, mock_get):

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"time_period_start": "2024-09-17T00:00:00"}]

        mock_get.return_value = mock_response

        headers = {"X-CoinAPI-Key": "test_key"}
        result = fetch_ohlc_data_from_api(
            headers=headers, dates=["2024-09-17"])

        self.assertIsNotNone(result)
        self.assertEqual(
            result[0]["time_period_start"], "2024-09-17T00:00:00")

    @patch('requests.get')
    def test_fetch_ohlc_data_http_error(self, mock_get):

        mock_get.side_effect = requests.exceptions.HTTPError("Error occurred")

        headers = {"X-CoinAPI-Key": "test_key"}
        result = fetch_ohlc_data_from_api(
            headers=headers, dates=["2024-09-17"])

        self.assertIsNone(result)


@patch('src.ohlc.ingestion.fetch_ohlc_hourly_data.secretmanager.SecretManagerServiceClient.access_secret_version')
def test_access_secret_version(self, mock_secret_manager_client):
    mock_secret = MagicMock()
    mock_secret.payload.data.decode.return_value = "test_key"
    mock_secret_manager_client.return_value.access_secret_version.return_value = mock_secret
    result = access_secret_version("shabubsinc", "coinapi-key")
    self.assertEqual(result, "test_key")


if __name__ == '__main__':
    unittest.main()
