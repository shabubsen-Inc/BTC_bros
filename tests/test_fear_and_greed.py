import unittest
from unittest.mock import patch
import requests
from src.fear_and_greed.ingestion.fetch_fear_greed_data import fetch_fear_greed_data


class TestFetchFearGreedData(unittest.TestCase):

    @patch('requests.get')
    def test_fetch_success(self, mock_get):

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "name": "test_data",
            "value": 50
        }

        api_url = "https://api.alternative.me/fng/?limit=1"
        result = fetch_fear_greed_data(api_url)

        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "test_data")

    @patch('requests.get')
    def test_fetch_http_error(self, mock_get):

        mock_get.side_effect = requests.exceptions.HTTPError("Error occurred")

        api_url = "https://api.alternative.me/fng/?limit=1"
        result = fetch_fear_greed_data(api_url)

        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
