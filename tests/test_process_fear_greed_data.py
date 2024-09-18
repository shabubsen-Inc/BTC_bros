import unittest
from unittest.mock import MagicMock, patch
from google.api_core.exceptions import NotFound
from src.fear_and_greed.processing.process_fear_greed_data import ensure_bigquery_fear_greed_table


class TestEnsureBigQueryFearGreedTable(unittest.TestCase):

    @patch('src.fear_and_greed.processing.process_fear_greed_data.bigquery.Client')
    def test_table_exists_with_schema(self, mock_bigquery_client):

        mock_table = MagicMock()
        mock_table.schema = [
            {'name': 'value', 'type': 'INTEGER'},
            {'name': 'value_classification', 'type': 'STRING'},
            {'name': 'timestamp', 'type': 'TIM ESTAMP'}
        ]
        mock_bigquery_client.return_value.get_table.return_value = mock_table

        ensure_bigquery_fear_greed_table(
            mock_bigquery_client.return_value, "test_dataset", "test_table")

        mock_bigquery_client.return_value.get_table.assert_called_once()

    @patch('src.fear_and_greed.processing.process_fear_greed_data.bigquery.Client')
    def test_table_does_not_exist(self, mock_bigquery_client):
        mock_bigquery_client.return_value.get_table.side_effect = NotFound(
            "Table not found")

        mock_table = MagicMock()
        mock_bigquery_client.return_value.create_table.return_value = mock_table

        ensure_bigquery_fear_greed_table(
            mock_bigquery_client.return_value, "test_dataset", "test_table")

        mock_bigquery_client.return_value.create_table.assert_called_once()


if __name__ == "__main__":
    unittest.main()
