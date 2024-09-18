import unittest
from unittest.mock import MagicMock, patch
from google.api_core.exceptions import NotFound
from src.ohlc.processing.process_ohlc_data import ensure_bigquery_ohlc_table


class TestEnsureBigQueryOHLCTable(unittest.TestCase):

    @patch('src.ohlc.processing.process_ohlc_data.bigquery.Client')
    def test_table_exists_with_schema(self, mock_bigquery_client):

        mock_table = MagicMock()
        mock_table.schema = [
            {'name': 'time_period_start', 'type': 'TIMESTAMP'},
            {'name': 'time_period_end', 'type': 'TIMESTAMP'},
        ]
        mock_bigquery_client.get_table.return_value = mock_table

        ensure_bigquery_ohlc_table(
            mock_bigquery_client, "test_dataset", "test_table")

        mock_bigquery_client.get_table.assert_called_once()

    @patch('src.ohlc.processing.process_ohlc_data.bigquery.Client')
    def test_table_does_not_exist(self, mock_bigquery_client):

        mock_bigquery_client.get_table.side_effect = NotFound(
            "Table not found")

        mock_table = MagicMock()
        mock_bigquery_client.create_table.return_value = mock_table

        ensure_bigquery_ohlc_table(
            mock_bigquery_client, "test_dataset", "test_table")

        mock_bigquery_client.create_table.assert_called_once()


if __name__ == '__main__':
    unittest.main()
