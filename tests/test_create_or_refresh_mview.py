import unittest
from unittest.mock import MagicMock, patch
from src.consumption.create_or_refresh_materialized_view import create_or_refresh_materialized_view
from google.cloud.exceptions import GoogleCloudError


class TestCreateOrRefreshMaterializedView(unittest.TestCase):

    @patch('google.cloud.bigquery.Client')
    def test_create_or_refresh_success(self, mock_bigquery_client):
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = None
        mock_bigquery_client.query.return_value = mock_query_job

        create_or_refresh_materialized_view(
            bigquery_client=mock_bigquery_client,
            project_id="test_project",
            dataset_id="test_dataset",
            view_id="test_view"
        )

        mock_bigquery_client.query.assert_called_once()

    @patch('google.cloud.bigquery.Client')
    def test_create_or_refresh_error(self, mock_bigquery_client):
        mock_bigquery_client.query.side_effect = GoogleCloudError("Test Error")

        with self.assertRaises(GoogleCloudError):
            create_or_refresh_materialized_view(
                bigquery_client=mock_bigquery_client,
                project_id="test_project",
                dataset_id="test_dataset",
                view_id="test_view"
            )


if __name__ == '__main__':
    unittest.main()
