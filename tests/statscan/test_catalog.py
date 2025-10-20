"""Tests for catalog enhancement functions."""

from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
from src.statscan import catalog


class TestEnhanceCatalog:
    """Test catalog enhancement logic."""

    def test_marks_availability_correctly(self):
        """Test that available column is set based on existing IDs."""
        catalog_df = pd.DataFrame({
            'productId': [1, 2, 3, 4, 5],
            'title': ['Dataset A', 'Dataset B', 'Dataset C', 'Dataset D', 'Dataset E'],
            'frequency_label': ['Monthly', 'Annual', 'Quarterly', 'Monthly', 'Annual'],
            'releaseTime': ['2024-01-01'] * 5,
            'available': [False] * 5
        })
        existing = {1, 3, 5}

        result = catalog.enhance_catalog(catalog_df,existing)

        assert result['available'].tolist() == [True, False, True, False, True]

    def test_preserves_all_columns(self):
        """Test that all columns are preserved."""
        catalog_df = pd.DataFrame({
            'productId': [1, 2],
            'title': ['A', 'B'],
            'subject': ['Economics', 'Immigration'],
            'frequency_label': ['Monthly', 'Annual'],
            'releaseTime': ['2024-01-01', '2024-02-01'],
            'dimensions': [5, 3],
            'nbDatapoints': [1000, 2000],
            'score': [0.8, 0.9],
            'available': [False, False]
        })

        result = catalog.enhance_catalog(catalog_df,{1})

        # All columns preserved
        assert set(result.columns) == set(catalog_df.columns)
        assert len(result.columns) == len(catalog_df.columns)

    def test_no_existing_datasets(self):
        """Test enhancement when no datasets exist in S3."""
        catalog_df = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency_label': ['Monthly', 'Annual', 'Quarterly'],
            'releaseTime': ['2024-01-01'] * 3,
            'available': [False] * 3
        })

        result = catalog.enhance_catalog(catalog_df,set())

        assert result['available'].sum() == 0
        assert all(result['available'] == False)

    def test_all_datasets_exist(self):
        """Test enhancement when all datasets exist in S3."""
        catalog_df = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency_label': ['Monthly', 'Annual', 'Quarterly'],
            'releaseTime': ['2024-01-01'] * 3,
            'available': [False] * 3
        })
        existing = {1, 2, 3}

        result = catalog.enhance_catalog(catalog_df,existing)

        assert result['available'].sum() == 3
        assert all(result['available'] == True)

    def test_preserves_original_dataframe(self):
        """Test that original catalog is not modified."""
        catalog_df = pd.DataFrame({
            'productId': [1, 2],
            'title': ['A', 'B'],
            'frequency_label': ['Monthly', 'Annual'],
            'releaseTime': ['2024-01-01', '2024-02-01'],
            'available': [False] * 2
        })
        original_data = catalog_df.copy()

        catalog.enhance_catalog(catalog_df, {1})

        # Original should be unchanged (except available column which we're testing)
        pd.testing.assert_frame_equal(catalog_df, original_data)


class TestMainIntegration:
    """Test main() orchestration with mocked I/O."""

    @patch('boto3.client')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_main_orchestration(self, mock_read_parquet, mock_get_existing, mock_boto_client):
        """Test that main() calls all functions in correct order."""
        # Mock catalog data (already has frequency_label and available columns)
        catalog_df = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency_label': ['Monthly', 'Annual', 'Quarterly'],
            'releaseTime': ['2024-01-01'] * 3,
            'available': [False] * 3
        })
        mock_read_parquet.return_value = catalog_df
        mock_get_existing.return_value = {1, 3}

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Run main
        catalog.main()

        # Verify catalog was read
        mock_read_parquet.assert_called_once_with('catalog.parquet')

        # Verify existing datasets were fetched
        mock_get_existing.assert_called_once_with('statscan')

        # Verify S3 upload was called
        mock_s3.upload_file.assert_called_once_with(
            'catalog.parquet',
            'build-cananda-dw',
            'statscan/catalog/catalog.parquet'
        )
