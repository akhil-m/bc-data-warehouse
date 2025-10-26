"""Tests for catalog enhancement functions."""

from datetime import datetime
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

    @patch('os.path.exists')
    @patch('boto3.client')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_main_orchestration(self, mock_read_parquet, mock_get_existing, mock_boto_client, mock_exists):
        """Test that main() calls all functions in correct order."""
        # Mock catalog data
        catalog_df = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency': ['Monthly', 'Annual', 'Quarterly'],
            'releaseTime': ['2024-01-01'] * 3
        })

        # Mock existing catalog (empty for first run)
        existing_catalog_df = pd.DataFrame()

        # Mock read_parquet to return different values based on filename
        def read_parquet_side_effect(filename):
            if filename == 'existing_catalog.parquet':
                return existing_catalog_df
            return catalog_df

        mock_read_parquet.side_effect = read_parquet_side_effect
        mock_get_existing.return_value = {1, 3}
        mock_exists.return_value = False  # No ingested.csv

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.download_file.side_effect = Exception("No existing catalog")  # First run
        mock_boto_client.return_value = mock_s3

        # Run main
        catalog.main()

        # Verify S3 download was attempted
        mock_s3.download_file.assert_called_once()

        # Verify existing datasets were fetched
        mock_get_existing.assert_called_once_with('statscan')

        # Verify S3 upload was called
        mock_s3.upload_file.assert_called_once_with(
            'catalog.parquet',
            'build-cananda-dw',
            'statscan/catalog/catalog.parquet'
        )


class TestInitializeIngestionDates:
    """Test initialization of last_ingestion_date column."""

    def test_adds_column_if_missing(self):
        """Test that column is added if not present."""
        catalog_df = pd.DataFrame({
            'productId': [1, 2],
            'title': ['A', 'B']
        })

        result = catalog.initialize_ingestion_dates(catalog_df)

        assert 'last_ingestion_date' in result.columns
        assert pd.isna(result['last_ingestion_date']).all()

    def test_preserves_column_if_present(self):
        """Test that existing column is preserved."""
        ingestion_date = datetime(2024, 1, 1)
        catalog_df = pd.DataFrame({
            'productId': [1, 2],
            'title': ['A', 'B'],
            'last_ingestion_date': [ingestion_date, pd.NaT]
        })

        result = catalog.initialize_ingestion_dates(catalog_df)

        assert result['last_ingestion_date'].iloc[0] == ingestion_date
        assert pd.isna(result['last_ingestion_date'].iloc[1])

    def test_preserves_original_dataframe(self):
        """Test that original catalog is not modified."""
        catalog_df = pd.DataFrame({
            'productId': [1],
            'title': ['A']
        })
        original_columns = catalog_df.columns.tolist()

        catalog.initialize_ingestion_dates(catalog_df)

        # Original should be unchanged
        assert catalog_df.columns.tolist() == original_columns


class TestUpdateIngestionDates:
    """Test updating last_ingestion_date for ingested datasets."""

    def test_updates_specified_datasets(self):
        """Test that only specified datasets are updated."""
        catalog_df = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'last_ingestion_date': [pd.NaT, pd.NaT, pd.NaT]
        })
        ingestion_date = datetime(2024, 2, 15)

        result = catalog.update_ingestion_dates(catalog_df, ['A', 'C'], ingestion_date)

        assert result['last_ingestion_date'].iloc[0] == ingestion_date
        assert pd.isna(result['last_ingestion_date'].iloc[1])
        assert result['last_ingestion_date'].iloc[2] == ingestion_date

    def test_overwrites_existing_dates(self):
        """Test that existing dates are overwritten for re-ingested datasets."""
        old_date = datetime(2024, 1, 1)
        new_date = datetime(2024, 2, 15)
        catalog_df = pd.DataFrame({
            'productId': ['A', 'B'],
            'last_ingestion_date': [old_date, pd.NaT]
        })

        result = catalog.update_ingestion_dates(catalog_df, ['A'], new_date)

        assert result['last_ingestion_date'].iloc[0] == new_date
        assert pd.isna(result['last_ingestion_date'].iloc[1])

    def test_handles_empty_ingested_list(self):
        """Test no updates when ingested list is empty."""
        catalog_df = pd.DataFrame({
            'productId': ['A', 'B'],
            'last_ingestion_date': [pd.NaT, pd.NaT]
        })
        ingestion_date = datetime(2024, 2, 15)

        result = catalog.update_ingestion_dates(catalog_df, [], ingestion_date)

        assert pd.isna(result['last_ingestion_date']).all()

    def test_preserves_original_dataframe(self):
        """Test that original catalog is not modified."""
        catalog_df = pd.DataFrame({
            'productId': ['A'],
            'last_ingestion_date': [pd.NaT]
        })
        original_data = catalog_df.copy()

        catalog.update_ingestion_dates(catalog_df, ['A'], datetime(2024, 1, 1))

        pd.testing.assert_frame_equal(catalog_df, original_data)


class TestMergeCatalogMetadata:
    """Test merging fresh catalog with existing last_ingestion_date."""

    def test_preserves_ingestion_dates_for_existing_datasets(self):
        """Test that last_ingestion_date is preserved for datasets in both catalogs."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B'],
            'title': ['Dataset A Updated', 'Dataset B Updated'],
            'frequency': ['Monthly', 'Quarterly']
        })
        existing = pd.DataFrame({
            'productId': ['A', 'B'],
            'title': ['Dataset A Old', 'Dataset B Old'],
            'frequency': ['Monthly', 'Quarterly'],
            'last_ingestion_date': [datetime(2024, 1, 1), datetime(2024, 1, 15)]
        })

        result = catalog.merge_catalog_metadata(fresh, existing)

        # Fresh metadata used
        assert result['title'].iloc[0] == 'Dataset A Updated'
        # But ingestion dates preserved
        assert result['last_ingestion_date'].iloc[0] == datetime(2024, 1, 1)
        assert result['last_ingestion_date'].iloc[1] == datetime(2024, 1, 15)

    def test_new_datasets_get_nat_ingestion_date(self):
        """Test that new datasets get NaT for last_ingestion_date."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['A', 'B', 'C'],
            'frequency': ['Monthly', 'Quarterly', 'Annual']
        })
        existing = pd.DataFrame({
            'productId': ['A'],
            'title': ['A'],
            'frequency': ['Monthly'],
            'last_ingestion_date': [datetime(2024, 1, 1)]
        })

        result = catalog.merge_catalog_metadata(fresh, existing)

        assert result['last_ingestion_date'].iloc[0] == datetime(2024, 1, 1)
        assert pd.isna(result['last_ingestion_date'].iloc[1])
        assert pd.isna(result['last_ingestion_date'].iloc[2])

    def test_empty_existing_catalog(self):
        """Test merge when existing catalog is empty (first run)."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B'],
            'title': ['A', 'B'],
            'frequency': ['Monthly', 'Quarterly']
        })
        existing = pd.DataFrame()

        result = catalog.merge_catalog_metadata(fresh, existing)

        assert len(result) == 2
        assert 'last_ingestion_date' in result.columns
        assert pd.isna(result['last_ingestion_date']).all()

    def test_existing_without_ingestion_date_column(self):
        """Test merge when existing catalog doesn't have last_ingestion_date column."""
        fresh = pd.DataFrame({
            'productId': ['A'],
            'title': ['A'],
            'frequency': ['Monthly']
        })
        existing = pd.DataFrame({
            'productId': ['A'],
            'title': ['A Old'],
            'frequency': ['Monthly']
        })

        result = catalog.merge_catalog_metadata(fresh, existing)

        assert 'last_ingestion_date' in result.columns
        assert pd.isna(result['last_ingestion_date']).all()

    def test_preserves_all_fresh_metadata_columns(self):
        """Test that all columns from fresh catalog are preserved."""
        fresh = pd.DataFrame({
            'productId': ['A'],
            'title': ['Dataset A'],
            'subject': ['Economics'],
            'frequency': ['Monthly'],
            'releaseTime': ['2024-01-01'],
            'dimensions': [5],
            'nbDatapoints': [1000]
        })
        existing = pd.DataFrame({
            'productId': ['A'],
            'last_ingestion_date': [datetime(2024, 1, 1)]
        })

        result = catalog.merge_catalog_metadata(fresh, existing)

        # All fresh columns present
        for col in fresh.columns:
            assert col in result.columns
        # Plus last_ingestion_date
        assert 'last_ingestion_date' in result.columns
