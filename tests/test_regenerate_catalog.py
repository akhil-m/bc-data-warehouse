"""Tests for catalog enhancement functions."""

from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
import regenerate_catalog


class TestDecodeFrequency:
    """Test frequency code decoding logic."""

    def test_decodes_known_frequencies(self):
        assert regenerate_catalog.decode_frequency(1) == 'Occasional'
        assert regenerate_catalog.decode_frequency(6) == 'Monthly'
        assert regenerate_catalog.decode_frequency(9) == 'Quarterly'
        assert regenerate_catalog.decode_frequency(12) == 'Annual'
        assert regenerate_catalog.decode_frequency(18) == 'Census'

    def test_decodes_all_frequency_codes(self):
        """Test all defined frequency codes."""
        expected = {
            1: 'Occasional', 2: 'Biannual', 6: 'Monthly', 9: 'Quarterly',
            11: 'Bimonthly', 12: 'Annual', 13: 'Biennial', 14: 'Triennial',
            15: 'Quinquennial', 16: 'Decennial', 17: 'Every 3 years',
            18: 'Census', 19: 'Every 4 years', 20: 'Every 6 years'
        }
        for code, label in expected.items():
            assert regenerate_catalog.decode_frequency(code) == label

    def test_returns_unknown_for_invalid_code(self):
        assert regenerate_catalog.decode_frequency(999) == 'Unknown'
        assert regenerate_catalog.decode_frequency(0) == 'Unknown'
        assert regenerate_catalog.decode_frequency(-1) == 'Unknown'


class TestEnhanceCatalog:
    """Test catalog enhancement logic."""

    def test_marks_availability_correctly(self):
        """Test that available column is set based on existing IDs."""
        catalog = pd.DataFrame({
            'productId': [1, 2, 3, 4, 5],
            'title': ['Dataset A', 'Dataset B', 'Dataset C', 'Dataset D', 'Dataset E'],
            'frequency': [6, 12, 1, 9, 6],
            'releaseTime': ['2024-01-01'] * 5
        })
        existing = {1, 3, 5}

        result = regenerate_catalog.enhance_catalog(catalog, existing)

        assert result['available'].tolist() == [True, False, True, False, True]

    def test_decodes_frequency_labels(self):
        """Test that frequency codes are decoded to labels."""
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency': [6, 12, 999],  # Monthly, Annual, Unknown
            'releaseTime': ['2024-01-01'] * 3
        })

        result = regenerate_catalog.enhance_catalog(catalog, set())

        assert result['frequency_label'].tolist() == ['Monthly', 'Annual', 'Unknown']

    def test_selects_correct_columns(self):
        """Test that only required columns are kept."""
        catalog = pd.DataFrame({
            'productId': [1],
            'title': ['Dataset'],
            'frequency': [6],
            'releaseTime': ['2024-01-01'],
            'extra_column': ['should be dropped'],
            'another_extra': [123]
        })

        result = regenerate_catalog.enhance_catalog(catalog, set())

        expected_columns = ['productId', 'title', 'frequency_label', 'releaseTime', 'available']
        assert list(result.columns) == expected_columns

    def test_no_existing_datasets(self):
        """Test enhancement when no datasets exist in S3."""
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency': [6, 12, 9],
            'releaseTime': ['2024-01-01'] * 3
        })

        result = regenerate_catalog.enhance_catalog(catalog, set())

        assert result['available'].sum() == 0
        assert all(result['available'] == False)

    def test_all_datasets_exist(self):
        """Test enhancement when all datasets exist in S3."""
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency': [6, 12, 9],
            'releaseTime': ['2024-01-01'] * 3
        })
        existing = {1, 2, 3}

        result = regenerate_catalog.enhance_catalog(catalog, existing)

        assert result['available'].sum() == 3
        assert all(result['available'] == True)

    def test_preserves_original_dataframe(self):
        """Test that original catalog is not modified."""
        catalog = pd.DataFrame({
            'productId': [1, 2],
            'title': ['A', 'B'],
            'frequency': [6, 12],
            'releaseTime': ['2024-01-01', '2024-02-01']
        })
        original_columns = list(catalog.columns)

        regenerate_catalog.enhance_catalog(catalog, {1})

        # Original should be unchanged
        assert list(catalog.columns) == original_columns
        assert 'available' not in catalog.columns
        assert 'frequency_label' not in catalog.columns


class TestMainIntegration:
    """Test main() orchestration with mocked I/O."""

    @patch('boto3.client')
    @patch('utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_main_orchestration(self, mock_read_parquet, mock_get_existing, mock_boto_client):
        """Test that main() calls all functions in correct order."""
        # Mock catalog data
        catalog_df = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C'],
            'frequency': [6, 12, 9],
            'releaseTime': ['2024-01-01'] * 3
        })
        mock_read_parquet.return_value = catalog_df
        mock_get_existing.return_value = {1, 3}

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Run main
        regenerate_catalog.main()

        # Verify catalog was read
        mock_read_parquet.assert_called_once_with('catalog.parquet')

        # Verify existing datasets were fetched
        mock_get_existing.assert_called_once_with('statscan')

        # Verify S3 upload was called
        mock_s3.upload_file.assert_called_once_with(
            'catalog_enhanced.parquet',
            'build-cananda-dw',
            'statscan/data/catalog/catalog_enhanced.parquet'
        )
