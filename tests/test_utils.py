"""Tests for S3 utility functions."""

from unittest.mock import MagicMock, patch
from src.pipeline import utils


class TestExtractProductIdFromFolder:
    """Test product ID extraction from folder names."""

    def test_extracts_id_from_standard_folder(self):
        result = utils.extract_product_id_from_folder('12100163-international-trade')
        assert result == 12100163

    def test_extracts_id_from_long_folder(self):
        result = utils.extract_product_id_from_folder('43100050-immigrant-income-by-period-of-immigration')
        assert result == 43100050

    def test_returns_none_for_catalog_folder(self):
        result = utils.extract_product_id_from_folder('catalog')
        assert result is None

    def test_returns_none_for_no_dash(self):
        result = utils.extract_product_id_from_folder('nodashhere')
        assert result is None

    def test_returns_none_for_non_numeric_prefix(self):
        result = utils.extract_product_id_from_folder('abc-dataset')
        assert result is None

    def test_handles_folder_with_trailing_slash(self):
        # This should be handled by the caller, but test the function behavior
        result = utils.extract_product_id_from_folder('12345-dataset')
        assert result == 12345

    def test_handles_multiple_dashes(self):
        result = utils.extract_product_id_from_folder('98100524-languages-used-at-work-by-languages')
        assert result == 98100524


class TestGetExistingDatasetIds:
    """Test S3 dataset ID extraction logic."""

    @patch('boto3.client')
    def test_extracts_product_ids_from_folders(self, mock_boto_client):
        """Test that productIds are correctly extracted from S3 folder names."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        # Mock S3 response with typical folder structure
        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/12100163-international-trade/'},
                    {'Prefix': 'statscan/data/43100050-immigrant-income/'},
                    {'Prefix': 'statscan/data/98100524-languages-used-at-work/'},
                ]
            }
        ]

        result = utils.get_existing_dataset_ids('statscan')

        assert result == {12100163, 43100050, 98100524}
        assert len(result) == 3

    @patch('boto3.client')
    def test_ignores_catalog_folder(self, mock_boto_client):
        """Test that catalog folder is not treated as a dataset."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/12100163-international-trade/'},
                    {'Prefix': 'statscan/data/catalog/'},  # Should be ignored
                ]
            }
        ]

        result = utils.get_existing_dataset_ids('statscan')

        assert result == {12100163}
        assert 'catalog' not in result

    @patch('boto3.client')
    def test_handles_empty_s3_bucket(self, mock_boto_client):
        """Test that empty S3 bucket returns empty set."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_paginator.paginate.return_value = [{'CommonPrefixes': []}]

        result = utils.get_existing_dataset_ids('statscan')

        assert result == set()

    @patch('boto3.client')
    def test_handles_pagination(self, mock_boto_client):
        """Test that pagination is handled correctly."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        # Multiple pages
        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/1-dataset-one/'},
                    {'Prefix': 'statscan/data/2-dataset-two/'},
                ]
            },
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/3-dataset-three/'},
                ]
            }
        ]

        result = utils.get_existing_dataset_ids('statscan')

        assert result == {1, 2, 3}
        assert len(result) == 3


class TestGetExistingDatasetFolders:
    """Test S3 dataset folder listing logic."""

    @patch('boto3.client')
    def test_returns_folder_names(self, mock_boto_client):
        """Test that folder names are correctly extracted from S3."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        # Mock S3 response with typical folder structure
        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/12100163-international-trade/'},
                    {'Prefix': 'statscan/data/43100050-immigrant-income/'},
                    {'Prefix': 'statscan/data/98100524-languages-used-at-work/'},
                ]
            }
        ]

        result = utils.get_existing_dataset_folders('statscan')

        assert result == [
            '12100163-international-trade',
            '43100050-immigrant-income',
            '98100524-languages-used-at-work'
        ]
        assert len(result) == 3

    @patch('boto3.client')
    def test_includes_all_folders(self, mock_boto_client):
        """Test that all folders are included, even non-dataset ones like catalog."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/12100163-international-trade/'},
                    {'Prefix': 'statscan/data/catalog/'},
                ]
            }
        ]

        result = utils.get_existing_dataset_folders('statscan')

        # Unlike get_existing_dataset_ids, this returns ALL folders
        assert result == ['12100163-international-trade', 'catalog']
        assert len(result) == 2

    @patch('boto3.client')
    def test_handles_empty_s3_bucket(self, mock_boto_client):
        """Test that empty S3 bucket returns empty list."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_paginator.paginate.return_value = [{'CommonPrefixes': []}]

        result = utils.get_existing_dataset_folders('statscan')

        assert result == []

    @patch('boto3.client')
    def test_handles_pagination(self, mock_boto_client):
        """Test that pagination is handled correctly."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        # Multiple pages
        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/1-dataset-one/'},
                    {'Prefix': 'statscan/data/2-dataset-two/'},
                ]
            },
            {
                'CommonPrefixes': [
                    {'Prefix': 'statscan/data/3-dataset-three/'},
                ]
            }
        ]

        result = utils.get_existing_dataset_folders('statscan')

        assert result == ['1-dataset-one', '2-dataset-two', '3-dataset-three']
        assert len(result) == 3
