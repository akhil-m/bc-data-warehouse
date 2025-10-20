"""Tests for Glue crawler update functions."""

from unittest.mock import MagicMock, patch
from src.statscan import crawler


class TestExtractProductIdFromTableName:
    """Test product ID extraction from Glue table names."""

    def test_extracts_from_standard_table_name(self):
        assert crawler.extract_product_id_from_table_name('12100163_international_trade') == 12100163

    def test_extracts_from_different_product_ids(self):
        assert crawler.extract_product_id_from_table_name('43100050_immigration_data') == 43100050
        assert crawler.extract_product_id_from_table_name('10100001_government_employment') == 10100001

    def test_returns_none_for_catalog_table(self):
        assert crawler.extract_product_id_from_table_name('catalog') is None

    def test_returns_none_for_table_without_underscore(self):
        assert crawler.extract_product_id_from_table_name('tablenamewithoutunderscore') is None

    def test_returns_none_for_non_numeric_prefix(self):
        assert crawler.extract_product_id_from_table_name('abc_table_name') is None

    def test_returns_none_for_empty_string(self):
        assert crawler.extract_product_id_from_table_name('') is None


class TestFindNewFolders:
    """Test incremental folder detection logic."""

    def test_all_folders_new_when_no_tables(self):
        folders = ['12100163-trade', '43100050-immigration']
        tables = ['catalog']
        result = crawler.find_new_folders(folders, tables)
        assert len(result) == 2
        assert '12100163-trade' in result
        assert '43100050-immigration' in result

    def test_no_new_folders_when_all_exist(self):
        folders = ['12100163-trade', '43100050-immigration']
        tables = ['catalog', '12100163_international_trade', '43100050_immigration_data']
        result = crawler.find_new_folders(folders, tables)
        assert len(result) == 0

    def test_finds_only_new_folders(self):
        folders = ['12100163-trade', '43100050-immigration', '10100001-government']
        tables = ['catalog', '12100163_international_trade']
        result = crawler.find_new_folders(folders, tables)
        assert len(result) == 2
        assert '43100050-immigration' in result
        assert '10100001-government' in result
        assert '12100163-trade' not in result

    def test_ignores_catalog_table(self):
        folders = ['12100163-trade']
        tables = ['catalog', 'catalog_old', 'catalog_backup']
        result = crawler.find_new_folders(folders, tables)
        assert len(result) == 1

    def test_handles_malformed_folder_names(self):
        folders = ['12100163-trade', 'bad-folder-name', 'also-bad']
        tables = []
        result = crawler.find_new_folders(folders, tables)
        assert len(result) == 1
        assert '12100163-trade' in result

    def test_handles_empty_folder_list(self):
        result = crawler.find_new_folders([], ['table1', 'table2'])
        assert result == []

    def test_handles_empty_table_list(self):
        folders = ['12100163-trade']
        result = crawler.find_new_folders(folders, [])
        assert len(result) == 1


class TestCreateS3Targets:
    """Test S3 target creation logic."""

    def test_creates_single_target(self):
        folders = ['12100163-trade']
        result = crawler.create_s3_targets(folders, 's3://bucket/data/')

        assert len(result) == 1
        assert result[0] == {
            'Path': 's3://bucket/data/12100163-trade/',
            'Exclusions': []
        }

    def test_creates_multiple_targets(self):
        folders = ['12100163-trade', '43100050-immigration']
        result = crawler.create_s3_targets(folders, 's3://bucket/data/')

        assert len(result) == 2
        assert result[0] == {
            'Path': 's3://bucket/data/12100163-trade/',
            'Exclusions': []
        }
        assert result[1] == {
            'Path': 's3://bucket/data/43100050-immigration/',
            'Exclusions': []
        }

    def test_empty_folder_list(self):
        """Test that empty folder list returns empty target list."""
        result = crawler.create_s3_targets([], 's3://bucket/data/')
        assert result == []

    def test_preserves_folder_names(self):
        """Test that folder names are used exactly as provided."""
        folders = ['folder-with-dashes', 'folder_with_underscores', '12345-numeric']
        result = crawler.create_s3_targets(folders, 's3://bucket/')

        assert result[0]['Path'] == 's3://bucket/folder-with-dashes/'
        assert result[1]['Path'] == 's3://bucket/folder_with_underscores/'
        assert result[2]['Path'] == 's3://bucket/12345-numeric/'

    def test_handles_different_bucket_prefixes(self):
        """Test with various bucket prefix formats."""
        folders = ['dataset']

        # With trailing slash
        result1 = crawler.create_s3_targets(folders, 's3://bucket/prefix/')
        assert result1[0]['Path'] == 's3://bucket/prefix/dataset/'

        # Without trailing slash (should still work)
        result2 = crawler.create_s3_targets(folders, 's3://bucket/prefix')
        assert result2[0]['Path'] == 's3://bucket/prefixdataset/'


class TestCreateCrawlerUpdateParams:
    """Test crawler update parameter creation."""

    def test_creates_correct_structure(self):
        """Test that all required parameters are present."""
        targets = [{'Path': 's3://bucket/folder/'}]
        result = crawler.create_crawler_update_params(
            targets,
            'my-crawler',
            'arn:aws:iam::123:role/MyRole',
            'my-database'
        )

        assert result['Name'] == 'my-crawler'
        assert result['Role'] == 'arn:aws:iam::123:role/MyRole'
        assert result['DatabaseName'] == 'my-database'
        assert result['Targets']['S3Targets'] == targets
        assert 'SchemaChangePolicy' in result

    def test_schema_change_policy(self):
        """Test that schema change policy is configured correctly."""
        targets = []
        result = crawler.create_crawler_update_params(
            targets, 'crawler', 'role', 'db'
        )

        policy = result['SchemaChangePolicy']
        assert policy['UpdateBehavior'] == 'UPDATE_IN_DATABASE'
        assert policy['DeleteBehavior'] == 'DEPRECATE_IN_DATABASE'

    def test_handles_multiple_targets(self):
        """Test with multiple S3 targets."""
        targets = [
            {'Path': 's3://bucket/folder1/'},
            {'Path': 's3://bucket/folder2/'},
            {'Path': 's3://bucket/folder3/'}
        ]
        result = crawler.create_crawler_update_params(
            targets, 'crawler', 'role', 'db'
        )

        assert result['Targets']['S3Targets'] == targets
        assert len(result['Targets']['S3Targets']) == 3

    def test_handles_empty_targets(self):
        """Test with empty target list."""
        targets = []
        result = crawler.create_crawler_update_params(
            targets, 'crawler', 'role', 'db'
        )

        assert result['Targets']['S3Targets'] == []


class TestMainIntegration:
    """Test main() orchestration with mocked I/O."""

    @patch('boto3.client')
    @patch('src.statscan.utils.get_existing_dataset_folders')
    def test_main_orchestration_with_new_folders(self, mock_get_folders, mock_boto_client):
        """Test that main() only crawls new folders."""
        # Mock S3 folder query
        mock_get_folders.return_value = ['12100163-trade', '43100050-immigration', '10100001-government']

        # Mock Glue client
        mock_glue = MagicMock()
        mock_boto_client.return_value = mock_glue

        # Mock get_tables paginator to return existing tables
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'TableList': [
                {'Name': 'catalog'},
                {'Name': '12100163_international_trade'}
            ]}
        ]
        mock_glue.get_paginator.return_value = mock_paginator

        # Run main
        crawler.main()

        # Verify S3 was queried
        mock_get_folders.assert_called_once_with('statscan')

        # Verify Glue client was created
        mock_boto_client.assert_called_once_with('glue', region_name='us-east-2')

        # Verify get_tables was called
        mock_glue.get_paginator.assert_called_once_with('get_tables')
        mock_paginator.paginate.assert_called_once_with(DatabaseName='statscan')

        # Verify update_crawler was called with correct parameters
        mock_glue.update_crawler.assert_called_once()
        call_args = mock_glue.update_crawler.call_args[1]

        assert call_args['Name'] == 'statscan'
        assert call_args['DatabaseName'] == 'statscan'
        assert call_args['Role'] == 'service-role/AWSGlueServiceRole-statscan'

        # Should only have 2 NEW folders + catalog (not the existing 12100163)
        assert len(call_args['Targets']['S3Targets']) == 3
        targets = call_args['Targets']['S3Targets']
        assert targets[0]['Path'] == 's3://build-cananda-dw/statscan/data/43100050-immigration/'
        assert targets[1]['Path'] == 's3://build-cananda-dw/statscan/data/10100001-government/'
        assert targets[2]['Path'] == 's3://build-cananda-dw/statscan/catalog/'

        # Verify crawler was started after update
        mock_glue.start_crawler.assert_called_once_with(Name='statscan')

    @patch('boto3.client')
    @patch('src.statscan.utils.get_existing_dataset_folders')
    def test_main_orchestration_with_no_new_folders(self, mock_get_folders, mock_boto_client):
        """Test that main() only crawls catalog when no new folders."""
        # Mock S3 folder query
        mock_get_folders.return_value = ['12100163-trade']

        # Mock Glue client
        mock_glue = MagicMock()
        mock_boto_client.return_value = mock_glue

        # Mock get_tables paginator - folder already has a table
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'TableList': [
                {'Name': 'catalog'},
                {'Name': '12100163_international_trade'}
            ]}
        ]
        mock_glue.get_paginator.return_value = mock_paginator

        # Run main
        crawler.main()

        # Verify update_crawler was called with only catalog target
        call_args = mock_glue.update_crawler.call_args[1]

        # Should only have catalog (no new data folders)
        assert len(call_args['Targets']['S3Targets']) == 1
        assert call_args['Targets']['S3Targets'][0]['Path'] == 's3://build-cananda-dw/statscan/catalog/'

        # Verify crawler was still started (to update catalog)
        mock_glue.start_crawler.assert_called_once_with(Name='statscan')
