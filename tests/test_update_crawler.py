"""Tests for Glue crawler update functions."""

from unittest.mock import MagicMock, patch, mock_open
from src.pipeline import update_crawler


class TestParseFolderList:
    """Test folder list parsing logic."""

    def test_parses_normal_list(self):
        content = "folder1\nfolder2\nfolder3"
        result = update_crawler.parse_folder_list(content)
        assert result == ['folder1', 'folder2', 'folder3']

    def test_strips_whitespace(self):
        """Test that leading/trailing whitespace is removed."""
        content = "  folder1  \n\tfolder2\t\n   folder3   "
        result = update_crawler.parse_folder_list(content)
        assert result == ['folder1', 'folder2', 'folder3']

    def test_filters_empty_lines(self):
        """Test that empty lines are removed."""
        content = "folder1\n\n\nfolder2\n  \nfolder3"
        result = update_crawler.parse_folder_list(content)
        assert result == ['folder1', 'folder2', 'folder3']

    def test_handles_mixed_whitespace_and_empty_lines(self):
        content = "  folder1  \n\n  \n  folder2\n\t\nfolder3  "
        result = update_crawler.parse_folder_list(content)
        assert result == ['folder1', 'folder2', 'folder3']

    def test_empty_content(self):
        """Test that completely empty content returns empty list."""
        content = "\n\n  \n  "
        result = update_crawler.parse_folder_list(content)
        assert result == []

    def test_single_folder(self):
        content = "single-folder"
        result = update_crawler.parse_folder_list(content)
        assert result == ['single-folder']

    def test_handles_dataset_folder_format(self):
        """Test with real dataset folder names."""
        content = "12100163-international-trade\n43100050-immigrant-income\n98100524-languages"
        result = update_crawler.parse_folder_list(content)
        assert result == [
            '12100163-international-trade',
            '43100050-immigrant-income',
            '98100524-languages'
        ]


class TestCreateS3Targets:
    """Test S3 target creation logic."""

    def test_creates_single_target(self):
        folders = ['12100163-trade']
        result = update_crawler.create_s3_targets(folders, 's3://bucket/data/')

        assert len(result) == 1
        assert result[0] == {
            'Path': 's3://bucket/data/12100163-trade/',
            'Exclusions': []
        }

    def test_creates_multiple_targets(self):
        folders = ['12100163-trade', '43100050-immigration']
        result = update_crawler.create_s3_targets(folders, 's3://bucket/data/')

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
        result = update_crawler.create_s3_targets([], 's3://bucket/data/')
        assert result == []

    def test_preserves_folder_names(self):
        """Test that folder names are used exactly as provided."""
        folders = ['folder-with-dashes', 'folder_with_underscores', '12345-numeric']
        result = update_crawler.create_s3_targets(folders, 's3://bucket/')

        assert result[0]['Path'] == 's3://bucket/folder-with-dashes/'
        assert result[1]['Path'] == 's3://bucket/folder_with_underscores/'
        assert result[2]['Path'] == 's3://bucket/12345-numeric/'

    def test_handles_different_bucket_prefixes(self):
        """Test with various bucket prefix formats."""
        folders = ['dataset']

        # With trailing slash
        result1 = update_crawler.create_s3_targets(folders, 's3://bucket/prefix/')
        assert result1[0]['Path'] == 's3://bucket/prefix/dataset/'

        # Without trailing slash (should still work)
        result2 = update_crawler.create_s3_targets(folders, 's3://bucket/prefix')
        assert result2[0]['Path'] == 's3://bucket/prefixdataset/'


class TestCreateCrawlerUpdateParams:
    """Test crawler update parameter creation."""

    def test_creates_correct_structure(self):
        """Test that all required parameters are present."""
        targets = [{'Path': 's3://bucket/folder/'}]
        result = update_crawler.create_crawler_update_params(
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
        result = update_crawler.create_crawler_update_params(
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
        result = update_crawler.create_crawler_update_params(
            targets, 'crawler', 'role', 'db'
        )

        assert result['Targets']['S3Targets'] == targets
        assert len(result['Targets']['S3Targets']) == 3

    def test_handles_empty_targets(self):
        """Test with empty target list."""
        targets = []
        result = update_crawler.create_crawler_update_params(
            targets, 'crawler', 'role', 'db'
        )

        assert result['Targets']['S3Targets'] == []


class TestMainIntegration:
    """Test main() orchestration with mocked I/O."""

    @patch('boto3.client')
    @patch('builtins.open', new_callable=mock_open, read_data='12100163-trade\n43100050-immigration\n')
    def test_main_orchestration(self, mock_file, mock_boto_client):
        """Test that main() calls all functions in correct order."""
        # Mock Glue client
        mock_glue = MagicMock()
        mock_boto_client.return_value = mock_glue

        # Run main
        update_crawler.main()

        # Verify file was opened
        mock_file.assert_called_once_with('dataset_folders.txt')

        # Verify Glue client was created
        mock_boto_client.assert_called_once_with('glue', region_name='us-east-2')

        # Verify update_crawler was called with correct parameters
        mock_glue.update_crawler.assert_called_once()
        call_args = mock_glue.update_crawler.call_args[1]

        assert call_args['Name'] == 'statscan'
        assert call_args['DatabaseName'] == 'statscan'
        assert call_args['Role'] == 'service-role/AWSGlueServiceRole-statscan'
        assert len(call_args['Targets']['S3Targets']) == 2
        assert call_args['Targets']['S3Targets'][0]['Path'] == 's3://build-cananda-dw/statscan/data/12100163-trade/'
        assert call_args['Targets']['S3Targets'][1]['Path'] == 's3://build-cananda-dw/statscan/data/43100050-immigration/'
