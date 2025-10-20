"""Tests for Glue crawler update functions."""

from unittest.mock import MagicMock, patch
from src.statscan import crawler


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
    def test_main_orchestration(self, mock_get_folders, mock_boto_client):
        """Test that main() calls all functions in correct order."""
        # Mock S3 folder query
        mock_get_folders.return_value = ['12100163-trade', '43100050-immigration']

        # Mock Glue client
        mock_glue = MagicMock()
        mock_boto_client.return_value = mock_glue

        # Run main
        crawler.main()

        # Verify S3 was queried
        mock_get_folders.assert_called_once_with('statscan')

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

        # Verify crawler was started after update
        mock_glue.start_crawler.assert_called_once_with(Name='statscan')
