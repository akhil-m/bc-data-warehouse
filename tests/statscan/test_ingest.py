"""Tests for ingestion logic."""

import os
import sys
import tempfile
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from unittest.mock import patch, MagicMock
from src.statscan import ingest


class TestLimitHandling:
    """Test LIMIT environment variable handling."""

    @patch('src.statscan.ingest.ThreadPoolExecutor')
    @patch('src.statscan.ingest.as_completed')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_limit_env_var_is_applied(
        self, mock_read_parquet, mock_get_existing, mock_as_completed, mock_executor
    ):
        """Test that LIMIT=5 only processes 5 datasets (THE BUG)."""
        # Create catalog with 100 datasets
        mock_catalog = pd.DataFrame({
            'productId': range(1, 101),
            'title': [f'Dataset {i}' for i in range(1, 101)]
        })
        mock_read_parquet.return_value = mock_catalog
        mock_get_existing.return_value = set()

        # Set LIMIT to 5
        os.environ['LIMIT'] = '5'

        # Track how many datasets were submitted for processing
        submitted_count = 0

        def track_submit(*args, **kwargs):
            nonlocal submitted_count
            submitted_count += 1
            future = MagicMock()
            return future

        mock_executor_instance = MagicMock()
        mock_executor_instance.submit = track_submit
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        # No futures to complete
        mock_as_completed.return_value = []

        # Mock pandas to_csv
        with patch('pandas.DataFrame.to_csv'):
            try:
                ingest.main()
            except SystemExit:
                pass

        # Should only submit 5 datasets for processing
        assert submitted_count == 5, f"Expected 5 datasets, but got {submitted_count}"

        # Cleanup
        del os.environ['LIMIT']

    @patch('src.statscan.ingest.ThreadPoolExecutor')
    @patch('src.statscan.ingest.as_completed')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_no_limit_processes_all_available(
        self, mock_read_parquet, mock_get_existing, mock_as_completed, mock_executor
    ):
        """Test that without LIMIT, all available datasets are processed."""
        mock_catalog = pd.DataFrame({
            'productId': range(1, 11),
            'title': [f'Dataset {i}' for i in range(1, 11)]
        })
        mock_read_parquet.return_value = mock_catalog
        mock_get_existing.return_value = set()

        # Ensure LIMIT is not set
        os.environ.pop('LIMIT', None)

        submitted_count = 0

        def track_submit(*args, **kwargs):
            nonlocal submitted_count
            submitted_count += 1
            return MagicMock()

        mock_executor_instance = MagicMock()
        mock_executor_instance.submit = track_submit
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        mock_as_completed.return_value = []

        with patch('pandas.DataFrame.to_csv'):
            try:
                ingest.main()
            except SystemExit:
                pass

        # Should process all 10 datasets
        assert submitted_count == 10


class TestInvisibleFiltering:
    """Test INVISIBLE dataset filtering."""

    @patch('src.statscan.ingest.ThreadPoolExecutor')
    @patch('src.statscan.ingest.as_completed')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_invisible_datasets_are_filtered(
        self, mock_read_parquet, mock_get_existing, mock_as_completed, mock_executor
    ):
        """Test that INVISIBLE datasets are excluded from processing."""
        mock_catalog = pd.DataFrame({
            'productId': [1, 2, 3, 4],
            'title': [
                'Normal Dataset',
                'INVISIBLE Table',
                'Another Normal Dataset',
                'Some INVISIBLE Data'
            ]
        })
        mock_read_parquet.return_value = mock_catalog
        mock_get_existing.return_value = set()

        submitted_count = 0

        def track_submit(*args, **kwargs):
            nonlocal submitted_count
            submitted_count += 1
            return MagicMock()

        mock_executor_instance = MagicMock()
        mock_executor_instance.submit = track_submit
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        mock_as_completed.return_value = []

        with patch('pandas.DataFrame.to_csv'):
            try:
                ingest.main()
            except SystemExit:
                pass

        # Should only process 2 normal datasets (not the 2 INVISIBLE ones)
        assert submitted_count == 2


class TestExistingDatasetsFiltering:
    """Test that existing datasets in S3 are skipped."""

    @patch('src.statscan.ingest.ThreadPoolExecutor')
    @patch('src.statscan.ingest.as_completed')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_existing_datasets_are_skipped(
        self, mock_read_parquet, mock_get_existing, mock_as_completed, mock_executor
    ):
        """Test that datasets already in S3 are not reprocessed."""
        mock_catalog = pd.DataFrame({
            'productId': [1, 2, 3, 4, 5],
            'title': [f'Dataset {i}' for i in range(1, 6)]
        })
        mock_read_parquet.return_value = mock_catalog

        # Datasets 2 and 4 already exist in S3
        mock_get_existing.return_value = {2, 4}

        submitted_count = 0

        def track_submit(*args, **kwargs):
            nonlocal submitted_count
            submitted_count += 1
            return MagicMock()

        mock_executor_instance = MagicMock()
        mock_executor_instance.submit = track_submit
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        mock_as_completed.return_value = []

        with patch('pandas.DataFrame.to_csv'):
            try:
                ingest.main()
            except SystemExit:
                pass

        # Should only process 3 datasets (1, 3, 5)
        assert submitted_count == 3


class TestProcessDataset:
    """Test dataset processing worker function."""

    @patch('src.statscan.ingest.download_table')
    def test_successful_download_updates_shared_state(self, mock_download):
        """Test that successful downloads update the shared state."""
        import threading

        # download_table returns (size_mb, file_path)
        mock_download.return_value = (15.5, 'path/to/file.parquet')

        state_lock = threading.Lock()
        shared_state = {'total_size_mb': 0, 'ingested': []}

        result = ingest.process_dataset(123, 'Test Dataset', state_lock, shared_state)

        assert result == 15.5
        assert shared_state['total_size_mb'] == 15.5
        assert len(shared_state['ingested']) == 1
        assert shared_state['ingested'][0]['productId'] == 123
        assert shared_state['ingested'][0]['title'] == 'Test Dataset'
        assert shared_state['ingested'][0]['size_mb'] == 15.5
        assert shared_state['ingested'][0]['file_path'] == 'path/to/file.parquet'

    @patch('src.statscan.ingest.download_table')
    def test_skipped_download_returns_none(self, mock_download):
        """Test that skipped files don't update shared state."""
        import threading

        mock_download.return_value = None  # File was skipped

        state_lock = threading.Lock()
        shared_state = {'total_size_mb': 0, 'ingested': []}

        result = ingest.process_dataset(123, 'Test Dataset', state_lock, shared_state)

        assert result is None
        assert shared_state['total_size_mb'] == 0
        assert len(shared_state['ingested']) == 0

    @patch('src.statscan.ingest.download_table')
    def test_error_handling_returns_none(self, mock_download):
        """Test that errors are caught and return None."""
        import threading

        mock_download.side_effect = Exception('Network error')

        state_lock = threading.Lock()
        shared_state = {'total_size_mb': 0, 'ingested': []}

        result = ingest.process_dataset(123, 'Test Dataset', state_lock, shared_state)

        assert result is None
        assert shared_state['total_size_mb'] == 0
        assert len(shared_state['ingested']) == 0


class TestMainManifest:
    """Test manifest file generation."""

    @patch('pandas.DataFrame.to_csv')
    @patch('src.statscan.ingest.ThreadPoolExecutor')
    @patch('src.statscan.ingest.as_completed')
    @patch('src.statscan.utils.get_existing_dataset_ids')
    @patch('pandas.read_parquet')
    def test_manifest_saved_after_ingestion(
        self, mock_read_parquet, mock_get_existing, mock_as_completed, mock_executor, mock_to_csv
    ):
        """Test that manifest CSV is saved after ingestion."""
        mock_catalog = pd.DataFrame({
            'productId': [1],
            'title': ['Dataset 1']
        })
        mock_read_parquet.return_value = mock_catalog
        mock_get_existing.return_value = set()

        # Mock executor
        mock_executor_instance = MagicMock()
        mock_executor_instance.submit = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_as_completed.return_value = []

        # Set LIMIT
        os.environ['LIMIT'] = '1'

        try:
            ingest.main()
        except SystemExit:
            pass

        # Verify manifest was saved
        mock_to_csv.assert_called_once_with('ingested.csv', index=False)

        # Cleanup
        del os.environ['LIMIT']


class TestConvertCsvToParquet:
    """Test CSV to parquet conversion with PyArrow."""

    @patch('src.statscan.ingest.subprocess.run')
    def test_runs_conversion_in_subprocess(self, mock_subprocess_run):
        """Test that conversion runs in isolated subprocess for memory efficiency."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / 'test.csv'
            parquet_path = Path(tmp_dir) / 'test.parquet'

            # Create minimal CSV
            test_data = pd.DataFrame({'col': [1, 2]})
            test_data.to_csv(csv_path, index=False)

            # Call conversion
            ingest.convert_csv_to_parquet(csv_path, parquet_path)

            # Verify subprocess.run was called
            mock_subprocess_run.assert_called_once()
            call_args = mock_subprocess_run.call_args

            # Verify subprocess parameters
            assert call_args[1]['check'] is True
            assert call_args[1]['capture_output'] is True
            assert call_args[0][0][0] == sys.executable
            assert call_args[0][0][1] == '-c'

    def test_converts_csv_to_parquet(self):
        """Test basic CSV to parquet conversion."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create test CSV
            csv_path = Path(tmp_dir) / 'test.csv'
            test_data = pd.DataFrame({
                'REF_DATE': ['2024-01', '2024-02'],
                'GEO': ['Canada', 'Ontario'],
                'VALUE': [100, 200]
            })
            test_data.to_csv(csv_path, index=False)

            # Convert to parquet
            parquet_path = Path(tmp_dir) / 'test.parquet'
            ingest.convert_csv_to_parquet(csv_path, parquet_path)

            # Verify output file exists
            assert parquet_path.exists()

            # Verify data integrity
            result = pq.read_table(parquet_path).to_pandas()
            pd.testing.assert_frame_equal(result, test_data)

    def test_sanitizes_column_names(self):
        """Test that column names with spaces/slashes/hyphens are sanitized."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create CSV with problematic column names
            csv_path = Path(tmp_dir) / 'test.csv'
            test_data = pd.DataFrame({
                'Column Name': [1, 2],
                'Another/Column': [3, 4],
                'With-Hyphen': [5, 6]
            })
            test_data.to_csv(csv_path, index=False)

            # Convert to parquet
            parquet_path = Path(tmp_dir) / 'test.parquet'
            ingest.convert_csv_to_parquet(csv_path, parquet_path)

            # Verify column names are sanitized
            result = pq.read_table(parquet_path)
            expected_columns = ['Column_Name', 'Another_Column', 'With_Hyphen']
            assert result.column_names == expected_columns

    def test_handles_large_columns(self):
        """Test conversion with many columns (StatsCan datasets can have 50+ dimensions)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create CSV with many columns
            csv_path = Path(tmp_dir) / 'test.csv'
            columns = [f'col_{i}' for i in range(50)]
            test_data = pd.DataFrame([[i] * 50 for i in range(10)], columns=columns)
            test_data.to_csv(csv_path, index=False)

            # Convert to parquet
            parquet_path = Path(tmp_dir) / 'test.parquet'
            ingest.convert_csv_to_parquet(csv_path, parquet_path)

            # Verify all columns present
            result = pq.read_table(parquet_path)
            assert len(result.column_names) == 50
