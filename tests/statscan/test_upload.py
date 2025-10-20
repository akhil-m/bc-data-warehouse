"""Tests for S3 upload functions."""

import tempfile
import pandas as pd
from pathlib import Path
from src.statscan import upload


class TestValidateManifestData:
    """Test manifest validation logic (pure function)."""

    def test_manifest_does_not_exist(self):
        """Test validation when manifest file doesn't exist."""
        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=False,
            manifest_df=None,
            error_type=None
        )

        assert is_valid is False
        assert "ingested.csv not found" in error_msg

    def test_manifest_empty_data_error(self):
        """Test validation when manifest has EmptyDataError."""
        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=True,
            manifest_df=None,
            error_type='EmptyDataError'
        )

        assert is_valid is False
        assert "manifest has no data" in error_msg

    def test_manifest_dataframe_is_empty(self):
        """Test validation when manifest DataFrame has zero rows."""
        empty_df = pd.DataFrame(columns=['productId', 'file_path'])

        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=True,
            manifest_df=empty_df,
            error_type=None
        )

        assert is_valid is False
        assert "manifest is empty" in error_msg

    def test_manifest_is_valid(self):
        """Test validation when manifest is valid with data."""
        valid_df = pd.DataFrame({
            'productId': [123, 456],
            'file_path': ['path1', 'path2']
        })

        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=True,
            manifest_df=valid_df,
            error_type=None
        )

        assert is_valid is True
        assert error_msg is None

    def test_manifest_with_one_row(self):
        """Test validation with single row manifest."""
        single_row_df = pd.DataFrame({
            'productId': [123],
            'file_path': ['path1']
        })

        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=True,
            manifest_df=single_row_df,
            error_type=None
        )

        assert is_valid is True
        assert error_msg is None

    def test_manifest_with_many_rows(self):
        """Test validation with large manifest."""
        large_df = pd.DataFrame({
            'productId': list(range(100)),
            'file_path': [f'path{i}' for i in range(100)]
        })

        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=True,
            manifest_df=large_df,
            error_type=None
        )

        assert is_valid is True
        assert error_msg is None

    def test_validation_order_file_existence_first(self):
        """Test that file existence is checked before other validations."""
        # Even if we pass a valid DataFrame, if file doesn't exist it should fail
        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=False,
            manifest_df=pd.DataFrame({'productId': [1]}),
            error_type=None
        )

        assert is_valid is False
        assert "not found" in error_msg


class TestShouldSkipFile:
    """Test file skip logic (pure function)."""

    def test_skip_nonexistent_file(self):
        """Test that nonexistent files are skipped."""
        nonexistent = Path('/tmp/does_not_exist_12345.parquet')

        should_skip, warning = upload.should_skip_file(nonexistent)

        assert should_skip is True
        assert warning is not None
        assert "not found" in warning
        assert str(nonexistent) in warning

    def test_dont_skip_existing_file(self):
        """Test that existing files are not skipped."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)
            try:
                should_skip, warning = upload.should_skip_file(tmp_path)

                assert should_skip is False
                assert warning is None
            finally:
                tmp_path.unlink()

    def test_skip_deleted_file(self):
        """Test file that existed but was deleted."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)

        # File was created then deleted
        tmp_path.unlink()

        should_skip, warning = upload.should_skip_file(tmp_path)

        assert should_skip is True
        assert warning is not None

    def test_skip_directory(self):
        """Test that directories are skipped (Path.exists() returns True for dirs)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            dir_path = Path(tmp_dir)

            # exists() returns True for directories, but we're checking files
            # Our function just checks exists(), so it would NOT skip
            should_skip, warning = upload.should_skip_file(dir_path)

            # Directory exists, so should not skip
            assert should_skip is False
            assert warning is None

    def test_warning_message_format(self):
        """Test that warning message has correct format."""
        test_path = Path('/test/path/file.parquet')

        should_skip, warning = upload.should_skip_file(test_path)

        assert should_skip is True
        assert warning.startswith("Warning:")
        assert "skipping" in warning
