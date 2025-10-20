"""Tests for S3 upload functions."""

import pandas as pd
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

    def test_validation_order_empty_data_error_before_length(self):
        """Test that EmptyDataError is checked before DataFrame length."""
        is_valid, error_msg = upload.validate_manifest_data(
            manifest_exists=True,
            manifest_df=None,
            error_type='EmptyDataError'
        )

        assert is_valid is False
        assert "has no data" in error_msg
