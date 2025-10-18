"""Tests for S3 upload functions."""

from pathlib import Path
from src.pipeline import upload_to_s3


class TestShouldUpload:
    """Test file upload filtering logic."""

    def test_allows_parquet_files(self):
        file_path = Path('data/12100163-trade/12100163.parquet')
        assert upload_to_s3.should_upload(file_path) is True

    def test_excludes_csv_files(self):
        file_path = Path('data/12100163-trade/12100163.csv')
        assert upload_to_s3.should_upload(file_path) is False

    def test_excludes_zip_files(self):
        file_path = Path('data/12100163-trade/12100163.zip')
        assert upload_to_s3.should_upload(file_path) is False

    def test_excludes_ds_store(self):
        file_path = Path('data/.DS_Store')
        assert upload_to_s3.should_upload(file_path) is False

    def test_allows_other_file_types(self):
        file_path = Path('data/notes.txt')
        assert upload_to_s3.should_upload(file_path) is True

    def test_allows_files_without_extension(self):
        file_path = Path('data/README')
        assert upload_to_s3.should_upload(file_path) is True
