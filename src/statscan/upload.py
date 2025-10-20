#!/usr/bin/env python3
"""Upload StatsCan datasets to S3."""

import boto3
import pandas as pd
from pathlib import Path


BUCKET = 'build-cananda-dw'
PREFIX = 'statscan/data/'
DATA_DIR = 'data/'
MANIFEST_FILE = 'ingested.csv'


# === Functional Core (Pure Functions - No I/O) ===

def validate_manifest_data(manifest_exists, manifest_df=None, error_type=None):
    """Validate manifest file data and return validation result.

    Args:
        manifest_exists: Whether the manifest file exists (bool)
        manifest_df: DataFrame loaded from manifest (None if error occurred)
        error_type: Type of error that occurred during read (str or None)

    Returns:
        Tuple of (is_valid: bool, error_message: str or None)
    """
    if not manifest_exists:
        return False, "No new datasets to upload (ingested.csv not found)"

    if error_type == 'EmptyDataError':
        return False, "No new datasets to upload (manifest has no data)"

    if manifest_df is not None and len(manifest_df) == 0:
        return False, "No new datasets to upload (manifest is empty)"

    return True, None


def should_skip_file(file_path):
    """Determine if file should be skipped during upload.

    Args:
        file_path: Path object to check

    Returns:
        Tuple of (should_skip: bool, warning_message: str or None)
    """
    if not file_path.exists():
        return True, f"Warning: {file_path} not found, skipping"
    return False, None


# === I/O Layer ===

def upload_datasets():
    """Upload newly ingested parquet files from data/ to S3."""
    # I/O: Check file existence and load manifest
    manifest_exists = Path(MANIFEST_FILE).exists()
    manifest_df = None
    error_type = None

    if manifest_exists:
        try:
            manifest_df = pd.read_csv(MANIFEST_FILE)
        except pd.errors.EmptyDataError:
            error_type = 'EmptyDataError'

    # Core: Validate manifest data
    is_valid, error_message = validate_manifest_data(manifest_exists, manifest_df, error_type)

    if not is_valid:
        print(error_message)
        return

    manifest = manifest_df

    print(f"Uploading {len(manifest)} newly ingested datasets to s3://{BUCKET}/{PREFIX}")
    print()

    s3 = boto3.client('s3', region_name='us-east-2')
    uploaded = 0

    for _, row in manifest.iterrows():
        file_path = Path(DATA_DIR) / row['file_path']
        s3_key = f"{PREFIX}{row['file_path']}"

        # Core: Check if file should be skipped
        should_skip, warning_msg = should_skip_file(file_path)
        if should_skip:
            print(warning_msg)
            continue

        print(f"Uploading {row['file_path']}")
        s3.upload_file(str(file_path), BUCKET, s3_key)
        uploaded += 1

    print()
    print(f"✓ Upload complete ({uploaded} files)")


if __name__ == '__main__':
    upload_datasets()
