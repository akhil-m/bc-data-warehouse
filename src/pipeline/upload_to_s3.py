#!/usr/bin/env python3
"""Upload StatsCan datasets to S3."""

import boto3
from pathlib import Path


BUCKET = 'build-cananda-dw'
PREFIX = 'statscan/data/'
DATA_DIR = 'data/'
EXCLUDE_EXTENSIONS = {'.csv', '.zip', '.DS_Store'}


def should_upload(file_path):
    """Check if file should be uploaded (exclude CSV/ZIP files).

    Args:
        file_path: Path object

    Returns:
        True if file should be uploaded, False otherwise
    """
    return file_path.suffix not in EXCLUDE_EXTENSIONS and file_path.name not in EXCLUDE_EXTENSIONS


def upload_datasets():
    """Upload all parquet files from data/ to S3."""
    s3 = boto3.client('s3', region_name='us-east-2')
    data_path = Path(DATA_DIR)

    if not data_path.exists():
        print(f"Error: {DATA_DIR} directory not found")
        return

    print(f"Uploading datasets from {DATA_DIR} to s3://{BUCKET}/{PREFIX}")
    print(f"Excluding: {', '.join(EXCLUDE_EXTENSIONS)}")
    print()

    uploaded = 0
    for file_path in data_path.rglob('*'):
        if file_path.is_file() and should_upload(file_path):
            # Relative path from data/ directory
            rel_path = file_path.relative_to(data_path)
            s3_key = f"{PREFIX}{rel_path}"

            print(f"Uploading {rel_path}")
            s3.upload_file(str(file_path), BUCKET, s3_key)
            uploaded += 1

    print()
    print(f"✓ Upload complete ({uploaded} files)")


if __name__ == '__main__':
    upload_datasets()
