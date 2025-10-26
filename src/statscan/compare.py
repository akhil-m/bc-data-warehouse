#!/usr/bin/env python3
"""Compare fresh catalog with existing to identify datasets for processing."""

import os
from datetime import datetime
import pandas as pd
import boto3
from .update_detection import identify_datasets_for_processing


def download_existing_catalog():
    """Download existing catalog from S3.

    Returns:
        DataFrame with existing catalog, or empty DataFrame if not found
    """
    try:
        s3 = boto3.client('s3', region_name='us-east-2')
        s3.download_file(
            'build-cananda-dw',
            'statscan/catalog/catalog.parquet',
            'existing_catalog.parquet'
        )
        return pd.read_parquet('existing_catalog.parquet')
    except Exception as e:
        print(f"No existing catalog in S3 (first run?): {e}")
        return pd.DataFrame()


def main():
    """Identify datasets needing processing."""
    # I/O: Load fresh catalog from discover step
    fresh_catalog = pd.read_parquet('catalog.parquet')
    print(f"Fresh catalog: {len(fresh_catalog)} datasets")

    # I/O: Download existing catalog from S3
    existing_catalog = download_existing_catalog()
    if len(existing_catalog) > 0:
        print(f"Existing catalog: {len(existing_catalog)} datasets")
    else:
        print("No existing catalog found (first run)")

    # Core: Identify datasets needing processing
    current_date = datetime.now()
    datasets_to_process = identify_datasets_for_processing(
        fresh_catalog,
        existing_catalog,
        current_date
    )

    # Count before limiting
    new_count_before = (datasets_to_process['reason'] == 'new').sum()
    update_count = (datasets_to_process['reason'] == 'update_due').sum()

    # I/O: Read LIMIT from environment
    limit = int(os.getenv('LIMIT')) if os.getenv('LIMIT') else None

    # Core: Apply limit to NEW datasets only, always process ALL updates
    from .update_detection import apply_limit_to_new_datasets
    datasets_to_process = apply_limit_to_new_datasets(datasets_to_process, limit)

    # Print summary
    new_count_after = (datasets_to_process['reason'] == 'new').sum()
    if limit is not None:
        print(f"\nLIMIT={limit} applied:")
        print(f"  - New datasets: {new_count_after} of {new_count_before}")
        print(f"  - Updates due: {update_count} (all processed)")
    else:
        print(f"\nNo LIMIT:")
        print(f"  - New datasets: {new_count_before}")
        print(f"  - Updates due: {update_count}")

    # I/O: Save filtered catalog for ingest step
    datasets_to_process.to_parquet('catalog_filtered.parquet', index=False)
    print(f"\n✓ Processing {len(datasets_to_process)} datasets total")
    print("✓ Saved to catalog_filtered.parquet")


if __name__ == '__main__':
    main()
