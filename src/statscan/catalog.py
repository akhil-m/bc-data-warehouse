#!/usr/bin/env python3
"""Update catalog availability flags based on S3 contents."""

import pandas as pd
import boto3
from . import utils


# === Functional Core (Pure Functions - No I/O) ===

def enhance_catalog(catalog_df, existing_ids):
    """Update available column based on what's in S3.

    Args:
        catalog_df: Catalog DataFrame with all columns
        existing_ids: Set of productIds already stored in S3

    Returns:
        Catalog with updated available flags (all columns preserved)
    """
    catalog = catalog_df.copy()
    catalog['available'] = catalog['productId'].isin(existing_ids)
    return catalog


# === I/O Layer ===

def main():
    """Update catalog availability flags."""
    # I/O: Load catalog (downloaded from S3 in previous workflow step)
    catalog = pd.read_parquet('catalog.parquet')

    # I/O: Get datasets from S3
    existing = utils.get_existing_dataset_ids('statscan')
    print(f'Found {len(existing)} datasets in S3')

    # Core: Update availability
    catalog_updated = enhance_catalog(catalog, existing)

    # I/O: Save locally
    catalog_updated.to_parquet('catalog.parquet', index=False)
    print(f'Updated catalog: {catalog_updated["available"].sum()} available out of {len(catalog_updated)} total')

    # I/O: Upload to S3
    s3 = boto3.client('s3', region_name='us-east-2')
    s3.upload_file(
        'catalog.parquet',
        'build-cananda-dw',
        'statscan/catalog/catalog.parquet'
    )
    print('✓ Uploaded to s3://build-cananda-dw/statscan/catalog/catalog.parquet')


if __name__ == '__main__':
    main()
