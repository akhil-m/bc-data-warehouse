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


def initialize_ingestion_dates(catalog_df):
    """Add last_ingestion_date column if not present.

    Args:
        catalog_df: Catalog DataFrame

    Returns:
        Catalog with last_ingestion_date column (NaT if not present)
    """
    catalog = catalog_df.copy()
    if 'last_ingestion_date' not in catalog.columns:
        catalog['last_ingestion_date'] = pd.NaT
    return catalog


def update_ingestion_dates(catalog_df, ingested_product_ids, ingestion_date):
    """Update last_ingestion_date for successfully ingested datasets.

    Args:
        catalog_df: Catalog DataFrame with last_ingestion_date column
        ingested_product_ids: Set/list of productIds that were ingested
        ingestion_date: Timestamp to record for these datasets

    Returns:
        Catalog with updated last_ingestion_date values
    """
    catalog = catalog_df.copy()
    mask = catalog['productId'].isin(ingested_product_ids)
    catalog.loc[mask, 'last_ingestion_date'] = ingestion_date
    return catalog


def merge_catalog_metadata(fresh_catalog_df, existing_catalog_df):
    """Merge fresh catalog metadata with existing last_ingestion_date.

    Args:
        fresh_catalog_df: Latest catalog from API
            Columns: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints
        existing_catalog_df: Current catalog from S3
            Columns: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints, available, last_ingestion_date

    Returns:
        Merged catalog preserving last_ingestion_date where available
    """
    # Start with fresh catalog (latest metadata)
    merged = fresh_catalog_df.copy()

    # If existing catalog has last_ingestion_date, preserve it
    if len(existing_catalog_df) > 0 and 'last_ingestion_date' in existing_catalog_df.columns:
        ingestion_dates = existing_catalog_df[['productId', 'last_ingestion_date']]
        merged = merged.merge(ingestion_dates, on='productId', how='left')
    else:
        merged['last_ingestion_date'] = pd.NaT

    return merged


# === I/O Layer ===

def main():
    """Update catalog availability flags and ingestion dates."""
    from datetime import datetime
    import os

    # I/O: Download existing catalog from S3 (has last_ingestion_date)
    s3 = boto3.client('s3', region_name='us-east-2')
    try:
        s3.download_file(
            'build-cananda-dw',
            'statscan/catalog/catalog.parquet',
            'existing_catalog.parquet'
        )
        existing_catalog = pd.read_parquet('existing_catalog.parquet')
        print(f'Downloaded existing catalog from S3: {len(existing_catalog)} datasets')
    except Exception as e:
        print(f'No existing catalog in S3 (first run?): {e}')
        existing_catalog = pd.DataFrame()

    # I/O: Load fresh catalog from discover step
    fresh_catalog = pd.read_parquet('catalog.parquet')
    print(f'Fresh catalog from API: {len(fresh_catalog)} datasets')

    # Core: Merge fresh metadata with existing ingestion dates
    catalog = merge_catalog_metadata(fresh_catalog, existing_catalog)

    # Core: Initialize last_ingestion_date column if needed
    catalog = initialize_ingestion_dates(catalog)

    # I/O: Load ingestion manifest to update last_ingestion_date
    if os.path.exists('ingested.csv'):
        manifest = pd.read_csv('ingested.csv')
        ingested_ids = manifest['productId'].tolist()
        print(f'Updating last_ingestion_date for {len(ingested_ids)} ingested datasets')

        # Core: Update ingestion dates
        current_time = datetime.now()
        catalog = update_ingestion_dates(catalog, ingested_ids, current_time)
    else:
        print('No ingested.csv found, skipping last_ingestion_date update')

    # I/O: Get datasets from S3
    existing = utils.get_existing_dataset_ids('statscan')
    print(f'Found {len(existing)} datasets in S3')

    # Core: Update availability
    catalog = enhance_catalog(catalog, existing)

    # I/O: Save locally
    catalog.to_parquet('catalog.parquet', index=False)
    print(f'Updated catalog: {catalog["available"].sum()} available out of {len(catalog)} total')

    # I/O: Upload to S3
    s3.upload_file(
        'catalog.parquet',
        'build-cananda-dw',
        'statscan/catalog/catalog.parquet'
    )
    print('✓ Uploaded to s3://build-cananda-dw/statscan/catalog/catalog.parquet')


if __name__ == '__main__':
    main()
