#!/usr/bin/env python3
"""Regenerate catalog_enhanced.parquet with updated availability."""

import pandas as pd
import boto3
import utils


# === Functional Core (Pure Functions - No I/O) ===

def decode_frequency(freq_code):
    """Map frequency code to human-readable label.

    Args:
        freq_code: Integer frequency code from StatsCan API

    Returns:
        Human-readable frequency label string
    """
    freq_map = {
        1: 'Occasional', 2: 'Biannual', 6: 'Monthly', 9: 'Quarterly',
        11: 'Bimonthly', 12: 'Annual', 13: 'Biennial', 14: 'Triennial',
        15: 'Quinquennial', 16: 'Decennial', 17: 'Every 3 years',
        18: 'Census', 19: 'Every 4 years', 20: 'Every 6 years'
    }
    return freq_map.get(freq_code, 'Unknown')


def enhance_catalog(catalog_df, existing_ids):
    """Add availability and frequency labels to catalog.

    This is the core transformation function that:
    1. Marks which datasets are available in S3
    2. Decodes frequency codes to human-readable labels
    3. Selects only the columns needed for the enhanced catalog

    Args:
        catalog_df: Base catalog DataFrame with all metadata
        existing_ids: Set of productIds already stored in S3

    Returns:
        Enhanced catalog DataFrame with columns:
        - productId, title, frequency_label, releaseTime, available
    """
    catalog = catalog_df.copy()

    # Mark available datasets
    catalog['available'] = catalog['productId'].isin(existing_ids)

    # Decode frequency codes
    catalog['frequency_label'] = catalog['frequency'].apply(decode_frequency)

    # Keep only useful columns
    catalog = catalog[['productId', 'title', 'frequency_label', 'releaseTime', 'available']]

    return catalog


# === I/O Layer ===

def main():
    """Main catalog regeneration orchestration."""
    # I/O: Load base catalog
    catalog = pd.read_parquet('catalog.parquet')

    # I/O: Get datasets from S3
    existing = utils.get_existing_dataset_ids('statscan')
    print(f'Found {len(existing)} datasets in S3')

    # Core: Enhance catalog
    catalog_enhanced = enhance_catalog(catalog, existing)

    # I/O: Save locally
    catalog_enhanced.to_parquet('catalog_enhanced.parquet', index=False)
    print(f'Updated catalog: {catalog_enhanced["available"].sum()} available out of {len(catalog_enhanced)} total')

    # I/O: Upload to S3
    s3 = boto3.client('s3', region_name='us-east-2')
    s3.upload_file(
        'catalog_enhanced.parquet',
        'build-cananda-dw',
        'statscan/data/catalog/catalog_enhanced.parquet'
    )
    print('✓ Uploaded to s3://build-cananda-dw/statscan/data/catalog/catalog_enhanced.parquet')


if __name__ == '__main__':
    main()
