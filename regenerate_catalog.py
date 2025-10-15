#!/usr/bin/env python3
"""Regenerate catalog_enhanced.parquet with updated availability."""

import pandas as pd
import boto3
import utils


def main():
    # Load base catalog
    catalog = pd.read_parquet('catalog.parquet')

    # Get datasets already in S3
    existing = utils.get_existing_dataset_ids('statscan')
    print(f'Found {len(existing)} datasets in S3')

    # Update available column
    catalog['available'] = catalog['productId'].isin(existing)

    # Decode frequency
    freq_map = {
        1: 'Occasional', 2: 'Biannual', 6: 'Monthly', 9: 'Quarterly',
        11: 'Bimonthly', 12: 'Annual', 13: 'Biennial', 14: 'Triennial',
        15: 'Quinquennial', 16: 'Decennial', 17: 'Every 3 years',
        18: 'Census', 19: 'Every 4 years', 20: 'Every 6 years'
    }
    catalog['frequency_label'] = catalog['frequency'].map(freq_map).fillna('Unknown')

    # Keep only useful columns
    catalog = catalog[['productId', 'title', 'frequency_label', 'releaseTime', 'available']]

    # Save locally
    catalog.to_parquet('catalog_enhanced.parquet', index=False)
    print(f'Updated catalog: {catalog["available"].sum()} available out of {len(catalog)} total')

    # Upload to S3
    s3 = boto3.client('s3', region_name='us-east-2')
    s3.upload_file(
        'catalog_enhanced.parquet',
        'build-cananda-dw',
        'statscan/data/catalog/catalog_enhanced.parquet'
    )
    print('✓ Uploaded to s3://build-cananda-dw/statscan/data/catalog/catalog_enhanced.parquet')


if __name__ == '__main__':
    main()
