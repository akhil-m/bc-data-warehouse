#!/usr/bin/env python3
"""Shared utilities for data ingestion workflows."""

import boto3


def get_existing_dataset_ids(source='statscan'):
    """Return set of productIds already in S3.

    Args:
        source: Data source name (e.g., 'statscan', 'ircc')

    Returns:
        Set of integer productIds found in S3
    """
    s3 = boto3.client('s3', region_name='us-east-2')
    bucket = 'build-cananda-dw'
    prefix = f'{source}/data/'

    existing = set()
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        # CommonPrefixes contains folder names like 'statscan/data/12100163-title/'
        for obj in page.get('CommonPrefixes', []):
            folder = obj['Prefix'].rstrip('/').split('/')[-1]

            # Extract productId (everything before first dash)
            if '-' in folder and folder.split('-')[0].isdigit():
                product_id = int(folder.split('-')[0])
                existing.add(product_id)

    return existing
