#!/usr/bin/env python3
"""Shared utilities for data ingestion workflows."""

import boto3


# === Functional Core ===

def extract_product_id_from_folder(folder_name):
    """Extract productId from S3 folder name.

    Args:
        folder_name: Folder name like '12100163-international-trade'

    Returns:
        Product ID as integer, or None if invalid format
    """
    if '-' in folder_name and folder_name.split('-')[0].isdigit():
        return int(folder_name.split('-')[0])
    return None


# === I/O Layer ===

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

            # Use pure function to extract productId
            product_id = extract_product_id_from_folder(folder)
            if product_id is not None:
                existing.add(product_id)

    return existing


def get_existing_dataset_folders(source='statscan'):
    """Return list of dataset folder names in S3.

    Args:
        source: Data source name (e.g., 'statscan', 'ircc')

    Returns:
        List of folder names like ['12100163-international-trade', ...]
    """
    s3 = boto3.client('s3', region_name='us-east-2')
    bucket = 'build-cananda-dw'
    prefix = f'{source}/data/'

    folders = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        # CommonPrefixes contains folder names like 'statscan/data/12100163-title/'
        for obj in page.get('CommonPrefixes', []):
            folder = obj['Prefix'].rstrip('/').split('/')[-1]
            folders.append(folder)

    return folders
