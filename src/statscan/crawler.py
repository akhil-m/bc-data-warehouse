#!/usr/bin/env python3
"""Update Glue crawler with all dataset folders as separate S3 targets."""

import boto3
from . import utils


# === Functional Core (Pure Functions - No I/O) ===


def extract_product_id_from_table_name(table_name):
    """Extract productId from Glue table name.

    Args:
        table_name: Glue table name like '12100163_international_trade'

    Returns:
        Product ID as int, or None if not a dataset table
    """
    if table_name == 'catalog' or '_' not in table_name:
        return None
    try:
        return int(table_name.split('_')[0])
    except (ValueError, IndexError):
        return None


def find_new_folders(all_folders, existing_tables):
    """Find folders that don't have corresponding Glue tables yet.

    Args:
        all_folders: List of all dataset folder names from S3
        existing_tables: List of Glue table names

    Returns:
        List of folder names that need to be crawled (no table exists yet)
    """
    # Extract productIds from existing tables
    existing_product_ids = set()
    for table_name in existing_tables:
        product_id = extract_product_id_from_table_name(table_name)
        if product_id is not None:
            existing_product_ids.add(product_id)

    # Filter folders to only those without tables
    new_folders = []
    for folder in all_folders:
        # Extract productId from folder name (format: '12100163-title')
        try:
            product_id = int(folder.split('-')[0])
            if product_id not in existing_product_ids:
                new_folders.append(folder)
        except (ValueError, IndexError):
            # Skip malformed folder names
            continue

    return new_folders


def create_s3_targets(folders, bucket_prefix):
    """Create S3 target configurations for Glue crawler.

    Args:
        folders: List of folder names (e.g., ['12100163-trade', '43100050-immigration'])
        bucket_prefix: S3 bucket prefix (e.g., 's3://bucket/path/')

    Returns:
        List of S3 target dicts for Glue crawler API
    """
    return [{"Path": f"{bucket_prefix}{folder}/", "Exclusions": []} for folder in folders]


def create_crawler_update_params(targets, crawler_name, role, database_name):
    """Create parameters for Glue crawler update API call.

    Args:
        targets: List of S3 target dicts
        crawler_name: Name of Glue crawler
        role: IAM role ARN for crawler
        database_name: Glue database name

    Returns:
        Dict of parameters for update_crawler() API call
    """
    return {
        'Name': crawler_name,
        'Role': role,
        'DatabaseName': database_name,
        'Targets': {'S3Targets': targets},
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
    }


# === I/O Layer ===

def main():
    """Main crawler update orchestration."""
    CRAWLER_NAME = "statscan-v3"
    S3_DATA_BUCKET = "s3://build-cananda-dw/statscan/data/"
    S3_CATALOG_BUCKET = "s3://build-cananda-dw/statscan/catalog/"

    # I/O: Create Glue client (used for both querying tables and updating crawler)
    client = boto3.client("glue", region_name="us-east-2")

    # I/O: Get all dataset folders from S3
    all_folders = utils.get_existing_dataset_folders('statscan')
    print(f"Found {len(all_folders)} dataset folders in S3")

    # I/O: Get existing Glue tables
    paginator = client.get_paginator('get_tables')
    existing_tables = []
    for page in paginator.paginate(DatabaseName='statscan'):
        existing_tables.extend([t['Name'] for t in page['TableList']])
    print(f"Found {len(existing_tables)} existing Glue tables")

    # Core: Find folders that don't have tables yet (incremental crawl)
    new_folders = find_new_folders(all_folders, existing_tables)
    print(f"Found {len(new_folders)} new folders to crawl")

    # Core: Create S3 targets for new folders only
    targets = create_s3_targets(new_folders, S3_DATA_BUCKET)

    # Always add catalog folder (it updates frequently)
    targets.append({"Path": S3_CATALOG_BUCKET, "Exclusions": []})

    # Core: Create update parameters
    update_params = create_crawler_update_params(
        targets,
        CRAWLER_NAME,
        "service-role/AWSGlueServiceRole-statscan",
        "statscan"
    )

    # I/O: Update and run crawler via AWS API
    client.update_crawler(**update_params)
    print(f"✓ Updated crawler '{CRAWLER_NAME}' with {len(targets)} S3 targets ({len(new_folders)} new datasets + catalog)")

    # Start the crawler to actually catalog the datasets
    client.start_crawler(Name=CRAWLER_NAME)
    print(f"✓ Started crawler '{CRAWLER_NAME}'")

    if len(new_folders) == 0:
        print("  (Only catalog will be updated, all dataset tables already exist)")


if __name__ == '__main__':
    main()
