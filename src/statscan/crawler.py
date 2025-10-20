#!/usr/bin/env python3
"""Update Glue crawler with all dataset folders as separate S3 targets."""

import boto3
from . import utils


# === Functional Core (Pure Functions - No I/O) ===


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
    CRAWLER_NAME = "statscan"
    S3_DATA_BUCKET = "s3://build-cananda-dw/statscan/data/"
    S3_CATALOG_BUCKET = "s3://build-cananda-dw/statscan/catalog/"

    # I/O: Get folder list from S3
    folders = utils.get_existing_dataset_folders('statscan')
    print(f"Found {len(folders)} dataset folders")

    # Core: Create S3 targets for data folders
    targets = create_s3_targets(folders, S3_DATA_BUCKET)

    # Add catalog folder as additional target
    targets.append({"Path": S3_CATALOG_BUCKET, "Exclusions": []})

    # Core: Create update parameters
    update_params = create_crawler_update_params(
        targets,
        CRAWLER_NAME,
        "service-role/AWSGlueServiceRole-statscan",
        "statscan"
    )

    # I/O: Update and run crawler via AWS API
    client = boto3.client("glue", region_name="us-east-2")
    client.update_crawler(**update_params)
    print(f"✓ Updated crawler '{CRAWLER_NAME}' with {len(targets)} S3 targets ({len(folders)} data folders + 1 catalog)")

    # Start the crawler to actually catalog the datasets
    client.start_crawler(Name=CRAWLER_NAME)
    print(f"✓ Started crawler '{CRAWLER_NAME}' (catalog will be updated)")


if __name__ == '__main__':
    main()
