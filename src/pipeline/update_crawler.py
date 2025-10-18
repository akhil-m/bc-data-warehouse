#!/usr/bin/env python3
"""Update Glue crawler with all dataset folders as separate S3 targets."""

import boto3


# === Functional Core (Pure Functions - No I/O) ===

def parse_folder_list(file_content):
    """Parse folder list from file content.

    Args:
        file_content: Raw text content with one folder name per line

    Returns:
        List of folder names (stripped, non-empty lines only)
    """
    return [line.strip() for line in file_content.split('\n') if line.strip()]


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
    S3_BUCKET = "s3://build-cananda-dw/statscan/data/"

    # I/O: Read folder list from disk
    with open("dataset_folders.txt") as f:
        file_content = f.read()

    # Core: Parse folders
    folders = parse_folder_list(file_content)
    print(f"Found {len(folders)} dataset folders")

    # Core: Create S3 targets
    targets = create_s3_targets(folders, S3_BUCKET)

    # Core: Create update parameters
    update_params = create_crawler_update_params(
        targets,
        CRAWLER_NAME,
        "service-role/AWSGlueServiceRole-statscan",
        "statscan"
    )

    # I/O: Update crawler via AWS API
    client = boto3.client("glue", region_name="us-east-2")
    client.update_crawler(**update_params)

    print(f"✓ Updated crawler '{CRAWLER_NAME}' with {len(targets)} S3 targets")


if __name__ == '__main__':
    main()
