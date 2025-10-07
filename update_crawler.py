#!/usr/bin/env python3
"""Update Glue crawler with all 251 dataset folders as separate S3 targets."""

import boto3

CRAWLER_NAME = "statscan"
S3_BUCKET = "s3://build-cananda-dw/statscan/data/"

with open("dataset_folders.txt") as f:
    folders = [line.strip() for line in f if line.strip()]

print(f"Found {len(folders)} dataset folders")

targets = [{"Path": f"{S3_BUCKET}{folder}/", "Exclusions": []} for folder in folders]

client = boto3.client("glue", region_name="us-east-2")

client.update_crawler(
    Name=CRAWLER_NAME,
    Role="service-role/AWSGlueServiceRole-statscan",
    DatabaseName="statscan",
    Targets={"S3Targets": targets},
    SchemaChangePolicy={
        "UpdateBehavior": "UPDATE_IN_DATABASE",
        "DeleteBehavior": "DEPRECATE_IN_DATABASE"
    }
)

print(f"✓ Updated crawler '{CRAWLER_NAME}' with {len(targets)} S3 targets")
