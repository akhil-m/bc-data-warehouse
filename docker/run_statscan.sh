#!/bin/bash
set -e

echo "[$(date)] Starting StatsCan ingestion pipeline"

echo "[$(date)] Step 1/5: Discovering datasets..."
python -m src.statscan.discover

echo "[$(date)] Step 2/5: Ingesting datasets..."
python -m src.statscan.ingest

echo "[$(date)] Step 3/5: Uploading to S3..."
python -m src.statscan.upload

echo "[$(date)] Step 4/5: Updating catalog..."
python -m src.statscan.catalog

echo "[$(date)] Step 5/5: Updating crawler..."
python -m src.statscan.crawler

echo "[$(date)] ✓ Pipeline complete"
