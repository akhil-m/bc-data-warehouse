#!/bin/bash
set -e

echo "[$(date)] Starting StatsCan ingestion pipeline"

echo "[$(date)] Step 1/6: Discovering datasets..."
python -m src.statscan.discover

echo "[$(date)] Step 2/6: Comparing with existing catalog..."
python -m src.statscan.compare

echo "[$(date)] Step 3/6: Ingesting datasets..."
python -m src.statscan.ingest

echo "[$(date)] Step 4/6: Uploading to S3..."
python -m src.statscan.upload

echo "[$(date)] Step 5/6: Updating catalog..."
python -m src.statscan.catalog

echo "[$(date)] Step 6/6: Updating crawler..."
python -m src.statscan.crawler

echo "[$(date)] ✓ Pipeline complete"
