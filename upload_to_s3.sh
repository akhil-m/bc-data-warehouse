#!/bin/bash
# Upload StatsCan datasets to S3

BUCKET="s3://build-cananda-dw/statscan/data/"
DATA_DIR="data/"

echo "Uploading datasets from ${DATA_DIR} to ${BUCKET}"
echo "Excluding: *.csv, *.zip"
echo ""

aws s3 sync "${DATA_DIR}" "${BUCKET}" \
  --exclude "*.csv" \
  --exclude "*.zip" \
  --exclude ".DS_Store"

echo ""
echo "✓ Upload complete"
