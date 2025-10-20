# Build Canada Data Warehouse

StatsCan and IRCC data warehouse with LibreChat + FastMCP for natural language querying via Athena.

## Project Structure

```
src/
  statscan/         # StatsCan data pipeline
    discover.py     # Discover datasets via API
    ingest.py       # Download and convert to parquet
    upload.py       # Upload to S3
    catalog.py      # Update catalog availability
    crawler.py      # Update Glue crawler
    utils.py        # S3 utilities
  mcp/              # MCP server for Athena
    athena_mcp_server.py
docker/             # Docker deployment (Dockerfile, docker-compose.yml)
tests/              # Test suite (FC/IS architecture)
hooks/              # Git hooks (pre-push)
```

## Setup

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas boto3 requests pyarrow
pip install -r requirements-dev.txt
```

### 2. Install git hooks (required)

Enable pre-push hook to run tests before pushing:

```bash
ln -s ../../hooks/pre-push .git/hooks/pre-push
```

This runs tests locally before pushing to catch issues immediately (1-2 seconds vs waiting for CI).

### 3. Configure AWS credentials

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-2
```

## Development

### Running pipeline scripts

All pipeline scripts are Python modules:

```bash
# Discover datasets
python -m src.statscan.discover

# Ingest datasets (optional LIMIT env var)
LIMIT=5 python -m src.statscan.ingest

# Upload to S3
python -m src.statscan.upload

# Update catalog
python -m src.statscan.catalog

# Update Glue crawler
python -m src.statscan.crawler
```

### Running tests

```bash
pytest --cov=src --cov-report=term-missing --cov-fail-under=64
```

Coverage threshold: 64% (71 tests, pure functions 100% covered)

### Architecture

This codebase follows the **Functional Core / Imperative Shell** pattern:

- **Functional core**: Pure functions with no I/O (100% test coverage)
- **Imperative shell**: I/O operations and orchestration (integration tested with mocks)

Example:
```python
# Functional core (pure, testable)
def filter_catalog(catalog_df, existing_ids, limit=None):
    filtered = catalog_df[~catalog_df['productId'].isin(existing_ids)]
    if limit:
        filtered = filtered.head(limit)
    return filtered

# Imperative shell (I/O)
def main():
    catalog = pd.read_parquet('catalog.parquet')  # I/O
    existing = utils.get_existing_dataset_ids('statscan')  # I/O
    filtered = filter_catalog(catalog, existing)  # Pure
    # ... more I/O
```

### Pre-push hook

The pre-push hook automatically runs tests before every push. To bypass (use sparingly):

```bash
git push --no-verify
```

## Docker Deployment

### Quick start (recommended)

Use the helper script to rebuild and run the pipeline:

```bash
# Test with limited datasets
./run-docker.sh 5

# Production run (all datasets)
./run-docker.sh

# Help
./run-docker.sh --help
```

The script automatically rebuilds the Docker image and runs the full pipeline.

### Manual Docker commands

If you need more control, use docker compose directly:

```bash
# Rebuild image
docker compose -f docker/docker-compose.yml build

# Run pipeline with limit
LIMIT=5 docker compose -f docker/docker-compose.yml up

# Run pipeline (all datasets)
docker compose -f docker/docker-compose.yml up
```

### Pipeline steps

The pipeline executes in sequence:
1. `discover` - Fetch catalog from StatsCan API
2. `ingest` - Download and convert CSVs to parquet
3. `upload` - Upload to S3
4. `catalog` - Update catalog availability
5. `crawler` - Sync Glue crawler with S3

### Environment variables

Set via `.env` or inline:
- `LIMIT` - Number of datasets to process (optional, for testing)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` - AWS credentials
