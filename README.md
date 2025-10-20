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
pip install -r requirements-dev.txt
```

### 2. Install git hooks (required)

Enable pre-push hook to run tests before pushing:

```bash
ln -s ../../hooks/pre-push .git/hooks/pre-push
```

This runs tests locally before pushing to catch issues immediately.

### 3. Configure AWS credentials

Create a `.env` file or export directly:

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
pytest --cov=src --cov-report=term-missing --cov-fail-under=70
```

### Architecture

This codebase follows the **Functional Core / Imperative Shell** pattern:

- **Functional core**: Pure functions with all logic, conditionals, edge cases (100% test coverage)
- **Imperative shell**: I/O orchestration only - HTTP, S3, files, subprocess, print (no tests needed - too thin to have bugs)

Example from `src/statscan/ingest.py`:
```python
# Functional core (pure, testable)
def sanitize_column_names(columns):
    """Replace spaces, slashes, hyphens with underscores."""
    return [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in columns]

# Imperative shell (I/O only)
def download_table(product_id, title):
    """Download CSV from StatsCan API, convert to parquet."""
    url = f"{API_BASE}/getFullTableDownloadCSV/{product_id}/en"
    resp = requests.get(url, headers=headers, timeout=60)
    data = resp.json()
    zip_url = data['object']

    # ... HTTP download, ZIP extraction, file I/O ...

    # Use pure function for column transformation
    df.columns = sanitize_column_names(df.columns)

    # ... more I/O: write parquet, upload to S3 ...
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

- `LIMIT` - Number of datasets to process (optional, for testing)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` - AWS credentials
