# Build Canada Data Warehouse

StatsCan and IRCC data warehouse with LibreChat + FastMCP for natural language querying via Athena.

## Project Structure

```
src/
  pipeline/          # Data ingestion pipeline
    discover.py      # Discover StatsCan datasets
    ingest_all.py    # Download and convert datasets
    upload_to_s3.py  # Upload to S3
    regenerate_catalog.py  # Update catalog availability
    update_crawler.py      # Update Glue crawler
    utils.py         # S3 utilities
  mcp/              # MCP server for Athena
    athena_mcp_server.py
deploy/             # Deployment configs (Docker, LibreChat)
tests/              # Test suite (69% coverage)
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
python -m src.pipeline.discover

# Ingest datasets (optional LIMIT env var)
LIMIT=5 python -m src.pipeline.ingest_all

# Upload to S3
python -m src.pipeline.upload_to_s3

# Update catalog
python -m src.pipeline.regenerate_catalog

# Update Glue crawler
python -m src.pipeline.update_crawler
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

## Workflows

### Ingestion

Run manually via GitHub Actions:
- Go to Actions → "Ingest StatsCan Data" → Run workflow
- Optional: Set LIMIT to test with small dataset

### CI/CD

- **Local**: Pre-push hook runs tests before push
- **GitHub**: PR checks run full test suite (backup validation)
