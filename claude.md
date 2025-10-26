The text, and especially the code, you write will be:
- **Concise**
- **Well-named**
- **Exquisitely readable**
- **Simple** (not complex)
- **Deep** (not shallow, per Osterhout)

At its best it is ruthlessly efficient but also elegant, even beautiful.

---

## ⚠️ CRITICAL: ALWAYS USE THE VENV

**NEVER run `python` directly. ALWAYS use `source .venv/bin/activate` first.**

This project has a virtual environment at `.venv/`. You MUST activate it before running any Python commands:

```bash
source .venv/bin/activate
python script.py
```

If you run `python` without activating the venv, the command will fail with "command not found".

---

## ⚠️ MANDATORY: Functional Core / Imperative Shell Architecture

**All new code MUST follow this pattern. No exceptions.**

### The Pattern:
- **Functional Core**: Pure functions with ALL complex logic, edge cases, conditionals
- **Imperative Shell**: Thin I/O layer with minimal logic, just orchestration

### Why:
- Functional Core = 100% testable without mocks (deterministic, pure functions)
- Imperative Shell = Minimal testing burden (thin wrapper)

### Examples from Codebase:

**Functional Core** (Pure, no I/O):
```python
def sanitize_column_names(columns):
    """Pure transformation: list → list"""
    return [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in columns]

def filter_catalog(catalog_df, existing_ids, skip_invisible=True, limit=None):
    """Pure transformation: DataFrame → DataFrame"""
    filtered = catalog_df[~catalog_df['productId'].isin(existing_ids)]
    if skip_invisible:
        filtered = filtered[~filtered['title'].str.contains('INVISIBLE', na=False)]
    if limit:
        filtered = filtered.head(limit)
    return filtered
```

**Imperative Shell** (I/O, orchestration only):
```python
def download_table(product_id, title):
    """I/O layer: HTTP, files, subprocess - NO complex logic"""
    url = f"{API_BASE}/getFullTableDownloadCSV/{product_id}/en"
    resp = requests.get(url)
    data = resp.json()
    # ... file operations, no branching logic
    return size_mb, file_path
```

### Testing Approach:
- **Functional Core**: Direct function calls with real data, NO mocks, 100% coverage
- **Imperative Shell**: Heavy mocking (requests, boto3, subprocess), test orchestration only

### Rules:
1. If it has complex logic → Functional Core
2. If it does I/O → Imperative Shell (extract logic to Core first)
3. Always write tests for new Functional Core functions
4. Keep Shell functions thin enough that testing is trivial

---

## ✅ FC/IS Architecture

**All code follows strict Functional Core / Imperative Shell pattern.**

### Why ~67% Coverage is Expected:

This is the natural result of FC/IS architecture:
- **Functional Core**: 100% tested (all logic, edge cases, calculations)
- **Imperative Shell**: 0% tested (only I/O: HTTP, S3, files, subprocess, print)
- **Weighted average**: ~67% (based on lines of code)

All LOGIC is extracted to pure functions and tested. All UNTESTED code is pure I/O.

---

## StatsCan Data Warehouse Plan

### Phase 1: Discover ✓
- Call `getAllCubesList` API → save to `catalog.parquet`
- Capture metadata: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints
- Output: 7985 datasets

### Phase 2: Ingest ✓
- Fetch CSV via `getFullTableDownloadCSV/{productId}/en` → download ZIP → extract CSV
- Memory-efficient conversion: PyArrow CSV-to-parquet in subprocess (prevents memory accumulation)
- Sanitize column names (spaces/slashes/hyphens → underscores)
- Store as `data/{productId}-{title}/{productId}.parquet`
- Sequential processing (1 worker), uploads to S3
- Each conversion runs in isolated subprocess (memory released on exit)

### Phase 3: Warehouse ✓
- **S3**: `s3://build-cananda-dw/statscan/data/` (3928 datasets, ~10GB)
- **Glue Crawler**: Incremental crawling via `crawler.py` using `statscan-v3` crawler
  - Compares S3 folders vs existing Glue tables
  - Only adds NEW datasets to crawler targets (incremental approach)
  - Always crawls catalog folder (updates availability flags)
  - **IMPORTANT**: AWS Glue has undocumented ~2k target limit per crawler
    - Crawlers with ≥2k targets enter broken "quantum superposition" state (RUNNING/0ms forever)
    - Keep incremental batches under 500 targets to avoid this bug
    - Initial bulk ingestion used 4 temporary crawlers (v3-v6) with 500 targets each
- **Catalog**: `s3://build-cananda-dw/statscan/catalog/catalog.parquet`
  - Columns: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints, available
  - Updated by `catalog.py` based on S3 contents
  - Processed in productId order (starts with small Bank of Canada/government datasets, larger trade/labor/immigration datasets come later)
- **Athena**: Presto SQL, database `statscan` (us-east-2), tables require double quotes
- **Status**: 3929 tables cataloged (3928 datasets + 1 catalog), all queryable via Athena

### Phase 4: Chat ✓
**Coolify Deployment:**
- LibreChat: `ghcr.io/danny-avila/librechat:latest`
- FastMCP: `akhil1710/fastmcp-athena:latest` (port 8001, linux/amd64)
- Config: `librechat.yaml` (v1.3.0), MCP at `http://fastmcp:8001/mcp`
- AWS credentials via environment variables

### Phase 5: Quality & Testing ✓
**Architecture:** Functional Core / Imperative Shell
- All logic extracted to pure functions (100% test coverage on core logic)
- ~67% overall coverage reflects FC/IS: 100% core logic + 0% I/O shell
- Pre-push hook runs tests (`.git/hooks/pre-push`)
- Test tools: pytest + pytest-cov + pytest-mock

### Phase 6: Docker Deployment ✓
**Structure:**
```
src/statscan/    # discover.py, ingest.py, upload.py, catalog.py, crawler.py
src/mcp/         # FastMCP Athena server
docker/          # Dockerfile, docker-compose.yml, run_statscan.sh
tests/statscan/  # Full test suite
```

**Commands:**
```bash
# Test run
LIMIT=5 docker compose -f docker/docker-compose.yml up

# Production run (respects LIMIT env var)
LIMIT=100 docker compose -f docker/docker-compose.yml up
```

**Pipeline:** discover → ingest → upload → catalog → crawler

### Phase 7: IRCC Open Data (Next)
**Source:** https://search.open.canada.ca/opendata/?owner_org=cic

**Available Datasets:**
- Express Entry - Invited Candidates (monthly draws, CRS scores)
- Express Entry - Permanent Residents (outcomes)
- Temporary Residents (work/study permits)
- Asylum Claimants
- Syrian Refugees
- Resettled Refugees

**Implementation Plan:**
- Use CKAN API to discover datasets programmatically
- Download CSV/XLSX files
- Convert to parquet and upload to S3 under `s3://build-cananda-dw/ircc/data/`
- Catalog in Glue alongside StatsCan data
- Single unified Athena database with both StatsCan and IRCC tables

### Chat Agent System Prompt
```
You are a Canadian economic data analyst. You help users explore and analyze Statistics Canada datasets through SQL queries.

Dataset Discovery:
- Query the `catalog` table first to discover available datasets
- Columns: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints, available
- Filter by `available = true` to see which datasets are in the warehouse
- Search titles for keywords related to user's question

Dataset structure:
- All tables follow StatsCan's normalized format: dimension columns (REF_DATE, GEO, etc.) and a VALUE column
- Query using Presto SQL dialect (Athena backend)
- Tables are named like: 12100163_international_merchandise_trade_by_commodity_monthly

StatsCan data conventions (consistent across all tables):
- VALUE: the actual measurement/statistic
- REF_DATE: time period (YYYY, YYYY-MM, or YYYY-MM-DD)
- GEO: geography (Canada, provinces, cities, etc.)
- UOM: unit of measure (Dollars, Percent, Persons, etc.)
- SCALAR_FACTOR: multiplier applied to VALUE (e.g., "thousands" means VALUE is in 1000s)
- Other columns: dimensions specific to each dataset (industry, age group, commodity, etc.)

Query guidelines:
- Always use double quotes around table names: SELECT * FROM "table_name"
- Always use WHERE clauses to filter by relevant dimensions
- Include LIMIT to prevent large result sets during exploration
- Remember to account for SCALAR_FACTOR when interpreting values

When users ask questions:
1. Search catalog table to identify relevant datasets
2. Check if dataset is available (available = true)
3. Query the relevant tables with appropriate filters
4. Explain results in context

Data Coverage:
- StatsCan: 3928 datasets available (complete StatsCan warehouse: trade, labor, immigration, economic indicators, demographics, health, government finance, etc.)
- IRCC Express Entry data: NOT available in StatsCan (managed by IRCC separately)
```
