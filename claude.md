The text, and especially the code, you write will be:
- **Concise**
- **Well-named**
- **Exquisitely readable**
- **Simple** (not complex)
- **Deep** (not shallow, per Osterhout)

At its best it is ruthlessly efficient but also elegant, even beautiful.

---

## StatsCan Data Warehouse Plan

### Phase 1: Discover ✓
**Script: `discover.py`**
- Call `getAllCubesList` API → save to `catalog.parquet`
- Capture metadata: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints, score
- Output: 7985 datasets
- Upload to S3: `s3://build-cananda-dw/statscan/catalog/catalog.parquet`

### Phase 2: Ingest ✓
**Script: `ingest_all.py`**
- Fetch CSV via `getFullTableDownloadCSV/{productId}/en` → download ZIP → extract CSV
- **Memory-efficient streaming**: Stream ZIP to temp file, extract to disk, only hold DataFrame in memory
- Sanitize column names (replace spaces/slashes/hyphens with underscores)
- Convert to parquet (pandas)
- Store in `data/{productId}-{descriptive-title}/{productId}.parquet`
- Sequential processing (1 worker) to prevent memory exhaustion with large files
- 10GB target, 5GB per-file limit (600s download timeout)
- Upload to S3 via `upload_to_s3.sh`
- **Ingestion modes:**
  - Manual: Run `python ingest_all.py` with optional `LIMIT` env var
  - Automated: GitHub Actions workflow `statscan-ingest.yml` (manual trigger with LIMIT input)

### Phase 3: Warehouse ✓
- S3: `s3://build-cananda-dw/statscan/data/` (268 datasets uploaded)
- Glue Crawler: configured with 269 separate S3 targets (268 datasets + catalog)
  - **Critical:** Crawler must have individual S3 targets to avoid merging tables with >70% schema similarity
  - Use `update_crawler.py` to update crawler with new dataset folders from `dataset_folders.txt`
- Catalog: `s3://build-cananda-dw/statscan/catalog/catalog.parquet`
  - **Single unified catalog** (no base/enhanced split)
  - Columns: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints, score, available
  - `available` flag updated by `regenerate_catalog.py` based on S3 contents
  - Queryable in Athena for dataset discovery
- Athena: queryable with Presto SQL
  - Database: `statscan`, Region: `us-east-2`
  - Table names contain special chars, require double quotes: `SELECT * FROM "table_name"`

### Phase 4: Chat ✓
**Solution: LibreChat + FastMCP**

**Coolify Deployment (production):**
- LibreChat: `ghcr.io/danny-avila/librechat:latest` (production image, not dev)
- FastMCP: `akhil1710/fastmcp-athena:latest` (Docker Hub, linux/amd64)
- FastMCP runs on port 8001
- LibreChat configured via `librechat.yaml` (version 1.3.0, mounted from Coolify)
- MCP connection: `http://fastmcp:8001/mcp` (Docker network)
- AWS credentials passed as environment variables (no IAM role)
- **Key fixes:**
  - Built amd64 image (not arm64) for cloud deployment
  - Removed logs volume to fix permission issues
  - Config version 1.3.0 required for MCP support

**ECS Deployment (decommissioned):**
- Was running on ECS Fargate - shut down to reduce costs
- Replaced by Coolify deployment

**Files:**
- `athena_mcp_server.py`: FastMCP server with `query()` tool (port 8001)
- `Dockerfile.fastmcp`: Container for FastMCP server
- `librechat.yaml`: MCP server configuration (mounted into LibreChat)
- `docker-compose.yml`: Local development setup (no hardcoded credentials)

### Phase 5: Quality & Testing ✓
**Architecture: Functional Core / Imperative Shell**
- Refactored all scripts to separate pure functions (testable) from I/O operations
- Pure functions: 100% test coverage
- I/O layer: integration tested with mocks

**Test Infrastructure:**
- pytest + pytest-cov + pytest-mock
- 65 tests, 69% coverage overall
- Coverage threshold: 69% (CI fails below this)
- Files: `tests/test_*.py` for all modules

**CI/CD:**
- **Local validation:** Pre-push hook (`.git/hooks/pre-push` symlinked from `hooks/pre-push`)
  - Runs tests before every push (1-2 seconds)
  - Prevents pushing broken code
  - Bypass with `git push --no-verify`
- **GitHub Actions:**
  - `test.yml`: Run tests on PRs
  - `statscan-ingest.yml`: Manual workflow for ingestion with optional LIMIT

**Refactored Modules:**
- `ingest_all.py`: Pure functions (sanitize_column_names, create_folder_name, should_download, filter_catalog)
- `regenerate_catalog.py`: Pure function (enhance_catalog) - simplified to only update `available` flags
- `update_crawler.py`: Pure functions (parse_folder_list, create_s3_targets, create_crawler_update_params)
- `utils.py`: Pure function (extract_product_id_from_folder)

**Data Quality Findings:**
- StatsCan API provides normalized columnar data (not formatted reports)
- All parquet files have consistent schema: REF_DATE, GEO, UOM, SCALAR_FACTOR, VALUE + dataset-specific dimensions
- Schema overlap 70-79% across datasets (why Glue crawler initially merged them)

**Current Status:**
- 268 datasets ingested (268/7985 = 3.4% of catalog)
- Using full catalog (switched from immigration subset)
- All datasets stored in `data/{productId}-{title}/` folders
- Uploaded to S3 and cataloged in Glue Athena

**Data Gaps:**
- StatsCan does NOT have Express Entry data (CRS scores, draws, invitations)
- Express Entry selection process data managed by IRCC, not StatsCan
- StatsCan focuses on immigration outcomes (who came, income, settlement), not selection process

### Phase 6: IRCC Open Data (Next)
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
- Columns: productId, title, subject, frequency, releaseTime, dimensions, nbDatapoints, score, available
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
- StatsCan: 268 datasets available (immigration outcomes, economic indicators, demographics)
- IRCC Express Entry data: NOT available in StatsCan (managed by IRCC separately)
```
