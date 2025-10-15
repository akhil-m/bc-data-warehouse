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
- Capture metadata: subject, frequency, dimensions, release time
- Score datasets by interestingness (frequency, recency, dimensions)
- Output: 7985 datasets ranked by score

### Phase 2: Ingest ✓
**Script: `ingest_all.py`**
- Fetch CSV via `getFullTableDownloadCSV/{productId}/en` → download ZIP → extract CSV
- **Memory-efficient streaming**: Stream ZIP to temp file, extract to disk, only hold DataFrame in memory
- Sanitize column names (replace spaces/slashes/hyphens with underscores)
- Convert to parquet (pandas)
- Store in `data/{productId}-{descriptive-title}/{productId}.parquet`
- Sequential processing (1 worker) to prevent memory exhaustion with large files
- 10GB target, 5GB per-file limit
- Upload to S3

### Phase 3: Warehouse ✓
- S3: `s3://build-cananda-dw/statscan/data/` (268 datasets uploaded)
- Glue Crawler: configured with 269 separate S3 targets (268 datasets + catalog)
  - **Critical:** Crawler must have individual S3 targets to avoid merging tables with >70% schema similarity
  - Use `update_crawler.py` to update crawler with new dataset folders from `dataset_folders.txt`
- Catalog: `s3://build-cananda-dw/statscan/catalog/catalog_enhanced.parquet`
  - Columns: productId, title, frequency_label, releaseTime, available
  - Queryable in Athena for dataset discovery
- Athena: queryable with Presto SQL
  - Database: `statscan`, Region: `us-east-2`
  - Table names contain special chars, require double quotes: `SELECT * FROM "table_name"`
- Upload script: `upload_to_s3.sh` - syncs `data/` folder to S3 (excludes CSV/ZIP files)

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
- Was running on ECS Fargate - being shut down to reduce costs
- Replaced by Coolify deployment

**Files:**
- `athena_mcp_server.py`: FastMCP server with `query()` tool (port 8001)
- `Dockerfile.fastmcp`: Container for FastMCP server
- `librechat.yaml`: MCP server configuration (mounted into LibreChat)
- `librechat-task-definition-updated.json`: ECS task definition (3 containers: librechat, fastmcp, cloudflared)

### Phase 5: Scale Up ✓
**Data Quality Findings:**
- StatsCan API provides normalized columnar data (not formatted reports)
- All parquet files have consistent schema: REF_DATE, GEO, UOM, SCALAR_FACTOR, VALUE + dataset-specific dimensions
- Schema overlap 70-79% across datasets (why Glue crawler initially merged them)

**Current Status:**
- 268 datasets ingested locally (268/7985 = 3.4% of catalog)
- Focus: immigration-related datasets from `immigration_catalog.parquet` (186 total)
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
- Query the `catalog_enhanced` table first to discover available datasets
- Columns: productId, title, frequency_label, releaseTime, available
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
1. Search catalog_enhanced table to identify relevant datasets
2. Check if dataset is available (available = true)
3. Query the relevant tables with appropriate filters
4. Explain results in context

Data Coverage:
- StatsCan: 268 datasets available (immigration outcomes, economic indicators, demographics)
- IRCC Express Entry data: NOT available in StatsCan (managed by IRCC separately)
```
