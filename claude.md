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
- Sanitize column names (replace spaces/slashes/hyphens with underscores)
- Convert to parquet (pandas)
- Store in `data/{productId}-{descriptive-title}/{productId}.parquet`
- Priority datasets first (trade, labour, wages, housing, unemployment)
- 5GB total cap, 1GB per-file download limit
- Upload to S3

### Phase 3: Warehouse ✓
- S3: `s3://build-cananda-dw/statscan/data/` (251 datasets uploaded)
- Glue Crawler: configured with 251 separate S3 targets (one per dataset folder)
  - **Critical:** Crawler must have individual S3 targets to avoid merging tables with >70% schema similarity
  - Use `update_crawler.py` to update crawler with new dataset folders from `dataset_folders.txt`
- Athena: queryable with Presto SQL
  - Database: `statscan`, Region: `us-east-2`
  - Table names contain special chars, require double quotes: `SELECT * FROM "table_name"`
- Upload script: `upload_to_s3.sh` - syncs `data/` folder to S3 (excludes CSV/ZIP files)

### Phase 4: Chat ✓
**Solution: LibreChat + FastMCP**

**ECS Deployment (for testing):**
- LibreChat: deployed on ECS Fargate (service: `librechat-service-uu5q6e84`, cluster: `amused-wolf-oy2wxo`)
- FastMCP server: runs in same ECS task, provides Athena query tool via MCP
- Cloudflared: HTTPS tunnel at https://louisville-clark-santa-households.trycloudflare.com
- Connection: streamable-http on localhost (port 8001)

**Coolify Deployment (production):**
- Docker Hub image: `akhil1710/fastmcp-athena:latest`
- FastMCP runs on port 8001
- LibreChat configured via inline `librechat.yaml` in docker-compose
- MCP connection: `http://fastmcp:8001/mcp`
- AWS credentials passed as environment variables (no IAM role)

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
- 251 datasets ingested locally, cataloged in Glue
- `immigration_catalog.parquet`: 186 immigration-focused datasets extracted from main catalog
- `ingest_all.py` configured to download from immigration catalog (10GB target, 8 workers)
- All datasets stored in `data/{productId}-{title}/` folders

### Chat Agent System Prompt
```
You are a Canadian economic data analyst. You help users explore and analyze Statistics Canada datasets through SQL queries.

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
1. Identify the economic topic (trade, employment, housing, etc.)
2. Find the relevant tables by looking at what tables are available
3. Write precise SQL with appropriate filters
4. Explain results in context
```
