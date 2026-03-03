# Chapter 9: Database & External Sources

> **Duration:** 32 minutes | **Difficulty:** Intermediate | **Format:** YAML, Python & CLI

Learn to load data from PostgreSQL, MySQL-compatible databases (StarRocks), and Iceberg tables into your Seeknal pipeline.

---

## What You'll Build

Four source nodes, each loading from a different external system:

```
PostgreSQL (warehouse)  →  source.pg_customers       (table scan)
PostgreSQL (incremental)→  source.events             (watermark tracking)
PostgreSQL (pushdown)   →  source.pg_active_orders   (server-side query)
StarRocks (MySQL)       →  source.sr_daily_metrics   (analytics DB)
Iceberg (Lakekeeper)    →  source.ice_events         (lakehouse)
```

**After this chapter, you'll have:**
- Understanding of profile-based connection management
- PostgreSQL sources with table scan, pushdown queries, and incremental detection
- StarRocks sources via MySQL protocol
- Iceberg sources via REST catalog
- Python `@source` equivalents for each type

---

## Prerequisites

Before starting, ensure you have:

- [ ] [Chapter 1: File Sources](1-file-sources.md) — Basic `draft → dry-run → apply` workflow
- [ ] [Chapter 8: Python Pipelines](8-python-pipelines.md) — `@source` decorator basics
- [ ] Access to at least one external database (PostgreSQL recommended)

---

## Part 1: Connection Profiles (5 minutes)

### Why Profiles?

Hard-coding database credentials in YAML files is insecure and inflexible. Seeknal uses **connection profiles** to centralize credentials with environment variable interpolation.

### Create a Profile

Create `profiles.yml` in your project root (or `~/.seeknal/profiles.yml` for global access):

```yaml
# profiles.yml
connections:
  warehouse_pg:
    type: postgresql
    host: ${PG_HOST:localhost}         # env var with fallback
    port: ${PG_PORT:5432}
    user: ${PG_USER:postgres}
    password: ${PG_PASSWORD}           # required — no fallback
    database: ${PG_DATABASE:my_warehouse}
    schema: public
    sslmode: prefer
    connect_timeout: 10

  analytics_sr:
    type: starrocks
    host: ${STARROCKS_HOST:localhost}
    port: 9030                         # MySQL protocol port
    user: ${STARROCKS_USER:root}
    password: ${STARROCKS_PASSWORD}
    database: analytics_db

source_defaults:
  postgresql:                          # default for all PG sources
    connection: warehouse_pg
    schema: public

  iceberg:                             # default for all Iceberg sources
    catalog_uri: ${LAKEKEEPER_URL:http://localhost:8181}
    warehouse: ${LAKEKEEPER_WAREHOUSE:seeknal-warehouse}
```

!!! warning "Use Canonical Type Names"
    Profile keys must use canonical names: `postgresql` (not `postgres`), `starrocks` (not `mysql`). The `source_defaults` section does **not** normalize aliases.

### Environment Variable Interpolation

Profiles support three formats:

| Syntax | Behavior |
|--------|----------|
| `${VAR}` | Required — errors if `VAR` not set |
| `${VAR:default}` | Uses `default` if `VAR` not set |
| `$VAR` | Same as `${VAR}` (no default) |

Set your environment variables before running the pipeline:

```bash
export PG_HOST=localhost
export PG_PASSWORD=my_secret_password
export PG_DATABASE=my_warehouse
```

### Profile Loading Priority

Seeknal resolves profiles in this order (first match wins):

1. `--profile` CLI flag (explicit override)
2. `profiles-{env}.yml` in project root (for `seeknal env`)
3. `~/.seeknal/profiles-{env}.yml` (user home, env-specific)
4. `~/.seeknal/profiles.yml` (user home, default)

**Checkpoint:** Create a `profiles.yml` with at least a PostgreSQL connection. Verify env vars are set.

---

## Part 2: PostgreSQL Sources (8 minutes)

### Table Scan Source

The simplest way to load from PostgreSQL — scan an entire table:

```bash
seeknal draft source pg_customers
```

Edit `draft_source_pg_customers.yml`:

```yaml
kind: source
name: pg_customers
description: "Customer records from PostgreSQL warehouse"
source: postgresql
table: public.customers            # schema.table format
connection: warehouse_pg           # from profiles.yml
columns:
  customer_id: "Unique customer identifier"
  name: "Customer full name"
  email: "Email address"
  created_at: "Account creation timestamp"
  region: "Geographic region"
```

```bash
seeknal dry-run draft_source_pg_customers.yml --profile profiles.yml
seeknal apply draft_source_pg_customers.yml
```

**How it works under the hood:**

1. Seeknal resolves `connection: warehouse_pg` from your profile
2. DuckDB loads the `postgres` extension
3. Attaches the PostgreSQL database: `ATTACH '{libpq_string}' AS pg_pg_customers (TYPE postgres)`
4. Creates a view: `SELECT * FROM pg_pg_customers.public.customers`

### Pushdown Query Source

For large tables, use a **pushdown query** to filter data server-side before it reaches DuckDB:

```bash
seeknal draft source pg_active_orders
```

Edit `draft_source_pg_active_orders.yml`:

```yaml
kind: source
name: pg_active_orders
description: "Active orders from last 90 days — filtered on PostgreSQL"
source: postgresql
connection: warehouse_pg
params:
  query: |
    SELECT
        order_id,
        customer_id,
        order_total,
        status,
        created_at
    FROM public.orders
    WHERE status = 'active'
    AND created_at > NOW() - INTERVAL '90 days'
```

```bash
seeknal dry-run draft_source_pg_active_orders.yml --profile profiles.yml
seeknal apply draft_source_pg_active_orders.yml
```

!!! tip "When to Use Pushdown Queries"
    Use pushdown queries when:

    - The table has millions of rows but you only need a subset
    - You want to filter, aggregate, or join on the remote server
    - You need to reduce network transfer

    The query runs **on PostgreSQL** and only the result is loaded into DuckDB.

!!! danger "Pushdown Query Rules"
    - Queries **must** start with `SELECT` or `WITH` (case-insensitive)
    - DDL/DML is rejected: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`
    - Do **not** set both `table:` and `params.query:` simultaneously

### Python Equivalent

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "pyarrow"]
# ///

from seeknal.pipeline import source

@source(
    name="pg_customers",
    source="postgresql",
    table="public.customers",
    connection="warehouse_pg",
)
def pg_customers(ctx=None):
    """Load customer records from PostgreSQL."""
    pass
```

For pushdown queries in Python:

```python
@source(
    name="pg_active_orders",
    source="postgresql",
    connection="warehouse_pg",
    query="SELECT * FROM public.orders WHERE status = 'active'",
)
def pg_active_orders(ctx=None):
    pass
```

**Checkpoint:** Run `seeknal plan` — you should see `pg_customers` and `pg_active_orders` in the DAG.

---

## Part 2.5: PostgreSQL Incremental Detection (7 minutes)

### What is Incremental Detection?

When processing large PostgreSQL tables on a schedule, loading the entire table every run is wasteful. **Incremental detection** automatically tracks the maximum value of a timestamp column (watermark) and only loads new rows on subsequent runs.

**Benefits:**

- ⚡ **Faster execution** — Only load rows added since last run
- 💰 **Lower costs** — Reduce network transfer and DuckDB memory
- 🔄 **Automatic tracking** — Watermark persisted in `run_state.json`

### How It Works

1. **First run:** Full table scan, maximum timestamp stored as watermark
2. **Subsequent runs:**
   - Compare current max timestamp with stored watermark
   - If unchanged: **Skip the source** (no new data)
   - If changed: Inject `WHERE time_column > 'watermark'` and load only new rows
3. **Full refresh:** Use `--full` flag to ignore watermark and load everything

### Enable Incremental Detection

Add a `freshness` config with `time_column` to your PostgreSQL source:

```yaml
# seeknal/sources/events.yml
kind: source
name: events
description: "Raw events from PostgreSQL with incremental detection"
source: postgresql
table: public.events
connection: warehouse_pg
freshness:
  time_column: created_at        # Column to track for incremental reads
```

### Run Behavior Examples

**Run 1: Initial Load**

```bash
seeknal run --profile profiles.yml
```

- Loads all rows from `public.events`
- Stores watermark: `pg_last_watermark: "2026-03-03 10:00:00"`
- `run_state.json` records the maximum `created_at`

**Run 2: No Changes (Skip Optimization)**

```bash
seeknal run --profile profiles.yml
```

- Detects watermark unchanged (max `created_at` still `2026-03-03 10:00:00`)
- **Skips** the `source.events` node entirely
- Downstream transforms are also skipped (cached)

**Run 3: New Data Inserted**

```sql
-- New row inserted into PostgreSQL
INSERT INTO events (event_type, payload, created_at)
VALUES ('signup', '{"plan": "pro"}', '2026-03-04 10:00:00');
```

```bash
seeknal run --profile profiles.yml
```

- Detects watermark changed (new max is `2026-03-04 10:00:00`)
- Injects WHERE clause: `WHERE created_at > '2026-03-03 10:00:00'`
- Loads only the 1 new row
- Updates watermark to `2026-03-04 10:00:00`

**Run 4: Full Refresh**

```bash
seeknal run --profile profiles.yml --full
```

- Ignores stored watermark
- Loads all rows (no WHERE clause injection)
- Recalculates and updates watermark

### Custom Queries Disable Incremental

When you use `params.query`, incremental detection is automatically disabled because Seeknal cannot safely modify your custom SQL:

```yaml
kind: source
name: purchase_events
source: postgresql
connection: warehouse_pg
freshness:
  time_column: created_at        # Has no effect with custom query
params:
  query: "SELECT * FROM events WHERE event_type = 'purchase'"
```

- No WHERE clause injection (your query runs as-is)
- No watermark tracking (would be inaccurate for filtered data)
- Every run executes the full query

### View Watermark State

Check `target/run_state.json` after each run:

```json
{
  "nodes": {
    "source.events": {
      "metadata": {
        "pg_last_watermark": "2026-03-04 10:00:00",
        "pg_time_column": "created_at",
        "row_count": 4
      }
    }
  }
}
```

### Python Equivalent

```python
from seeknal.pipeline import source

@source(
    name="events",
    source="postgresql",
    table="public.events",
    connection="warehouse_pg",
    freshness={"time_column": "created_at"},  # Enable incremental
)
def events(ctx=None):
    """Load events incrementally based on created_at watermark."""
    pass
```

### When to Use Incremental Detection

| Scenario | Recommendation |
|----------|----------------|
| Append-only tables (logs, events) | ✅ Use incremental with `time_column` |
| Large tables with frequent inserts | ✅ Use incremental |
| Tables with updates/deletes | ⚠️ Use with caution (may miss updates) |
| Custom filtered queries | ❌ Incremental disabled automatically |
| Ad-hoc analysis | ❌ Use `--full` for complete data |

!!! tip "Best Practices"
    - Choose a **monotonically increasing** column (timestamps, auto-increment IDs)
    - Use `created_at` for append-only tables, not `updated_at`
    - Monitor `run_state.json` to verify watermark updates
    - Use `--full` for backfills or when you need complete data

**Checkpoint:** Add `freshness.time_column` to your PostgreSQL source and run twice — observe skip behavior on the second run.

---

## Part 3: StarRocks / MySQL Sources (5 minutes)

### StarRocks via MySQL Protocol

StarRocks exposes a MySQL-compatible interface on port 9030. Seeknal uses `pymysql` to connect:

```bash
seeknal draft source sr_daily_metrics
```

Edit `draft_source_sr_daily_metrics.yml`:

```yaml
kind: source
name: sr_daily_metrics
description: "Daily aggregated metrics from StarRocks"
source: starrocks
table: analytics_db.daily_metrics
params:
  host: "${STARROCKS_HOST}"
  port: 9030
  user: "${STARROCKS_USER}"
  password: "${STARROCKS_PASSWORD}"
  database: "analytics_db"
```

With a custom query:

```yaml
kind: source
name: sr_recent_metrics
description: "Last 7 days of metrics from StarRocks"
source: starrocks
table: analytics_db.daily_metrics
params:
  host: "${STARROCKS_HOST}"
  port: 9030
  user: "${STARROCKS_USER}"
  password: "${STARROCKS_PASSWORD}"
  database: "analytics_db"
  query: |
    SELECT *
    FROM daily_metrics
    WHERE metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
```

```bash
seeknal dry-run draft_source_sr_daily_metrics.yml
seeknal apply draft_source_sr_daily_metrics.yml
```

!!! note "pymysql Required"
    StarRocks sources require `pymysql`. Install with:
    ```bash
    pip install pymysql
    # or
    uv pip install pymysql
    ```

**How it works:**

1. `pymysql` connects to StarRocks on port 9030 (MySQL protocol)
2. Executes the query (or `SELECT * FROM table`)
3. Fetches results into a pandas DataFrame
4. Registers the DataFrame as a DuckDB view

### Connection Differences vs PostgreSQL

| Aspect | PostgreSQL | StarRocks |
|--------|-----------|-----------|
| **Protocol** | libpq (native) | MySQL (port 9030) |
| **DuckDB integration** | `ATTACH` + `postgres_query()` | pymysql → pandas → DuckDB view |
| **Pushdown** | Runs on PostgreSQL server | Runs on StarRocks server |
| **Extension** | DuckDB `postgres` extension | Python `pymysql` package |
| **Profile key** | `postgresql` | `starrocks` |

---

## Part 4: Iceberg Sources (7 minutes)

### Iceberg via Lakekeeper REST Catalog

Iceberg tables are accessed through a REST catalog (e.g., [Lakekeeper](https://lakekeeper.io/)) with OAuth2 authentication:

```bash
seeknal draft source ice_events
```

Edit `draft_source_ice_events.yml`:

```yaml
kind: source
name: ice_events
description: "Signal events from Iceberg lakehouse"
source: iceberg
table: atlas.events.signals          # 3-part: catalog.namespace.table
params:
  catalog_uri: "http://lakekeeper:8181"
  warehouse: "seeknal-warehouse"
```

```bash
seeknal dry-run draft_source_ice_events.yml
seeknal apply draft_source_ice_events.yml
```

### Required Environment Variables

Iceberg sources need S3 and OAuth2 credentials set as environment variables:

```bash
# S3 / MinIO credentials
export AWS_ENDPOINT_URL=http://minio:9000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=your_secret_key

# OAuth2 credentials (Keycloak)
export KEYCLOAK_TOKEN_URL=http://keycloak:8080/realms/iceberg/protocol/openid-connect/token
export KEYCLOAK_CLIENT_ID=seeknal
export KEYCLOAK_CLIENT_SECRET=your_client_secret
```

### Table Name Format

Iceberg tables **must** use 3-part format: `catalog.namespace.table`

```yaml
# ✅ Correct — 3 parts
table: atlas.events.signals

# ❌ Wrong — only 2 parts
table: events.signals

# ❌ Wrong — only 1 part
table: signals
```

### Using source_defaults

Instead of repeating `catalog_uri` and `warehouse` in every source, configure defaults in your profile:

```yaml
# profiles.yml
source_defaults:
  iceberg:
    catalog_uri: ${LAKEKEEPER_URL:http://localhost:8181}
    warehouse: ${LAKEKEEPER_WAREHOUSE:seeknal-warehouse}
```

Then your YAML source only needs the table name:

```yaml
kind: source
name: ice_events
source: iceberg
table: atlas.events.signals
# catalog_uri and warehouse resolved from source_defaults
```

### How It Works

1. DuckDB installs and loads the `iceberg` and `httpfs` extensions
2. Configures S3 credentials (endpoint, access key, secret key)
3. Obtains an OAuth2 bearer token from Keycloak
4. Attaches the Lakekeeper REST catalog: `ATTACH '...' AS atlas (TYPE ICEBERG, ENDPOINT '...', TOKEN '...')`
5. Creates a view: `SELECT * FROM atlas.events.signals`

!!! warning "DuckDB HUGEINT for Iceberg"
    DuckDB's `COUNT(*)` and `SUM()` return `HUGEINT`, which Iceberg doesn't support. If you write transforms that materialize to Iceberg, always cast:

    ```sql
    SELECT
        customer_id,
        CAST(COUNT(*) AS BIGINT) AS order_count,
        CAST(SUM(amount) AS DOUBLE) AS total_amount
    FROM orders
    GROUP BY customer_id
    ```

---

## Source Type Comparison

| Source | `source:` value | Connection | Table Format | Pushdown? |
|--------|----------------|------------|-------------|-----------|
| **CSV** | `csv` | File path | `data/file.csv` | No |
| **Parquet** | `parquet` | File path | `data/file.parquet` | No |
| **JSONL** | `jsonl` | File path | `data/file.jsonl` | No |
| **PostgreSQL** | `postgresql` | Profile or inline | `schema.table` | Yes (`params.query`) |
| **StarRocks** | `starrocks` | Inline params | `database.table` | Yes (`params.query`) |
| **Iceberg** | `iceberg` | Env vars + params | `catalog.namespace.table` | No |

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Connection profile not found**

    - Symptom: `Connection profile 'my_pg' not found`
    - Fix: Ensure `profiles.yml` exists and contains the connection. Use `--profile profiles.yml` if not in the default location (`~/.seeknal/profiles.yml`).

    **2. Environment variable not set**

    - Symptom: `Environment variable 'PG_PASSWORD' is not set`
    - Fix: `export PG_PASSWORD=my_secret` before running. Use `${VAR:default}` syntax for optional vars.

    **3. Iceberg table not 3-part format**

    - Symptom: `Iceberg table must be 3-part format 'catalog.namespace.table'`
    - Fix: Use the full `catalog.namespace.table` format (e.g., `atlas.events.signals`).

    **4. pymysql not installed**

    - Symptom: `pymysql is required for StarRocks sources`
    - Fix: `pip install pymysql` or `uv pip install pymysql`.

    **5. OAuth2 token failure for Iceberg**

    - Symptom: `Iceberg source requires OAuth2 credentials`
    - Fix: Set `KEYCLOAK_TOKEN_URL`, `KEYCLOAK_CLIENT_ID`, and `KEYCLOAK_CLIENT_SECRET` environment variables. Verify Keycloak is reachable.

    **6. source_defaults key not normalized**

    - Symptom: PostgreSQL defaults not applied silently
    - Fix: Use canonical type name `postgresql` as the key in `source_defaults:`, not `postgres`.

    **7. Both table and query specified**

    - Symptom: `Cannot specify both 'table' and 'query' in params`
    - Fix: For pushdown queries, put the query in `params.query` only. Remove or leave `table:` as documentation.

    **8. Incremental detection with non-monotonic column**

    - Symptom: Missed rows when using `updated_at` for incremental
    - Fix: Use a monotonically increasing column like `created_at` or auto-increment ID. Avoid `updated_at` for append-only logic.

    **9. Incremental not working with custom query**

    - Symptom: Watermark not tracked, full data loaded every run
    - Fix: Incremental detection is **automatically disabled** for custom queries (`params.query`). This is expected behavior — the custom query runs as-is.

    **10. Stale watermark after data deletion**

    - Symptom: No new rows loaded after deleting recent data
    - Fix: Run with `--full` flag to reset watermark, or manually edit `target/run_state.json` to clear `pg_last_watermark`.

---

## Summary

In this chapter, you learned:

- [x] **Connection Profiles** — Centralize credentials in `profiles.yml` with env var interpolation
- [x] **PostgreSQL Sources** — Table scan (`table: schema.table`), pushdown query (`params.query`), and incremental detection (`freshness.time_column`)
- [x] **Incremental Detection** — Automatic watermark tracking, WHERE clause injection, skip optimization, and `--full` refresh
- [x] **StarRocks Sources** — MySQL protocol via pymysql (port 9030)
- [x] **Iceberg Sources** — REST catalog with OAuth2 and S3 credentials
- [x] **source_defaults** — Default connection settings per source type in profiles

**Source Configuration Reference:**

| Config | PostgreSQL | StarRocks | Iceberg |
|--------|-----------|-----------|---------|
| **Profile key** | `postgresql` | `starrocks` | `iceberg` |
| **Connection** | `connection: name` | inline `params:` | env vars + `params:` |
| **Table format** | `schema.table` | `database.table` | `catalog.namespace.table` |
| **Pushdown** | `params.query` | `params.query` | Not supported |
| **Incremental** | `freshness.time_column` | Not supported | See Chapter 10 |
| **DuckDB mechanism** | `ATTACH` + postgres ext | pymysql → pandas → view | `ATTACH` + iceberg ext |

**Key Commands:**
```bash
seeknal draft source <name>                           # Generate source template
seeknal dry-run <file>.yml --profile profiles.yml    # Preview with profile
seeknal apply <file>.yml                              # Apply to project
seeknal run --profile profiles.yml                    # Execute with profile
seeknal repl                                          # Query results
```

---

## What's Next?

Now that you can connect to external databases and Iceberg catalogs, learn how to **process Iceberg data incrementally** — detecting changes via snapshots, loading only new rows with watermark tracking, and cascading selectively through mixed-source pipelines.

**[Chapter 10: Iceberg Incremental Processing →](10-iceberg-incremental.md)**

---

## See Also

- **[Chapter 1: File Sources](1-file-sources.md)** — CSV, JSONL, Parquet sources
- **[Chapter 8: Python Pipelines](8-python-pipelines.md)** — `@source` and `@transform` decorators
- **[Data Engineer Path](../data-engineer-path/)** — ELT pipelines with environment management
