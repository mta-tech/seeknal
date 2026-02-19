# Apache Iceberg Materialization

This guide covers using Apache Iceberg table format for materializing Seeknal pipeline outputs with ACID transactions, time travel, and incremental updates.

## Overview

Apache Iceberg is an open table format for huge analytic datasets. Iceberg adds tables to compute engines like Spark, Trino, Flink, and DuckDB using a high-performance table format that works just like a SQL table.

### Why Iceberg?

- **ACID Transactions**: Atomic commits with rollback
- **Time Travel**: Query data as it was at any point in time
- **Schema Evolution**: Add, remove, or modify columns without rewriting data
- **Hidden Partitioning**: Partition evolution without rewriting data
- **Compatibility**: Works with DuckDB, Spark, Trino, and more

### Use Cases

- **Pipeline Output Materialization**: Persist pipeline results to production tables
- **Incremental Updates**: Append new data without rewriting existing data
- **Data Quality Rollback**: Revert to previous snapshot if bad data is detected
- **Schema Evolution**: Add new columns to tables as requirements change
- **Compliance & Auditing**: Query historical data for regulatory requirements

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Seeknal Pipeline                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐             │
│  │  Source    │→ │ Transform  │→ │Feature Grp │             │
│  └────────────┘  └────────────┘  └────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Materialization Decorator                     │
│  - Checks if enabled in profiles.yml                           │
│  - Validates schema compatibility                               │
│  - Handles mode (append/overwrite)                             │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  DuckDB + Iceberg Extension                    │
│  - Loads Iceberg extension                                      │
│  - Creates/updates Iceberg table                                │
│  - Writes to S3/GCS/Azure                                       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Lakekeeper REST Catalog                     │
│  - Manages table metadata                                        │
│  - Tracks snapshots                                             │
│  - Handles schema evolution                                     │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Object Storage (S3/GCS)                       │
│  - Parquet data files                                           │
│  - Manifest files                                               │
│  - Snapshot metadata                                            │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start (Lakekeeper + MinIO)

This section shows a verified end-to-end setup using Lakekeeper REST catalog with MinIO S3-compatible storage.

### Prerequisites

| Component | Purpose | Default Address |
|-----------|---------|-----------------|
| Lakekeeper | Iceberg REST catalog | `http://localhost:8181` |
| MinIO | S3-compatible object storage | `http://localhost:9000` |
| Keycloak | OAuth2 authentication (optional) | `http://localhost:8080` |

### 1. Set Environment Variables

```bash
# Lakekeeper catalog
export LAKEKEEPER_URI=http://localhost:8181

# MinIO/S3 credentials
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=your-minio-password
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_REGION=us-east-1

# OAuth2 (if Lakekeeper uses Keycloak)
export KEYCLOAK_TOKEN_URL=http://localhost:8080/realms/master/protocol/openid-connect/token
export KEYCLOAK_CLIENT_ID=duckdb
export KEYCLOAK_CLIENT_SECRET=your-client-secret
```

### 2. Create profiles.yml

```yaml
# profiles.yml (project root or ~/.seeknal/profiles.yml)
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: target/dev.duckdb

materialization:
  enabled: true
  catalog:
    type: rest
    uri: http://localhost:8181
    warehouse: s3://my-bucket/warehouse
    verify_tls: false                    # Set true for production
  default_mode: append
  schema_evolution:
    mode: auto
    allow_column_add: true
    allow_column_type_change: false
  duckdb:
    iceberg_extension: true
    threads: 4
    memory_limit: 1GB
```

### 3. Add Materialization to YAML Nodes

```yaml
# seeknal/sources/customers.yml
kind: source
name: customers
source: csv
table: "customers.csv"
materialization:
  enabled: true
  mode: overwrite
  table: atlas.my_namespace.customers    # 3-part name: catalog.namespace.table
```

### 4. Run the Pipeline

```bash
seeknal run
```

### 5. Verify Data in Iceberg

```bash
seeknal iceberg profile-show --profile profiles.yml
```

## Configuration

### profiles.yml Setup

Create a `profiles.yml` file in your project root or `~/.seeknal/`:

```yaml
materialization:
  enabled: true
  catalog:
    type: rest
    uri: ${LAKEKEEPER_URI}              # e.g., http://lakekeeper.example.com:8181
    warehouse: s3://my-bucket/warehouse  # S3/GCS/Azure path
    verify_tls: true                     # Enable TLS verification (default)
  default_mode: append                   # or "overwrite"

  schema_evolution:
    mode: safe                           # safe, auto, or strict
    allow_column_add: true               # Allow adding new columns
    allow_column_type_change: false      # Allow type changes
    allow_column_drop: false             # Allow dropping columns

  duckdb:
    iceberg_extension: true
    threads: 4                           # Number of DuckDB threads
    memory_limit: 1GB                    # DuckDB memory limit
```

> **Note:** When using `seeknal iceberg profile-show`, you must pass `--profile profiles.yml` explicitly if using a local project file. The default looks at `~/.seeknal/profiles.yml`.

### Environment Variables

Set credentials as environment variables (recommended):

```bash
# Lakekeeper REST catalog
export LAKEKEEPER_URI=http://lakekeeper.example.com:8181
export LAKEKEEPER_WAREHOUSE=s3://my-bucket/warehouse
export LAKEKEEPER_WAREHOUSE_ID=your-warehouse-uuid   # From /catalog/v1/config endpoint

# MinIO/S3 credentials
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_REGION=us-east-1

# OAuth2/Keycloak (if using authenticated catalog)
export KEYCLOAK_TOKEN_URL=http://localhost:8080/realms/master/protocol/openid-connect/token
export KEYCLOAK_CLIENT_ID=duckdb
export KEYCLOAK_CLIENT_SECRET=your-client-secret
```

### Per-Node Override

Override global settings for specific nodes in YAML:

```yaml
kind: source
name: orders
materialization:
  enabled: true
  mode: overwrite                        # Override default mode
  table: atlas.my_namespace.orders       # 3-part name required
```

### Table Naming Convention

Materialization table names **must** use 3-part format:

```
catalog.namespace.table
```

| Part | Description | Example |
|------|-------------|---------|
| `catalog` | DuckDB catalog alias (always `atlas`) | `atlas` |
| `namespace` | Iceberg namespace in Lakekeeper | `production`, `staging` |
| `table` | Table name | `customers`, `orders` |

Examples:
- `atlas.production.customers`
- `atlas.staging.order_enriched`
- `atlas.iceberg_test.daily_metrics`

> **Important:** The namespace must exist in Lakekeeper before tables can be created. See [Lakekeeper Setup](#lakekeeper-setup) below.

## Schema Evolution Modes

### SAFE Mode (Default)

Conservative approach that prevents breaking changes:

- ✅ Allow: Adding new columns (nullable)
- ❌ Block: Type changes (even safe conversions like int→long)
- ❌ Block: Dropping columns
- ❌ Block: Renaming columns

```yaml
schema_evolution:
  mode: safe
  allow_column_add: true
```

### AUTO Mode

Allow safe automatic schema changes:

- ✅ Allow: Adding new columns
- ✅ Allow: Safe type conversions (int→long, float→double)
- ❌ Block: Unsafe type changes (string→int)
- ❌ Block: Dropping columns

```yaml
schema_evolution:
  mode: auto
  allow_column_add: true
  allow_column_type_change: true
```

### STRICT Mode

Manual approval required for any changes:

- All schema changes require manual intervention
- Useful for production environments with strict governance

```yaml
schema_evolution:
  mode: strict
```

## Write Modes

### OVERWRITE Mode

Replace entire table contents:

```yaml
materialization:
  mode: overwrite
```

**Use cases:**
- Full refresh of dimension tables
- Correcting bad data
- Rebuilding from scratch

**Warning:** Deletes all existing data before writing new data.

### APPEND Mode (Default)

Add new data without modifying existing:

```yaml
materialization:
  mode: append
```

**Use cases:**
- Incremental pipeline runs
- Time-series data ingestion
- Event streaming

**Note:** Requires schema compatibility with existing data.

## CLI Commands

### Validate Configuration

Check if your Iceberg configuration is valid:

```bash
seeknal iceberg validate-materialization
seeknal iceberg validate-materialization --profile custom-profiles.yml
```

Validates:
- Profile file exists and is valid YAML
- Catalog credentials are accessible
- Catalog connection succeeds
- Warehouse path is valid
- Schema evolution settings are valid

### Show Profile

Display current materialization configuration:

```bash
seeknal iceberg profile-show
```

Output:

```
Materialization Profile

  Enabled:         Yes
  Catalog Type:    rest
  Catalog URI:      https://lakekeeper.example.com
  Warehouse:        s3://my-bucket/warehouse
  Default Mode:     append
  TLS Verification:  Yes

  Schema Evolution:
    Mode:           safe
    Allow Add:       Yes
    Allow Type:      No

  Partitions:       event_date, region
```

### Interactive Setup

Set up credentials interactively:

```bash
# Environment variables (default)
seeknal iceberg setup

# System keyring (more secure)
seeknal iceberg setup --keyring
```

Prompts for:
- Lakekeeper catalog URI
- Warehouse path
- Credentials (optional)

### List Snapshots

View all snapshots for a table:

```bash
seeknal iceberg snapshot-list atlas.production.orders
seeknal iceberg snapshot-list atlas.production.orders --limit 20
```

Output:

```
Found 10 snapshot(s):

  12345678 | 2024-01-26 12:30:45 | Schema v1
  abcd1234 | 2024-01-25 10:15:22 | Schema v1
  ...
```

### Show Snapshot Details

View detailed information about a snapshot:

```bash
seeknal iceberg snapshot-show atlas.production.orders 12345678
```

Output:

```
Snapshot Details

  ID:              1234567812345678
  Created:         2024-01-26 12:30:45
  Schema Version:  1
  Expires At:      Never (retention policy)
```

## YAML Pipeline Integration

### Enable Materialization for a Node

Add a `materialization` section to any source or transform node:

```yaml
kind: source
name: orders
source: csv
table: "orders.csv"
schema:
  - name: order_id
    data_type: integer
  - name: customer_id
    data_type: integer
  - name: order_date
    data_type: date
  - name: amount
    data_type: float
  - name: status
    data_type: string
materialization:
  enabled: true
  table: atlas.production.orders         # 3-part name: catalog.namespace.table
  mode: append                           # append or overwrite
```

### Materialization Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | Yes | `false` | Enable materialization for this node |
| `table` | string | When enabled | - | Fully qualified table name (`catalog.namespace.table`) |
| `mode` | string | No | `append` | Write mode: `append` or `overwrite` |
| `create_table` | boolean | No | `true` | Auto-create table if it doesn't exist |

### Configuration Hierarchy

1. **Node-level** (`materialization:` in YAML) - Highest priority
2. **Profile-level** (`profiles.yml`) - Override with `--profile`
3. **Defaults** - Safe defaults if nothing specified

### Complete Example

This example shows a working 3-node pipeline with Iceberg materialization:

```yaml
# seeknal/sources/customers.yml
kind: source
name: customers
description: "Customer master data"
source: csv
table: "customers.csv"
schema:
  - name: customer_id
    data_type: integer
  - name: name
    data_type: string
  - name: email
    data_type: string
  - name: region
    data_type: string
materialization:
  enabled: true
  mode: overwrite                        # Full refresh each run
  table: atlas.production.customers
```

```yaml
# seeknal/sources/orders.yml
kind: source
name: orders
description: "Order transactions"
source: csv
table: "orders.csv"
schema:
  - name: order_id
    data_type: integer
  - name: customer_id
    data_type: integer
  - name: order_date
    data_type: date
  - name: amount
    data_type: float
  - name: status
    data_type: string
materialization:
  enabled: true
  mode: append                           # Accumulate across runs
  table: atlas.production.orders
```

```yaml
# seeknal/transforms/order_enriched.yml
kind: transform
name: order_enriched
description: "Orders enriched with customer data"
inputs:
  - ref: source.orders                   # Referenced as input_0 in SQL
  - ref: source.customers                # Referenced as input_1 in SQL
transform: |
  SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.region,
    o.order_date,
    o.amount,
    o.status
  FROM input_0 o
  JOIN input_1 c
    ON o.customer_id = c.customer_id
materialization:
  enabled: true
  mode: overwrite
  table: atlas.production.order_enriched
```

> **Transform SQL syntax:** When a transform has multiple inputs, reference them as `input_0`, `input_1`, etc. in order of the `inputs` list. Single-input transforms can reference input by the source name.

## Time Travel Queries

After materializing data, you can query historical snapshots using DuckDB:

```sql
-- Query current data
SELECT * FROM atlas.production.orders;

-- Query snapshot as of specific time
SELECT * FROM atlas.production.orders
FOR VERSION AS OF '1234567812345678';

-- Query snapshot as of specific timestamp
SELECT * FROM atlas.production.orders
FOR TIMESTAMP AS OF '2024-01-26 12:00:00';
```

## Schema Evolution Examples

### Adding a Column

**Initial schema:**
```yaml
features:
  - name: order_count
    type: int
  - name: total_revenue
    type: double
```

**Add new column:**
```yaml
features:
  - name: order_count
    type: int
  - name: total_revenue
    type: double
  - name: customer_segment  # New column
    type: string
```

**Result:** With `mode: safe` or `mode: auto`, new column is added automatically.

### Changing Column Type (AUTO mode)

```yaml
# Before
features:
  - name: order_count
    type: int    # 32-bit integer

# After (safe conversion)
features:
  - name: order_count
    type: long   # 64-bit integer
```

**Result:** With `mode: auto`, type change is allowed.

## Best Practices

### 1. Use Partitioning for Large Tables

```yaml
materialization:
  partition_by:
    - event_date    # Partition by date for time-series data
    - region        # Add secondary partition for filtering
```

**Benefits:**
- Faster queries with partition pruning
- Reduced cost for querying recent data
- Efficient snapshot management

### 2. Enable Schema Evolution for Development

```yaml
# Development environment
schema_evolution:
  mode: auto
  allow_column_add: true
  allow_column_type_change: true
```

```yaml
# Production environment
schema_evolution:
  mode: strict
```

### 3. Use APPEND Mode for Incremental Pipelines

```yaml
materialization:
  mode: append      # Preserve historical data
```

**Best for:**
- Time-series data
- Event streaming
- Daily/hourly pipeline runs

### 4. Use OVERWRITE Mode for Dimension Tables

```yaml
materialization:
  mode: overwrite   # Full refresh
```

**Best for:**
- Slowly changing dimensions
- Reference data
- Full rebuild scenarios

### 5. Set Snapshot Retention Policy

Configure snapshot expiration to manage storage costs:

```yaml
catalog:
  snapshot_retention_days: 30    # Keep snapshots for 30 days
```

### 6. Validate Before Deploying

Always validate configuration in development first:

```bash
seeknal iceberg validate-materialization
seeknal dry-run sources/orders.yml
```

## Security Considerations

### Credentials

**✅ Recommended:** Environment variables or system keyring

```bash
export LAKEKEEPER_URI=https://lakekeeper.example.com
export LAKEKEEPER_CREDENTIAL=user:password
```

**❌ Avoid:** Hardcoding credentials in YAML files

```yaml
# DON'T DO THIS
catalog:
  uri: https://user:password@lakekeeper.example.com  # Cleartext password!
```

### TLS Verification

Always enable TLS verification in production:

```yaml
catalog:
  verify_tls: true    # Default: true
```

Only disable for local development:

```yaml
catalog:
  verify_tls: false   # Only for development!
```

### Path Security

All warehouse paths are validated for path traversal attacks:

```yaml
# ✅ Valid
warehouse: s3://my-bucket/warehouse

# ❌ Blocked (path traversal attempt)
warehouse: s3://my-bucket/../etc/passwd
```

### SQL Injection Prevention

All table names are validated and quoted:

```yaml
# ✅ Valid
table: atlas.production.orders

# ❌ Blocked (SQL injection attempt)
table: orders; DROP TABLE users;
```

## Verifying Data in Iceberg

After running a pipeline with materialization, verify your data using DuckDB:

```python
import duckdb, json, urllib.request

con = duckdb.connect()
con.install_extension('httpfs'); con.load_extension('httpfs')
con.install_extension('iceberg'); con.load_extension('iceberg')

# Configure S3/MinIO
con.execute("SET s3_region='us-east-1'; SET s3_endpoint='localhost:9000'")
con.execute("SET s3_url_style='path'; SET s3_use_ssl=false")
con.execute("SET s3_access_key_id='minioadmin'")
con.execute("SET s3_secret_access_key='your-secret-key'")

# Get OAuth token (if using Keycloak)
data = b'grant_type=client_credentials&client_id=duckdb&client_secret=your-client-secret'
req = urllib.request.Request(
    'http://localhost:8080/realms/master/protocol/openid-connect/token', data=data)
token = json.loads(urllib.request.urlopen(req).read())['access_token']

# Attach to Lakekeeper catalog
con.execute(f"""
    ATTACH 'seeknal-warehouse' AS atlas (
        TYPE ICEBERG,
        ENDPOINT 'http://localhost:8181/catalog',
        AUTHORIZATION_TYPE 'oauth2',
        TOKEN '{token}'
    );
""")

# Query materialized tables
con.execute("SELECT * FROM atlas.production.customers").fetchdf()
con.execute("SELECT COUNT(*) FROM atlas.production.orders").fetchone()
```

You can also verify via the Lakekeeper REST API:

```bash
# Get OAuth token
TOKEN=$(curl -s -X POST "http://localhost:8080/realms/master/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=duckdb" \
  -d "client_secret=your-client-secret" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Get warehouse UUID prefix
curl -s "http://localhost:8181/catalog/v1/config?warehouse=seeknal-warehouse" \
  -H "Authorization: Bearer $TOKEN"

# List namespaces (replace {prefix} with UUID from above)
curl -s "http://localhost:8181/catalog/v1/{prefix}/namespaces" \
  -H "Authorization: Bearer $TOKEN"

# List tables in a namespace
curl -s "http://localhost:8181/catalog/v1/{prefix}/namespaces/production/tables" \
  -H "Authorization: Bearer $TOKEN"
```

## Lakekeeper Setup

### Create Namespace

Namespaces must exist in Lakekeeper before tables can be created:

```bash
# 1. Get warehouse prefix UUID
PREFIX=$(curl -s "http://localhost:8181/catalog/v1/config?warehouse=seeknal-warehouse" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print(json.load(sys.stdin)['overrides']['prefix'])")

# 2. Create namespace
curl -s -X POST "http://localhost:8181/catalog/v1/$PREFIX/namespaces" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["production"], "properties": {}}'
```

### Configure Storage Credentials

Lakekeeper needs S3 credentials to manage Iceberg data files:

```bash
# Set storage credentials for the warehouse
curl -s -X POST "http://localhost:8181/management/v1/warehouse/{warehouse-uuid}/storage-credential" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "minioadmin",
    "aws-secret-access-key": "your-secret-key"
  }'
```

> **Note:** Lakekeeper validates credentials by performing a test write to S3 at registration time.

## Troubleshooting

### Connection Issues

**Problem:** `Failed to connect to catalog`

**Solution:**
1. Verify catalog URI is accessible: `curl http://localhost:8181/health`
2. Check environment variables: `env | grep LAKEKEEPER`
3. Validate configuration: `seeknal iceberg validate-materialization --profile profiles.yml`

### OAuth2 Token Issues

**Problem:** `Failed to get OAuth token from Keycloak`

**Solution:**
1. Verify Keycloak is running: `curl http://localhost:8080/health`
2. Check client credentials: `echo $KEYCLOAK_CLIENT_ID` / `echo $KEYCLOAK_CLIENT_SECRET`
3. Test token endpoint manually:
```bash
curl -s -X POST "http://localhost:8080/realms/master/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=$KEYCLOAK_CLIENT_ID" \
  -d "client_secret=$KEYCLOAK_CLIENT_SECRET"
```

### HTTP 500 "Error fetching secret"

**Problem:** Table creation fails with HTTP 500 from Lakekeeper.

**Solution:** Lakekeeper can't access S3 storage credentials. Register credentials:
```bash
curl -s -X POST "http://localhost:8181/management/v1/warehouse/{uuid}/storage-credential" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"type":"s3","credential-type":"access-key","aws-access-key-id":"KEY","aws-secret-access-key":"SECRET"}'
```

### Schema Mismatch

**Problem:** `Schema validation failed: column 'new_col' not found`

**Solution:**
1. Check schema evolution mode in `profiles.yml`
2. Use `mode: auto` to allow automatic column addition
3. Or manually add column to existing table first

### Permission Issues

**Problem:** `Permission denied: s3://my-bucket/warehouse`

**Solution:**
1. Verify AWS credentials: `env | grep AWS`
2. Check S3 access: `aws s3 ls s3://my-bucket/ --endpoint-url http://localhost:9000`
3. For MinIO with Docker: ensure Lakekeeper can reach MinIO (Docker networking can cause issues)
4. As a development workaround for MinIO, set bucket to public:
```bash
mc anonymous set public myminio/my-bucket
```

### Extension Loading

**Problem:** `Failed to load Iceberg extension`

**Solution:**
1. Verify DuckDB version: `python -c "import duckdb; print(duckdb.__version__)"`
2. Install latest DuckDB: `pip install --upgrade duckdb`
3. Check extension installation: `INSTALL iceberg; LOAD iceberg;`

## Performance Considerations

### Write Optimization

For large datasets (>1M rows):

1. **Use partitioning** to distribute data
2. **Increase DuckDB memory** in `profiles.yml`:

```yaml
duckdb:
  memory_limit: 4GB    # Default: 1GB
  threads: 8           # Default: 4
```

3. **Use batch writes** for incremental data

### Read Optimization

For fast queries:

1. **Partition by frequently filtered columns**
2. **Use Z-ordering** for multi-column queries:

```yaml
materialization:
  z_order_by:
    - event_date
    - customer_id
```

3. **Enable statistics collection**:

```yaml
materialization:
  collect_statistics: true
```

### Snapshot Management

Manage storage costs:

1. **Set retention policy** to expire old snapshots
2. **Clean up orphaned snapshots** after failures
3. **Use VACUUM** to remove deleted data:

```sql
VACUUM atlas.production.orders;
```

## Reference

### MaterializationConfig

```python
@dataclass
class MaterializationConfig:
    enabled: bool                          # Enable materialization
    catalog: CatalogConfig                 # Catalog configuration
    default_mode: MaterializationMode      # append or overwrite
    schema_evolution: SchemaEvolutionConfig
    partition_by: List[str]                # Partition columns
```

### CatalogConfig

```python
@dataclass
class CatalogConfig:
    type: CatalogType                      # rest for Lakekeeper
    uri: str                               # Catalog endpoint
    warehouse: str                         # S3/GCS/Azure path
    bearer_token: Optional[str]            # Auth token
    verify_tls: bool = True               # Enable TLS
    connection_timeout: int = 10           # Connection timeout (seconds)
    request_timeout: int = 30              # Request timeout (seconds)
```

### SchemaEvolutionConfig

```python
@dataclass
class SchemaEvolutionConfig:
    mode: SchemaEvolutionMode              # safe, auto, or strict
    allow_column_add: bool = False
    allow_column_type_change: bool = False
    allow_column_drop: bool = False
```

## Known Issues

| Issue | Status | Workaround |
|-------|--------|------------|
| `iceberg profile-show` defaults to `~/.seeknal/profiles.yml` | Bug | Pass `--profile profiles.yml` explicitly for local project files |
| `iceberg validate-materialization` crashes | Bug | `RequestException` variable not imported in `materialization_cli.py` |
| `iceberg snapshot-list` fails with 3-part names | Bug | DuckDB "NameListToString NOT IMPLEMENTED" error with `catalog.namespace.table` format |
| Docker networking: Lakekeeper can't auth to MinIO | Infra | Set MinIO bucket to `public` for development environments |
| Orders table accumulates with append mode | By Design | Use `overwrite` mode for full refresh, or clear between runs |
| DuckDB `SUM()` produces HUGEINT, invalid for Iceberg | DuckDB Limitation | Cast aggregates explicitly: `CAST(SUM(col) AS BIGINT)` |

## Further Reading

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Lakekeeper Documentation](https://www.lakekeeper.io/)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [Time Travel Queries](https://iceberg.apache.org/spec/#incremental-data-writes)
- [YAML Pipeline Tutorial - Iceberg Section](tutorials/yaml-pipeline-tutorial.md#part-10-iceberg-materialization)

## Changelog

### v1.1.0 (2026-02-18)

End-to-end verified with Lakekeeper + MinIO:
- Added Quick Start section with verified Lakekeeper + MinIO setup
- Added OAuth2/Keycloak authentication documentation
- Added 3-part table naming convention (`catalog.namespace.table`)
- Added Lakekeeper namespace and storage credential setup
- Added data verification section with DuckDB and REST API examples
- Fixed transform SQL examples to use `input_0`/`input_1` syntax
- Added Known Issues table from E2E testing
- Updated environment variables for OAuth2 and S3 configuration

### v1.0.0 (2026-01-26)

Initial release:
- Profile-driven configuration (dbt-like profiles.yml)
- Per-node YAML overrides
- Atomic write operations with rollback
- Schema evolution (safe/auto/strict modes)
- Snapshot management for time travel
- Security features (TLS, audit logging, SQL injection prevention)
- CLI commands (validate-materialization, snapshot-list, snapshot-show, setup, profile-show)
- Environment variable and keyring support for credentials
