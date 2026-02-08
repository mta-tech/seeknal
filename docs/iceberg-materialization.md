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

## Configuration

### profiles.yml Setup

Create a `profiles.yml` file in `~/.seeknal/`:

```yaml
materialization:
  enabled: true
  catalog:
    type: rest
    uri: ${LAKEKEEPER_URI}              # e.g., https://lakekeeper.example.com
    warehouse: s3://my-bucket/warehouse  # S3, GCS, or Azure path
    verify_tls: true                     # Enable TLS verification (default)
  default_mode: append                   # or "overwrite"

  schema_evolution:
    mode: safe                           # safe, auto, or strict
    allow_column_add: true               # Allow adding new columns
    allow_column_type_change: false      # Allow type changes
    allow_column_drop: false             # Allow dropping columns

  partition_by:                          # Optional partitioning
    - event_date
    - region
```

### Environment Variables

Set credentials as environment variables (recommended):

```bash
export LAKEKEEPER_URI=https://lakekeeper.example.com
export LAKEKEEPER_CREDENTIAL=user:password      # Optional bearer token
export LAKEKEEPER_WAREHOUSE=s3://my-bucket/warehouse
```

### Per-Node Override

Override global settings for specific nodes in YAML:

```yaml
kind: source
name: orders
materialization:
  enabled: true
  mode: overwrite              # Override default mode
  partition_by:
    - order_date
```

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
seeknal iceberg snapshot-list warehouse.prod.orders
seeknal iceberg snapshot-list warehouse.prod.orders --limit 20
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
seeknal iceberg snapshot-show warehouse.prod.orders 12345678
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

```yaml
kind: source
name: orders
connection:
  uri: postgresql://user:pass@host/db
  table: public.orders
materialization:
  enabled: true
  table: warehouse.prod.orders    # Optional: override table name
  mode: append                    # Optional: override default mode
  partition_by:
    - order_date
```

### Configuration Hierarchy

1. **Node-level** (`materialization:` in YAML) - Highest priority
2. **Profile-level** (`profiles.yml`) - Override with `--profile`
3. **Defaults** - Safe defaults if nothing specified

### Complete Example

```yaml
# sources/orders.yml
kind: source
name: orders
connection:
  uri: postgresql://user:pass@host/db
  table: public.orders
materialization:
  enabled: true
  mode: append
  partition_by:
    - order_date

---

# transforms/clean_orders.yml
kind: transform
name: clean_orders
sql: |
  SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount
  FROM source_orders
  WHERE status != 'cancelled'
depends_on:
  - source.orders
materialization:
  enabled: true
  mode: append
  partition_by:
    - order_date

---

# feature_groups/daily_orders.yml
kind: feature_group
name: daily_orders
entity: order
features:
  - name: order_count
    type: int
  - name: total_revenue
    type: double
  - name: avg_order_value
    type: double
keys:
  - order_date
depends_on:
  - transform.clean_orders
materialization:
  enabled: true
  mode: append
  partition_by:
    - order_date
```

## Time Travel Queries

After materializing data, you can query historical snapshots using DuckDB:

```sql
-- Query current data
SELECT * FROM warehouse.prod.orders;

-- Query snapshot as of specific time
SELECT * FROM warehouse.prod.orders
FOR VERSION AS OF '1234567812345678';

-- Query snapshot as of specific timestamp
SELECT * FROM warehouse.prod.orders
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
table: warehouse.prod.orders

# ❌ Blocked (SQL injection attempt)
table: orders; DROP TABLE users;
```

## Troubleshooting

### Connection Issues

**Problem:** `Failed to connect to catalog`

**Solution:**
1. Verify catalog URI is accessible: `curl https://lakekeeper.example.com/health`
2. Check credentials: `echo $LAKEKEEPER_CREDENTIAL`
3. Validate configuration: `seeknal iceberg validate-materialization`

### Schema Mismatch

**Problem:** `Schema validation failed: column 'new_col' not found`

**Solution:**
1. Check schema evolution mode in `profiles.yml`
2. Use `mode: auto` to allow automatic column addition
3. Or manually add column to existing table first

### Permission Issues

**Problem:** `Permission denied: s3://my-bucket/warehouse`

**Solution:**
1. Verify AWS credentials: `aws s3 ls s3://my-bucket/`
2. Check IAM policies include: `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`
3. Ensure bucket policy allows write access

### Extension Loading

**Problem:** `Failed to load Iceberg extension`

**Solution:**
1. Verify DuckDB version: `duckdb --version`
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
VACUUM warehouse.prod.orders;
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

## Further Reading

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Lakekeeper Documentation](https://www.lakekeeper.io/)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [Time Travel Queries](https://iceberg.apache.org/spec/#incremental-data-writes)

## Changelog

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
