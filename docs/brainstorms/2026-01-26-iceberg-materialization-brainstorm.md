# Iceberg Materialization for YAML Pipelines

**Date:** 2026-01-26
**Status:** Brainstorm
**Related:** YAML Pipeline Execution, Lakekeeper Integration

---

## What We're Building

Add Apache Iceberg materialization to Seeknal's YAML pipeline workflow, enabling persistent storage of pipeline results with time travel, incremental updates, and schema evolution. The configuration follows dbt's profile pattern (centralized config with per-node overrides) and integrates with Lakekeeper REST catalog.

---

## User Goals

| Goal | Description |
|------|-------------|
| **Persistent Output Storage** | Store pipeline results in Iceberg tables for long-term storage |
| **Incremental Updates** | Append/update data as new data arrives without full refresh |
| **Time Travel & Versioning** | Leverage Iceberg's snapshot capabilities for data versioning |

---

## Why This Approach: Hybrid Profile-Driven with Per-Node Overrides

### Chosen Design

**Profile-driven defaults with YAML overrides** - similar to dbt's `profiles.yml`

**Rationale:**
- Familiar to dbt users (lower learning curve)
- Centralized configuration for teams
- Flexibility for node-specific requirements
- Clear separation of infrastructure vs. pipeline logic

### Key Design Decisions

#### 1. Configuration Priority: YAML Overrides Profile

**Decision:** Profile defines defaults, YAML can override any setting.

**Structure:**
```
~/.seeknal/profiles.yml    (global defaults)
├── Project-level profile   (team/project overrides)
└── YAML files               (per-node overrides)
```

**Example:**
```yaml
# profiles.yml (global)
materialization:
  enabled: false  # Opt-in by default
  catalog:
    type: rest
    uri: http://localhost:8181
  mode: append

# seeknal/sources/orders.yml (per-node)
kind: source
name: orders
materialization:
  enabled: true  # This node materializes
  mode: overwrite_daily  # Override default
  table: warehouse.prod.orders  # Custom table name
```

#### 2. Schema Handling: Auto-Create from Source/Transform Schema

**Decision:** Automatically create Iceberg tables from existing schema definitions.

**Why:** Users already define schemas in source YAML. Reuse this for table creation.

**Implementation:**
- Read `schema` section from source YAML
- Map Seeknal data types to Iceberg types
- Use `CREATE TABLE IF NOT EXISTS` on first materialization
- Schema evolution via Iceberg's built-in schema evolution APIs

**Type Mapping:**
| Seeknal Type | Iceberg Type |
|--------------|---------------|
| `integer` | `long` |
| `float` | `double` |
| `string` | `string` |
| `date` | `date` |
| `timestamp` | `timestamp` |
| `boolean` | `boolean` |

#### 3. Partitioning: User-Specified Columns

**Decision:** Support partitioning by user-specified columns (time-based or arbitrary).

**Configuration:**
```yaml
kind: source
name: orders
materialization:
  enabled: true
  partition_by:
    - order_date  # Time-based partition
    - region       # Arbitrary column partition
```

**Implementation:**
- Pass partition spec to DuckDB's Iceberg writer
- Validate partition columns exist in schema
- Support Iceberg partition transformations (day, month, year, bucket, truncate)

#### 4. Mode Behavior: Smart Append with Full Overwrite

**Decision:** Incremental runs use append mode; `--full` flag triggers overwrite.

**Behavior:**
- **Incremental run (`seeknal run`):** Append new data to table
  - Preserves existing snapshots
  - Idempotent for unchanged nodes (skip if hash matches)
  - Use for daily/hourly incremental loads

- **Full refresh (`seeknal run --full`):** Overwrite entire table
  - Deletes all data before writing
  - Creates new snapshot
  - Use for complete data refresh or corrections

**Implementation:**
```python
if full_mode:
    # DELETE all data, then write
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"COPY {table_name} FROM 'duckdb_view'")
else:
    # Append only
    con.execute(f"INSERT INTO {table_name} SELECT * FROM 'duckdb_view'")
```

#### 5. Error Handling: Fail Fast on Catalog Issues

**Decision:** Pipeline fails immediately if Lakekeeper catalog is unreachable.

**Rationale:** Data reliability is critical - if we can't write to the catalog, fail loudly rather than silently skipping materialization.

**Implementation:**
- Validate catalog connection at pipeline start
- Fail immediately on connection errors
- Provide clear error messages for common issues:
  - "Cannot connect to Lakekeeper at http://localhost:8181"
  - "Table warehouse.prod.orders does not exist in catalog"
  - "Catalog error: insufficient permissions"

#### 6. Table Naming: Fully Qualified Names

**Decision:** Iceberg tables use fully qualified names: `catalog.namespace.table`.

**Profile Default:**
```yaml
materialization:
  catalog:
    name: warehouse  # Catalog name
    namespace: prod    # Namespace (database/schema)
```

**YAML Override:**
```yaml
materialization:
  table: analytics.staging.orders  # catalog.namespace.table
```

**Implementation:**
- If `table` is fully qualified (contains dots), parse as cat.ns.table
- If `table` is simple name, use profile default: `{catalog}.{namespace}.{name}`
- Validate against catalog to ensure namespace exists

#### 7. Schema Evolution: Conservative by Default

**Decision:** Safe schema handling with explicit opt-in for changes.

| Scenario | Behavior | Config |
|----------|----------|--------|
| **New columns added** | Don't auto-add | Set `allow_column_add: true` |
| **Type changes** | Don't auto-change | Set `allow_column_type_change: true` |
| **Columns removed** | Never drop | Keep column (NULL in new data) |

**Rationale:** Pipeline stability and data safety are critical. Accidental schema changes can break downstream consumers.

**Configuration:**
```yaml
# profiles.yml
materialization:
  schema_evolution:
    mode: safe  # safe (default), auto (opt-in), strict (manual)
    allow_column_add: false
    allow_column_type_change: false
    allow_column_drop: false
```

**Per-node opt-in:**
```yaml
# seeknal/sources/orders.yml
kind: source
name: orders
materialization:
  enabled: true
  schema_evolution:
    allow_column_add: true  # This table accepts new columns
```

**Implementation:**
```python
# On materialization
existing_schema = get_iceberg_table_schema(table_name)
source_schema = get_source_schema(node)

# New columns
new_columns = set(source_schema) - set(existing_schema)
if new_columns:
    if allow_column_add:
        alter_table_add_columns(table_name, new_columns)
    else:
        raise SchemaEvolutionError(
            f"New columns detected: {new_columns}. "
            f"Enable schema_evolution.allow_column_add to proceed."
        )

# Dropped columns
dropped_columns = set(existing_schema) - set(source_schema)
if dropped_columns:
    # Never drop - just keep them NULL in new data
    logger.warning(f"Columns removed from source: {dropped_columns}. "
                   f"These will be NULL in new rows.")

# Type changes
type_changes = detect_type_changes(existing_schema, source_schema)
if type_changes:
    if allow_column_type_change:
        alter_table_change_types(table_name, type_changes)
    else:
        raise SchemaEvolutionError(
            f"Type changes detected: {type_changes}. "
            f"Enable schema_evolution.allow_column_type_change to proceed."
        )
```

**With --force flag:**
```bash
# Allow type changes for this run only
seeknal run --force-schema-evolution
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Profile Configuration                         │
│  ~/.seeknal/profiles.yml                                       │
│  - Catalog connection (Lakekeeper REST)                        │
│  - Default materialization settings                             │
│  - Warehouse location (S3, GCS, Azure)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    YAML Pipeline Nodes                           │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────────────┐    │
│  │   Source   │  │  Transform   │  │   Feature Group     │    │
│  │ materialize?│  │ materialize? │  │   materialize?      │    │
│  └──────┬─────┘  └──────┬───────┘  └─────────┬──────────┘    │
│         │                │                     │                 │
└─────────┼────────────────┼─────────────────────┼─────────────────┘
          │                │                     │
          ▼                ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              MaterializationExecutor (if enabled)                │
│  1. Load profile config                                         │
│  2. Merge with YAML override                                     │
│  3. Execute source/transform (data in DuckDB)                   │
│  4. Materialize to Iceberg via DuckDB                           │
│     - Create table if not exists                                │
│     - Write data (append/overwrite)                              │
│     - Handle partitions                                          │
│  5. Update state with Iceberg metadata                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Lakekeeper REST Catalog                       │
│  - Table metadata                                               │
│  - Snapshot management                                          │
│  - Schema evolution                                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Object Storage (S3/GCS)                        │
│  - Iceberg data files                                           │
│  - Metadata files                                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Configuration Schema

### Profile Configuration (`~/.seeknal/profiles.yml`)

```yaml
materialization:
  # Global enable/disable (opt-in by default)
  enabled: false

  # Catalog configuration
  catalog:
    type: rest  # Only type initially (Lakekeeper)
    uri: http://localhost:8181  # Lakekeeper REST endpoint
    warehouse: s3://my-bucket/warehouse

  # Default behavior (can be overridden per-node)
  default_format: iceberg
  default_mode: append  # append, overwrite, overwrite_daily

  # DuckDB-specific settings
  duckdb:
    iceberg_extension: true
    threads: 4

  # Schema evolution settings
  schema_evolution:
    mode: auto  # auto, none, strict
    allow_column_add: true
    allow_column_type_change: false
```

### Per-Node YAML Configuration

```yaml
kind: source
name: orders
description: "Order transactions"
source: csv
table: "orders.csv"

# Materialization config (optional, overrides profile)
materialization:
  enabled: true
  table: warehouse.prod.orders  # Fully qualified table name
  mode: append  # append, overwrite, overwrite_daily

  # Partitioning (optional)
  partition_by:
    - order_date  # Time-based partition
    - region      # Arbitrary column

  # Schema overrides (optional)
  schema_overrides:
    order_date:
      partition_transform: month  # day, month, year

  # Write properties (optional)
  write_properties:
    write.format.parquet.compression-level: 9
```

---

## Implementation Plan (High-Level)

### Phase 1: Profile Configuration System
1. Add profile loading to `context.py`
2. Parse `~/.seeknal/profiles.yml`
3. Merge profile with project/session config
4. Add validation for materialization config

### Phase 2: DuckDB Iceberg Integration
1. Load DuckDB iceberg extension in executor context
2. Configure REST catalog connection
3. Test `COPY TO` with Iceberg format
4. Implement table creation from schema

### Phase 3: Materialization Logic
1. Modify SourceExecutor/TransformExecutor to check materialization config
2. Add materialization step after data processing
3. Handle append/overwrite modes
4. Implement partitioning support

### Phase 4: State Management
1. Store Iceberg snapshot IDs in run state
2. Track table metadata for incremental updates
3. Handle schema evolution in state

---

## Open Questions

| Question | Options | Triage |
|----------|---------|---------|
| **Incremental mode detection** | Time column lookup, state-based watermark, user-specified | Later |
| **Backfill handling** | Parallel backfill, sequential, separate backfill command | Later |
| **Error handling** | Fail on catalog error, retry with backoff, skip node | Later |
| **Multi-catalog support** | Single catalog initially, add catalog-per-node later | Later |
| **Lakekeeper auth** | No auth, bearer token, AWS Sigv4 | Implement when needed |

---

## Non-Goals (v1)

- ❌ Multi-catalog support (single Lakekeeper catalog initially)
- ❌ Backfill command (use `seeknal run --full` for now)
- ❌ Custom table formats (Iceberg only initially)
- ❌ Materialization of feature groups (sources and transforms only)
- ❌ Automatic partition detection (user must specify)

---

## Success Criteria

A user can:

1. ✅ Configure Lakekeeper catalog in `profiles.yml`
2. ✅ Enable materialization for a source/transform node
3. ✅ Run `seeknal run` and see data in Iceberg table
4. ✅ Query table with external tools (Spark, Trino, etc.)
5. ✅ Run incremental updates (append mode)
6. ✅ Time travel to previous snapshots
7. ✅ Override partition settings per node

---

## Examples

### Example 1: Basic Source Materialization

```yaml
# ~/.seeknal/profiles.yml
materialization:
  enabled: true
  catalog:
    type: rest
    uri: http://localhost:8181
    warehouse: s3://my-bucket/warehouse
  default_mode: append
```

```yaml
# seeknal/sources/orders.yml
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
# No materialization section = uses profile defaults
```

**Result:** Data from `orders.csv` written to `s3://my-bucket/warehouse/orders` in Iceberg format.

### Example 2: Partitioned Table

```yaml
# seeknal/sources/events.yml
kind: source
name: events
source: json
table: "events/*.json"
materialization:
  enabled: true
  mode: append
  partition_by:
    - event_date  # Creates daily partitions
  write_properties:
    write.format.parquet.compression-level: 9
```

### Example 3: Transform Materialization

```yaml
# seeknal/transforms/customer_metrics.yml
kind: transform
name: customer_metrics
transform: |
  SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
  FROM source.orders
  GROUP BY customer_id
inputs:
  - ref: source.orders
materialization:
  enabled: true
  mode: overwrite_daily  # Overwrite once per day
  partition_by:
    - as_date(CURRENT_TIMESTAMP())  # Partition by run date
```

---

## Next Steps

Ready for planning! Run `/workflows:plan` to create a detailed implementation plan.

**Key areas to plan:**
1. Profile configuration system architecture
2. DuckDB Iceberg extension integration details
3. Executor modification strategy
4. State management for Iceberg metadata
5. Testing strategy (Lakekeeper integration, local dev, CI)
