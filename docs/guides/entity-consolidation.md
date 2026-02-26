# Entity Consolidation Guide

Entity consolidation merges per-feature-group parquet files into consolidated per-entity views with struct-namespaced columns. This enables fast cross-FG feature retrieval without N-way joins at query time.

---

## How It Works

```
seeknal run
  → FeatureGroupExecutor writes per-FG parquets (existing)
  → _consolidate_entities() runs automatically after DAG execution
    → Discovers FG parquets grouped by entity
    → LEFT JOINs FGs on (join_keys, event_time)
    → Wraps each FG's features in struct_pack()
    → Writes target/feature_store/{entity}/features.parquet
    → Writes target/feature_store/{entity}/_entity_catalog.json
```

Consolidation is **best-effort** — failures are logged as warnings and never fail the pipeline run.

---

## Storage Layout

After running a pipeline with feature groups, the storage looks like:

```
target/
├── feature_group/                     # Per-FG outputs (existing)
│   ├── customer_features/
│   │   ├── result.parquet
│   │   └── _metadata.json
│   └── product_features/
│       ├── result.parquet
│       └── _metadata.json
├── feature_store/                     # Consolidated per-entity (NEW)
│   └── customer/
│       ├── features.parquet           # Struct columns: customer_features.*, product_features.*
│       └── _entity_catalog.json       # Catalog metadata
└── intermediate/                      # Intermediate parquets (existing)
```

The consolidated parquet has struct columns where each FG's features are namespaced:

```sql
-- Query consolidated entity in REPL
SELECT
    customer_id,
    event_time,
    customer_features.revenue,       -- Dot notation into struct
    customer_features.orders,
    product_features.price
FROM entity_customer;
```

---

## Retrieval API

### ctx.features() — Selective Cross-FG Retrieval

```python
@transform(name="training_data")
def build_training_data(ctx):
    # Select specific features across feature groups
    df = ctx.features("customer", [
        "customer_features.revenue",
        "customer_features.orders",
        "product_features.avg_price",
    ])
    # Returns flat DataFrame with columns:
    # customer_id, event_time, customer_features__revenue, customer_features__orders, product_features__avg_price
    return df
```

With point-in-time filter:

```python
# Get features as of a specific date (latest per key before cutoff)
df = ctx.features("customer", [
    "customer_features.revenue",
], as_of="2026-01-15")
```

### ctx.entity() — Full Entity View

```python
@transform(name="full_entity_view")
def get_full_entity(ctx):
    # Get all features with struct columns intact
    df = ctx.entity("customer")
    # Returns DataFrame with struct columns:
    # customer_id, event_time, customer_features (struct), product_features (struct)
    return df
```

---

## CLI Commands

### List Entities

```bash
seeknal entity list
```

Shows all consolidated entities with FG counts, feature counts, and consolidation timestamps.

### Show Entity Details

```bash
seeknal entity show customer
```

Displays the full catalog: join keys, per-FG features, row counts, schemas, and last update times.

### Manual Consolidation

```bash
# Consolidate all entities
seeknal consolidate

# Re-consolidate and prune stale FG columns
seeknal consolidate --prune
```

Use `seeknal consolidate` after running individual nodes with `seeknal run --nodes feature_group.X`.

---

## REPL Integration

Consolidated entity parquets are automatically registered as views in the REPL:

```bash
seeknal repl

# Query consolidated entity
sql> SELECT customer_features.revenue FROM entity_customer LIMIT 5;
```

Views are named `entity_{name}` (e.g., `entity_customer`, `entity_product`).

---

## External Materialization

Consolidated views can be materialized to external targets.

### Iceberg (Native Struct Columns)

Writes struct columns directly to Iceberg tables. DuckDB Iceberg extension supports nested types natively.

```python
materializer = ConsolidationMaterializer()
result = materializer.materialize_iceberg(
    con=con,
    entity_name="customer",
    parquet_path=parquet_path,
    target_config={
        "table_pattern": "atlas.features.{entity}_entity",
        "mode": "overwrite",
    },
)
```

### PostgreSQL (Flattened Columns)

PostgreSQL doesn't support struct columns, so features are flattened to `{fg_name}__{feature_name}` prefixed columns:

```
customer_features__revenue, customer_features__orders, product_features__price
```

Column names exceeding PostgreSQL's 63-character identifier limit are automatically truncated with a CRC32 hash suffix.

---

## Incremental Consolidation

Consolidation is incremental by default:

- Only entities with **changed FGs** (successful in the current run) are re-consolidated
- Unchanged entities keep their existing consolidated parquet
- The `_entity_catalog.json` tracks `last_updated` per FG to detect staleness

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **LEFT JOIN** across FGs | Preserves all rows from the base FG; missing FG data appears as NULL structs |
| **struct_pack()** for namespacing | Schema isolation — FGs can have overlapping column names without conflicts |
| **Alphabetical FG ordering** | First FG alphabetically is the base table for deterministic joins |
| **Best-effort consolidation** | Pipeline runs should never fail due to consolidation errors |
| **Atomic writes** | Temp file + `Path.replace()` prevents corrupted parquets on failure |

---

## Related Topics

- **[Feature Store Guide](feature-store.md)** — Feature groups and offline/online stores
- **[Python Pipelines](python-pipelines.md)** — Writing transforms with `ctx.features()`
- **[CLI Reference](../reference/cli.md#entity-consolidation)** — Entity and consolidate commands
