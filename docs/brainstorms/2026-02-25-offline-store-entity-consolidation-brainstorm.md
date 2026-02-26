---
topic: Offline Store Entity Consolidation
date: 2026-02-25
status: complete
approach: C (Hybrid — Per-FG Write, Per-Entity Materialized View)
---

# Offline Store: Entity-Level Feature Consolidation

## What We're Building

A hybrid offline store where feature groups write independently to per-FG parquet files (current pattern + upsert support), then a post-run consolidation step materializes all FGs for the same entity into a single entity-level table with struct-namespaced columns for fast, cross-FG feature retrieval.

## Why This Approach

### Problem

The current per-FG storage (`target/feature_group/{name}/result.parquet`) optimizes for write simplicity but makes cross-FG feature retrieval expensive — consumers who need "total_revenue from customer_features AND avg_price from product_features for customer entity as-of date X" must join across multiple files.

### Research: How Others Do It

| System | Storage Model | Strength | Weakness |
|--------|--------------|----------|----------|
| **ds2_sdk (previous)** | Per-entity table, `name` column, MD5 `__pk__` | Fast reads, single table | Write contention, schema coupling |
| **Feast** | Per-feature-view table, join at query time | Write isolation, simple schema evolution | Slow multi-FG reads (N joins) |
| **Chronon** | Per-GroupBy KV table (Avro binary), Join assembly | Streaming support, IR compression | Complex, JVM-dependent |

### Why Hybrid (Approach C)

- **Write phase**: Per-FG parquet (like Feast) — no contention, easy parallelization, simple schema evolution
- **Read phase**: Per-entity consolidated view (like ds2_sdk) — single file read for all features
- **Tradeoff**: Extra disk space (2x) and consolidation time, but acceptable for medium scale (10-50 FGs, 1-100M rows)

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Write target | Per-FG: `target/feature_group/{name}/` | Write isolation, parallel execution, current pattern |
| Read target | Per-entity: `target/feature_store/{entity}/` | Fast entity-level retrieval with struct columns |
| Feature namespacing | DuckDB struct columns | Schema isolation — FGs can have same column names without collision |
| Composite key | `(entity_join_keys, event_time)` | Enables upsert and point-in-time retrieval |
| Upsert strategy | Per-FG partition-level overwrite | Rewrite only affected time partitions, not whole table |
| Consolidation trigger | Post-run step (after all FGs execute) | Ensures all FGs are up-to-date before consolidation |
| Consolidation method | LEFT JOIN across FGs → struct_pack → write parquet | DuckDB-native, efficient for medium scale |
| Source of truth | Per-FG files (not consolidated view) | Consolidated view is derived, can be regenerated |
| Iceberg entity materialization | Native struct columns | Iceberg supports nested types, dot notation queries |
| PostgreSQL entity materialization | Flatten to `fgname__feature` prefixed columns | Type-safe, queryable, compatible with all SQL tools |
| Per-FG external materialization | Unchanged (flat columns per FG) | Existing pattern, no migration needed |

## Storage Layout

```
target/
├── feature_group/                    # Source of truth (per-FG)
│   ├── customer_features/
│   │   ├── result.parquet            # Current pattern
│   │   └── _metadata.json
│   └── product_features/
│       ├── result.parquet
│       └── _metadata.json
│
├── feature_store/                    # Consolidated read view (per-entity)
│   ├── customer/
│   │   ├── features.parquet          # Struct columns: customer_features.*, product_features.*
│   │   └── _entity_catalog.json      # Entity metadata, FG list, schemas
│   └── category/
│       ├── features.parquet
│       └── _entity_catalog.json
│
└── intermediate/                     # Cross-process ref() (existing)
    └── feature_group_customer_features.parquet
```

## Struct Column Schema

```sql
-- Entity table schema (consolidated view)
CREATE TABLE customer_entity AS
SELECT
    customer_id,                                    -- entity join key
    event_time,                                     -- composite key part 2
    struct_pack(                                     -- FG 1: customer_features
        total_orders := cf.total_orders,
        total_revenue := cf.total_revenue,
        avg_order_value := cf.avg_order_value
    ) AS customer_features,
    struct_pack(                                     -- FG 2: product_features
        num_products := pf.num_products,
        avg_price := pf.avg_price,
        categories := pf.categories                  -- LIST(VARCHAR) supported
    ) AS product_features
FROM customer_features cf
LEFT JOIN product_features pf
    ON cf.customer_id = pf.customer_id
    AND cf.event_time = pf.event_time;
```

## Retrieval Patterns

```python
# Pattern 1: By feature group name (current — unchanged)
df = ctx.ref("feature_group.customer_features")

# Pattern 2: By entity + feature list (NEW — cross-FG selection)
df = ctx.features("customer", ["customer_features.total_revenue", "product_features.avg_price"],
                   as_of="2026-01-20")

# Pattern 3: By entity (all features)
df = ctx.entity("customer", as_of="2026-01-20")

# Pattern 4: Direct parquet read (notebooks, scripts)
import pandas as pd
df = pd.read_parquet("target/feature_store/customer/features.parquet")
```

## Data Types in Struct Columns

DuckDB structs support all common feature types:

| Feature Pattern | DuckDB Type | Notes |
|---|---|---|
| Counts, IDs | `BIGINT` | Native |
| Monetary values | `DOUBLE` | Native |
| Booleans/flags | `BOOLEAN` | Native |
| Categories | `VARCHAR` | Native |
| Timestamps | `TIMESTAMP` | Native |
| Multi-value (tags) | `LIST(VARCHAR)` | Native nested type |
| Key-value maps | `MAP(VARCHAR, DOUBLE)` | Native |
| ML embeddings | `FLOAT[N]` or `LIST(FLOAT)` | Fixed or variable length |

Each FG is a separate struct column — types don't leak between FGs. A NULL struct means "no data from this FG for this entity+time" (sparse representation).

## External Materialization Strategies

The entity consolidation produces struct columns locally (DuckDB parquet). External targets have different type system constraints, so materialization uses **target-appropriate serialization**.

### Per-FG Materialization (Existing — Unchanged)

Each feature group materializes independently with **flat columns** via `MaterializationDispatcher`. This is the current pattern and remains the source-of-truth write path for external targets.

```yaml
# Per-FG materialization — flat columns, works everywhere
materializations:
  - type: postgresql
    connection: local_pg
    table: features.customer_features
    mode: upsert_by_key
    unique_keys: [customer_id, event_time]
  - type: iceberg
    table: atlas.features.customer_features
    mode: append
```

### Entity Consolidation → Iceberg (Native Struct Support)

Iceberg natively supports `struct` types in its schema. DuckDB can write struct columns directly via `INSERT INTO` / `CREATE TABLE AS`. The consolidated entity view maps naturally to Iceberg's nested type system.

```sql
-- DuckDB → Iceberg: struct columns written as Iceberg STRUCT type
CREATE TABLE iceberg_catalog.features.customer_entity AS
SELECT
    customer_id,
    event_time,
    struct_pack(total_orders := cf.total_orders, total_revenue := cf.total_revenue) AS customer_features,
    struct_pack(num_products := pf.num_products, avg_price := pf.avg_price) AS product_features
FROM customer_features cf
LEFT JOIN product_features pf ON cf.customer_id = pf.customer_id AND cf.event_time = pf.event_time;

-- Iceberg schema result:
-- customer_id: string
-- event_time: timestamp
-- customer_features: struct<total_orders: bigint, total_revenue: double>
-- product_features: struct<num_products: bigint, avg_price: double>

-- Query with dot notation:
SELECT customer_id, customer_features.total_revenue FROM iceberg_catalog.features.customer_entity;
```

### Entity Consolidation → PostgreSQL (Flatten to Prefixed Columns)

DuckDB's postgres extension uses binary COPY protocol — it handles flat types but **cannot** write `STRUCT` columns to PostgreSQL. The consolidation step flattens structs to double-underscore-prefixed columns.

```sql
-- Consolidated view flattened for PostgreSQL
CREATE TABLE pg_db.features.customer_entity AS
SELECT
    customer_id,
    event_time,
    cf.total_orders    AS customer_features__total_orders,
    cf.total_revenue   AS customer_features__total_revenue,
    cf.avg_order_value AS customer_features__avg_order_value,
    pf.num_products    AS product_features__num_products,
    pf.avg_price       AS product_features__avg_price
FROM customer_features cf
LEFT JOIN product_features pf
    ON cf.customer_id = pf.customer_id
    AND cf.event_time = pf.event_time;
```

### Strategy Comparison

| Target | Column Format | Struct Support | Trade-off |
|--------|--------------|----------------|-----------|
| **Local parquet** | `struct_pack()` columns | Native | Best read perf, DuckDB dot notation |
| **Iceberg (S3)** | Iceberg `STRUCT` type | Native | Dot notation works, schema evolution supported |
| **PostgreSQL** | `fgname__feature` flat columns | Flattened | Type-safe, queryable, but column explosion with many FGs |
| **PostgreSQL (alt)** | `JSONB` per FG | Semi-structured | Flexible, fewer columns, but loses type safety |

### Decision: PostgreSQL Flattening Approach

**Recommended: Option A — Flatten to prefixed columns** (`customer_features__total_revenue`)

- Type-safe and queryable with standard SQL
- Compatible with any PostgreSQL tool (BI, ORM, etc.)
- Column explosion is acceptable at medium scale (50 FGs x 10 features = 500 columns)
- The `__` delimiter is unambiguous and easy to parse back to `{fg_name}.{feature_name}`

**Alternative (JSONB)** only if column count becomes impractical (>1000 columns) or FG schemas change very frequently.

### Materialization Flow (Updated)

```
Pipeline Run → Per-FG write (source of truth)
    │
    ├─► Per-FG Materialization (EXISTING — unchanged)
    │     Each FG → flat columns → PostgreSQL / Iceberg
    │     Configured via materializations: in YAML or @materialize decorator
    │
    └─► Entity Consolidation Step (NEW — post-run)
          Merges all FGs for same entity
          │
          ├─► Local:      target/feature_store/{entity}/features.parquet  (struct columns)
          ├─► Iceberg:    atlas.features.{entity}_entity                  (native struct)
          └─► PostgreSQL: features.{entity}_entity                        (flattened prefixed)
```

## Open Questions

*(All resolved during brainstorm session)*

## Resolved Questions

1. **Scale target**: Medium (10-50 FGs, 1-100M rows) — confirmed by user
2. **Struct motivation**: Schema isolation (FGs can have same column names)
3. **Primary access pattern**: By entity + feature list (cross-FG selection)
4. **Write pattern**: Upsert by (entity_key, event_time) — not full overwrite
5. **DuckDB struct support**: Full native support for all common feature types including nested LIST, MAP
6. **External materialization strategy**: Iceberg gets native struct columns; PostgreSQL gets flattened `fgname__feature` prefixed columns (type-safe, queryable). Per-FG materialization stays unchanged (flat columns).
7. **PostgreSQL flattening approach**: Double-underscore delimiter (`__`) chosen — unambiguous, easy to parse back to `{fg_name}.{feature_name}`, standard SQL compatible

## Scope Boundaries

**In scope:**
- Per-FG write path with upsert support
- Per-entity consolidation with struct columns
- Entity catalog metadata
- Point-in-time retrieval API (`ctx.features()`, `ctx.entity()`)
- Entity consolidation → Iceberg (native struct columns)
- Entity consolidation → PostgreSQL (flattened prefixed columns)

**Out of scope (future):**
- Online store (real-time serving)
- Streaming/incremental consolidation
- Cross-entity joins in consolidated view
- Time-partitioned per-FG storage (optimize later if needed)

## Sources

- **ds2_sdk**: `~/project/self/bmad-new/ds2_sdk` — entity-level table with MD5 `__pk__`, Java SerDe for struct collapse
- **Feast**: `~/project/self/bmad-new/feast` — per-feature-view storage, point-in-time joins via LEFT JOIN + ROW_NUMBER
- **Chronon**: `~/project/self/bmad-new/chronon` — per-GroupBy KV upload tables (Avro binary), Join assembly for wide vectors
