# SQLMesh-Inspired Features for Seeknal

**Date**: 2026-02-08
**Status**: Brainstorm
**Inspired By**: SQLMesh (deep codebase research at ~/project/self/bmad-new/sqlmesh)
**Target Personas**: Data Engineers, ML Engineers, Analytics Engineers (equal priority)

---

## What We're Building

11 high-impact features inspired by SQLMesh's architecture, adapted to Seeknal's feature-store-first philosophy. These features transform Seeknal from a capable YAML pipeline tool into a production-grade data platform with safety guarantees, performance, and multi-engine portability.

## Why SQLMesh as Inspiration

SQLMesh solves problems that Seeknal currently faces:
- **Broken incremental execution** -> SQLMesh's content-based fingerprinting + interval tracking
- **No change safety** -> SQLMesh's virtual environments + plan/apply + change categorization
- **Sequential execution** -> SQLMesh's parallel scheduler
- **Single-engine lock-in** -> SQLMesh's SQLGlot transpilation
- **No data quality gates** -> SQLMesh's audit framework
- **No testing story** -> SQLMesh's auto-generated unit tests

---

## Feature 1: Virtual Environments & Plan/Apply Workflow

### What
Create isolated dev/staging environments that share unchanged data with production. Preview exact changes (new nodes, modified SQL, schema diffs) before applying them. Like Terraform for data pipelines.

### How SQLMesh Does It
- Environments are "virtual" — they reference production tables for unchanged models
- `sqlmesh plan dev` shows what will change, categorizes impacts, calculates backfill needs
- `sqlmesh apply` executes the approved plan
- Environments have TTL-based expiration (auto-cleanup)
- Three suffix strategies: schema-level, catalog-level, table-level isolation

### Seeknal Adaptation
- Seeknal already has `draft/dry-run/apply` — extend this into full environment management
- `seeknal plan --env dev` builds a plan showing:
  - New nodes to create
  - Modified nodes with SQL diffs
  - Schema changes (columns added/removed/changed)
  - Downstream impact assessment
  - Estimated backfill time
- `seeknal apply --env dev` executes the plan
- `seeknal promote dev -> prod` promotes environment to production
- Environments stored in `target/environments/{name}/` with metadata
- Virtual: unchanged nodes point to production output files (symlinks or references)

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Safe deployments, rollback capability |
| ML Engineer | Experiment with feature definitions without breaking prod |
| Analytics Engineer | Test SQL changes in isolation |

### Complexity: HIGH (2-3 weeks)
Requires: environment state management, virtual table references, promotion workflow, TTL cleanup.

---

## Feature 2: Content-Based Fingerprinting & Smart Incremental Execution

### What
Hash node content (SQL, config, schema) into fingerprints. Only re-execute nodes whose content or upstream dependencies actually changed. Track completed time intervals for time-series data. Fix Seeknal's currently broken incremental execution.

### How SQLMesh Does It
- `SnapshotFingerprint`: combines `data_hash` (SQL/logic) + `metadata_hash` (config) + `parent_data_hash` + `parent_metadata_hash`
- Change categories: BREAKING, NON_BREAKING, FORWARD_ONLY, INDIRECT_BREAKING, METADATA
- Interval tracking: stores `(start_ts, end_ts)` tuples for completed processing windows
- Backfill calculator: identifies missing intervals and only processes those

### Seeknal Adaptation
- Extend current `state.py` NodeState with:
  ```python
  @dataclass
  class NodeFingerprint:
      content_hash: str      # SHA256 of SQL/Python/config
      schema_hash: str       # SHA256 of output schema
      upstream_hash: str     # Combined hash of all input fingerprints
      config_hash: str       # SHA256 of node config (minus SQL)
  ```
- Fingerprint comparison determines execution:
  - Same fingerprint + same upstream -> SKIP (cached)
  - Different content_hash -> RE-EXECUTE this node
  - Different upstream_hash -> RE-EXECUTE (upstream changed)
  - Different config_hash only -> METADATA change (may skip data rebuild)
- **Critical fix**: Persist source outputs to `target/sources/{name}.parquet`
  - When source is cached, load from parquet instead of re-querying
  - Transforms can always find their input data
  - Solves the DuckDB in-memory view disappearing problem
- Add interval tracking for time-series nodes:
  ```yaml
  kind: source
  name: events
  schedule: "0 * * * *"  # hourly
  time_column: event_time
  ```
  State tracks: `completed_intervals: [(2026-01-01T00:00, 2026-01-01T01:00), ...]`

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Reliable incremental runs, no wasted compute |
| ML Engineer | Faster iteration (only recompute changed features) |
| Analytics Engineer | Confidence that results are fresh |

### Complexity: HIGH (2-3 weeks)
Requires: fingerprint computation, persisted outputs, interval tracking, backfill logic.

### Priority: HIGHEST
This is the most critical feature — without it, `seeknal run` is unreliable for production use.

---

## Feature 3: Column-Level Lineage

### What
Trace how each column flows through the DAG — from source columns through SQL transforms to feature group outputs and exposures. Power impact analysis ("if I change this column, what breaks downstream?").

### How SQLMesh Does It
- Uses SQLGlot's `lineage()` function to parse SQL and trace column references
- Builds lineage trees: `source.user_id -> transform.clean_user_id -> feature_group.user_id`
- `column_dependencies()` maps each output column to its upstream sources
- Integrated into plan explanations and UI visualization

### Seeknal Adaptation
- Parse SQL in transform nodes to extract column references
- Build column-level dependency graph alongside the node-level DAG
- Store in manifest:
  ```python
  @dataclass
  class ColumnLineage:
      node_id: str
      column: str
      upstream: list[tuple[str, str]]  # [(node_id, column), ...]
  ```
- CLI commands:
  - `seeknal lineage transform.clean_users.user_id` -> shows full column path
  - `seeknal impact source.events.event_type` -> shows all downstream columns affected
- Integrate with `seeknal plan` to show column-level diffs:
  ```
  transform.clean_users:
    + new_column: derived from source.events.raw_value
    ~ renamed_col: was 'old_name', now 'new_name'
    - dropped_col: WARNING: used by feature_group.user_features.dropped_col
  ```

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Impact analysis before schema changes |
| ML Engineer | Feature provenance (what data feeds this feature?) |
| Analytics Engineer | Understand data flow without reading SQL |

### Complexity: MEDIUM (1-2 weeks)
Requires: SQL parsing (SQLGlot or sqlparse), lineage graph construction, CLI integration.

---

## Feature 4: Built-in Data Quality Audits

### What
Attach audit rules directly to nodes. Run automatically during pipeline execution. Block or warn on failures. Seeknal already has a basic `rule` node type — this upgrades it to inline audits attached to any node.

### How SQLMesh Does It
- Audits attached to MODEL definition:
  ```sql
  MODEL (name my_model, audits (
    UNIQUE_VALUES(columns=(user_id)),
    NOT_NULL(columns=(amount, user_id)),
    ACCEPTED_VALUES(column=status, is_in=('active', 'inactive')),
  ));
  ```
- Built-in audit types: UNIQUE_VALUES, NOT_NULL, ACCEPTED_VALUES, NUMBER_OF_ROWS, FORALL
- Custom SQL audits: any SQL that returns failing rows
- Audits run automatically after backfill
- Blocking (fail pipeline) vs non-blocking (warn only)

### Seeknal Adaptation
- Add `audits:` section to any YAML node:
  ```yaml
  kind: transform
  name: clean_users
  sql: "SELECT * FROM {{ ref('source.raw_users') }} WHERE age > 0"
  audits:
    - type: not_null
      columns: [user_id, email]
    - type: unique
      columns: [user_id]
    - type: accepted_values
      column: status
      values: [active, inactive, pending]
    - type: row_count
      min: 1000
      max: 10000000
    - type: custom_sql
      sql: "SELECT * FROM __THIS__ WHERE age < 0 OR age > 150"
      severity: warn  # warn or error
  ```
- Execute audits after each node completes:
  - `error` severity: fail the node, stop pipeline (or continue if `--continue-on-error`)
  - `warn` severity: log warning, continue execution
- Store audit results in state:
  ```python
  @dataclass
  class AuditResult:
      audit_type: str
      passed: bool
      failing_rows: int
      severity: str  # "warn" | "error"
      message: str
  ```
- CLI: `seeknal audit` runs all audits, `seeknal audit transform.clean_users` runs specific node's audits

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Quality gates prevent bad data from propagating |
| ML Engineer | Training data integrity guarantees |
| Analytics Engineer | Trust in dashboard numbers |

### Complexity: MEDIUM (1-2 weeks)
Requires: audit execution engine, built-in audit types, YAML schema extension, state tracking.

---

## Feature 5: Auto-Generated Unit Tests

### What
Capture real query results as test fixtures. Generate tests from actual data. Run `seeknal test` to catch regressions. Make testing frictionless instead of manual.

### How SQLMesh Does It
- `sqlmesh create_test model_name` captures live query results
- Generates YAML test files:
  ```yaml
  test_clean_users:
    model: clean_users
    inputs:
      raw_users:
        - {user_id: 1, name: "Alice", age: 30}
        - {user_id: 2, name: "Bob", age: -1}
    outputs:
      - {user_id: 1, name: "Alice", age: 30}
      # Bob filtered out (age < 0)
  ```
- `sqlmesh test` runs all tests in isolation (mock inputs, verify outputs)
- Tests are deterministic — no external data dependencies

### Seeknal Adaptation
- `seeknal create-test transform.clean_users` workflow:
  1. Execute the node with current data
  2. Capture input DataFrames (sample N rows from each input)
  3. Capture output DataFrame (sample N rows)
  4. Write to `tests/generated/test_transform_clean_users.yml`
- Test file format:
  ```yaml
  test: transform.clean_users
  description: "Auto-generated from live data on 2026-02-08"
  inputs:
    source.raw_users:
      rows:
        - {user_id: 1, name: "Alice", email: "alice@example.com", age: 30}
        - {user_id: 2, name: "Bob", email: null, age: 25}
    source.config:
      rows:
        - {key: "min_age", value: "18"}
  expected_output:
    rows:
      - {user_id: 1, name: "Alice", email: "alice@example.com", age: 30}
      - {user_id: 2, name: "Bob", email: null, age: 25}
    row_count: 2
    columns: [user_id, name, email, age]
  ```
- `seeknal test` runs all tests:
  1. Load mock inputs into DuckDB
  2. Execute node SQL against mock data
  3. Compare output with expected (row-level diff)
  4. Report pass/fail with detailed diff on failure
- `seeknal test --update` re-captures expected outputs (when intentional changes happen)

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Regression testing without manual fixture creation |
| ML Engineer | Verify feature logic doesn't drift |
| Analytics Engineer | Confidence when modifying shared transforms |

### Complexity: MEDIUM (1-2 weeks)
Requires: test runner, fixture capture, YAML test format, diff comparison.

---

## Feature 6: SQL Transpilation via SQLGlot

### What
Write SQL in any dialect (Postgres, Snowflake, BigQuery, DuckDB), auto-transpile to the target engine. Currently Seeknal only runs DuckDB SQL. This unlocks multi-engine portability.

### How SQLMesh Does It
- Deep integration with SQLGlot (same creator)
- Models declare their dialect: `MODEL (name x, dialect snowflake)`
- Rendering auto-transpiles to target engine dialect
- Supports 15+ dialects with full function mapping
- Handles dialect-specific quirks (date functions, string operations, etc.)

### Seeknal Adaptation
- Add optional `dialect:` field to transform nodes:
  ```yaml
  kind: transform
  name: user_metrics
  dialect: postgres  # Write in Postgres SQL
  sql: |
    SELECT user_id,
           DATE_TRUNC('month', created_at) as signup_month,
           NOW() - created_at as account_age
    FROM {{ ref('source.users') }}
  ```
- Transpilation layer between YAML parsing and DuckDB execution:
  1. Parse SQL with SQLGlot using declared dialect
  2. Transpile to DuckDB dialect for local execution
  3. Optionally transpile to production dialect for deployment
- Profile-based target engine:
  ```yaml
  # profiles.yml
  dev:
    engine: duckdb
  staging:
    engine: postgres
    connection: postgresql://...
  prod:
    engine: snowflake
    account: ...
  ```
- `seeknal render transform.user_metrics --target snowflake` shows transpiled SQL

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Write once, deploy to any warehouse |
| Analytics Engineer | Use familiar SQL dialect (Postgres, BigQuery) |
| ML Engineer | Same feature definitions work across environments |

### Complexity: LOW-MEDIUM (1 week)
Requires: SQLGlot dependency (already pure Python), dialect detection, transpilation pipeline.
Note: SQLGlot is well-maintained and handles most cases. Edge cases need dialect-specific testing.

---

## Feature 7: Scheduled Runs with Interval Tracking

### What
Cron-based scheduling with automatic interval tracking. Know exactly which time ranges have been processed and which are missing. Support backfill and restatement.

### How SQLMesh Does It
- Models declare `cron` schedules: `MODEL (name x, cron '@hourly')`
- IntervalUnit auto-inferred from cron (YEAR, MONTH, DAY, HOUR, etc.)
- Scheduler calculates `merged_missing_intervals()` — which time windows need processing
- Backfill: `sqlmesh plan --start 2025-01-01 --end 2025-12-31` reprocesses specific ranges
- Restatement: mark intervals as needing reprocessing without full rebuild

### Seeknal Adaptation
- Add scheduling metadata to nodes:
  ```yaml
  kind: source
  name: daily_events
  schedule: "0 6 * * *"  # daily at 6am
  time_column: event_date
  lookback: 3 days  # reprocess last 3 days each run
  ```
- Interval state tracking:
  ```python
  @dataclass
  class IntervalState:
      completed: list[tuple[datetime, datetime]]  # processed ranges
      pending: list[tuple[datetime, datetime]]     # waiting to process
      restatement: list[tuple[datetime, datetime]] # marked for reprocessing
  ```
- CLI commands:
  - `seeknal run` processes missing intervals only
  - `seeknal run --start 2025-01-01 --end 2025-06-30` backfill specific range
  - `seeknal invalidate source.events --start 2026-01-01` mark for restatement
  - `seeknal check-intervals` show what's been processed and what's missing
- Scheduler integration (future):
  - Export to crontab / systemd timer / Airflow DAG
  - Or built-in lightweight scheduler daemon

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Know exactly what's processed, backfill confidently |
| ML Engineer | Feature freshness SLAs, know training data coverage |
| Analytics Engineer | Understand data recency without checking manually |

### Complexity: HIGH (2-3 weeks)
Requires: cron parsing, interval algebra, state persistence, backfill logic.

---

## Feature 8: Model Change Categorization (Breaking vs Non-Breaking)

### What
Automatically classify node changes as BREAKING (downstream rebuild needed), NON_BREAKING (only this node), or METADATA (no data impact). Show clear impact assessment before execution.

### How SQLMesh Does It
- `SnapshotChangeCategory`: BREAKING, NON_BREAKING, FORWARD_ONLY, INDIRECT_BREAKING, METADATA
- Analyzes SQL AST to determine change type:
  - Added column -> NON_BREAKING
  - Removed column used downstream -> BREAKING
  - Changed filter condition -> BREAKING
  - Changed description/tags -> METADATA
  - Changed sort order -> NON_BREAKING
- Shows in plan output with propagation visualization

### Seeknal Adaptation
- Integrate with `seeknal plan`:
  ```
  $ seeknal plan --env dev

  Changes detected:

  transform.clean_users [NON_BREAKING]
    ~ SQL changed: added new column 'signup_month'
    Downstream impact: none (new column, not used downstream yet)

  transform.user_metrics [BREAKING]
    ~ SQL changed: removed column 'legacy_score'
    Downstream impact:
      - feature_group.user_features (uses legacy_score) -> REBUILD
      - aggregation.daily_scores (uses legacy_score) -> REBUILD
      - exposure.api (serves legacy_score) -> UPDATE

  source.events [METADATA]
    ~ description changed
    Downstream impact: none (data unchanged)

  Plan summary:
    1 non-breaking change (no downstream rebuild)
    1 breaking change (2 downstream nodes to rebuild)
    1 metadata change (no action needed)
  ```
- Change classification rules:
  - Column added -> NON_BREAKING
  - Column removed + used downstream -> BREAKING
  - Column removed + NOT used downstream -> NON_BREAKING
  - SQL logic changed -> BREAKING (conservative default)
  - Config-only changed (description, tags, owner) -> METADATA
  - Schema type change (widening: int->bigint) -> NON_BREAKING
  - Schema type change (narrowing: bigint->int) -> BREAKING

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Know exactly what will break before deploying |
| ML Engineer | Understand if feature changes affect downstream models |
| Analytics Engineer | Confident schema evolution |

### Complexity: MEDIUM (1-2 weeks)
Requires: SQL AST comparison, downstream usage analysis, change propagation logic.
Builds on: Feature 3 (column-level lineage) for accurate impact analysis.

---

## Feature 9: Parallel Node Execution

### What
Run independent nodes concurrently using thread/process pools. Seeknal currently executes nodes sequentially even when they have no dependencies on each other.

### How SQLMesh Does It
- Scheduler uses `ThreadPoolExecutor` for concurrent snapshot evaluation
- Topological sort identifies independent groups (same DAG level)
- Nodes within the same level run in parallel
- Dependency barriers between levels ensure correctness
- Configurable concurrency: `max_workers` setting

### Seeknal Adaptation
- Group nodes by DAG level (topological layers):
  ```
  Level 0: [source.users, source.transactions, source.events]  # parallel
  Level 1: [transform.clean_users, transform.enrich_events]     # parallel (after level 0)
  Level 2: [feature_group.user_features]                        # after level 1
  Level 3: [aggregation.daily_metrics]                          # after level 2
  ```
- Execution modes:
  ```yaml
  # seeknal_project.yml or CLI flag
  execution:
    parallel: true
    max_workers: 4  # or 'auto' (CPU count)
  ```
- Implementation:
  ```python
  from concurrent.futures import ThreadPoolExecutor, as_completed

  for level_nodes in dag.topological_layers():
      with ThreadPoolExecutor(max_workers=config.max_workers) as pool:
          futures = {pool.submit(execute_node, node): node for node in level_nodes}
          for future in as_completed(futures):
              result = future.result()  # raises on failure
  ```
- Important consideration: DuckDB connections are single-threaded
  - Option A: Separate DuckDB connection per worker (memory cost)
  - Option B: Single connection with serialized DuckDB access but parallel I/O
  - Option C: Use process pool instead of thread pool for true parallelism
- CLI: `seeknal run --parallel` or `seeknal run --workers 4`

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | 2-5x faster pipeline execution on multi-source pipelines |
| ML Engineer | Faster feature computation cycles |
| Analytics Engineer | Quicker feedback on changes |

### Complexity: MEDIUM (1-2 weeks)
Requires: topological layering, thread/process pool, DuckDB connection management, error propagation.

---

## Feature 10: Janitor & Lifecycle Management

### What
Automatic cleanup of expired environments, stale outputs, orphaned artifacts, and old state files. TTL-based expiration. Prevents storage bloat and state corruption.

### How SQLMesh Does It
- `Janitor` class with `cleanup_expired_views()` and `delete_expired_snapshots()`
- TTL-based environment expiration (configurable per environment)
- Runs automatically as part of scheduler
- Batch deletion for performance
- Cascade cleanup (removes schemas/catalogs when empty)

### Seeknal Adaptation
- `seeknal janitor` command:
  ```
  $ seeknal janitor

  Cleaning up...
    - Removed 3 expired dev environments (>7 days old)
    - Removed 12 orphaned output files (156 MB recovered)
    - Removed 5 stale state entries (nodes no longer in DAG)
    - Compressed run history (kept last 30 runs, archived 45)

  Storage recovered: 312 MB
  ```
- Configuration:
  ```yaml
  # seeknal_project.yml
  janitor:
    environment_ttl: 7d        # Auto-delete dev environments after 7 days
    output_retention: 30        # Keep last 30 runs of output files
    state_history: 90d          # Keep state history for 90 days
    auto_run: true              # Run janitor at end of each `seeknal run`
    orphan_detection: true      # Detect outputs for nodes no longer in DAG
  ```
- What gets cleaned:
  - `target/environments/dev-*` older than TTL
  - `target/sources/*.parquet` not referenced by current state
  - `target/run_state.json` entries for deleted nodes
  - `target/history/` compressed to archive after retention period
  - Iceberg expired snapshots (via catalog API)

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | No manual cleanup, predictable storage growth |
| ML Engineer | Clean experiment artifacts |
| Analytics Engineer | No confusion from stale dev environments |

### Complexity: LOW (3-5 days)
Requires: file age detection, state cross-referencing, safe deletion with dry-run preview.

---

## Feature 11: StarRocks Integration & Semantic Layer

### What
Connect Seeknal to StarRocks as a high-performance OLAP serving layer and add a semantic layer that defines metrics, dimensions, and measures — enabling self-service analytics with consistent metric definitions across BI tools.

### Why StarRocks
- **Sub-second analytics** on billions of rows via MPP + vectorized execution + CBO
- **MySQL-compatible wire protocol** (port 9030) — any MySQL Python driver works (`pip install starrocks` for SQLAlchemy dialect, or `pymysql`)
- **Native Iceberg external catalogs** — query Seeknal's Iceberg tables directly without data copying (REST catalog supported, integration-tested with Lakekeeper)
- **Async materialized views** — auto-refresh, multi-table joins, transparent query rewriting, partition-level incremental refresh on Iceberg tables
- **Zero-copy lakehouse pattern**: DuckDB writes Iceberg -> StarRocks reads Iceberg via external catalog -> no ETL between them

### Seeknal Adaptation

#### Part A: StarRocks as Connection & Query Engine

**Connection profile** (`profiles.yml`):
```yaml
connections:
  reporting:
    type: starrocks
    host: starrocks-fe.internal
    port: 9030                    # MySQL protocol port
    http_port: 8030               # For Stream Load (data ingestion)
    user: seeknal
    password: ${STARROCKS_PASSWORD}
    database: analytics
    catalog: iceberg_catalog      # Optional: default external catalog
```

**Python driver**: Use official `starrocks` package (v1.3.3, SQLAlchemy dialect) or `pymysql` for lightweight access. Connection string: `starrocks://user:pass@host:9030/[catalog.]database`

**Note**: `starrocks` package currently has limited SQLAlchemy 2.0 support (tracked in StarRocks issue #50819). Use `pymysql` as fallback.

**Integration points**:

1. **REPL** — query StarRocks interactively:
   ```bash
   seeknal repl --connection reporting
   > SELECT user_id, COUNT(*) FROM events GROUP BY 1 LIMIT 10;
   ```

2. **Source nodes** — read from StarRocks tables:
   ```yaml
   kind: source
   name: live_transactions
   type: starrocks
   connection: reporting
   query: "SELECT * FROM transactions WHERE dt >= CURDATE() - INTERVAL 7 DAY"
   ```

3. **Exposure nodes** — create StarRocks materialized views on Iceberg data:
   ```yaml
   kind: exposure
   name: user_dashboard
   type: starrocks_materialized_view
   connection: reporting
   sql: |
     SELECT user_id, feature_a, feature_b, updated_at
     FROM iceberg_catalog.seeknal.user_features
   config:
     distributed_by: [user_id]
     partition_by: "date_trunc('day', updated_at)"
     refresh: "ASYNC EVERY(INTERVAL 5 MINUTE)"
     properties:
       partition_ttl: "2 MONTH"
       mv_rewrite_staleness_second: "300"
       force_external_table_query_rewrite: "true"
   ```

4. **Iceberg catalog setup** — `seeknal starrocks setup-catalog`:
   ```sql
   -- Generated SQL for StarRocks admin
   CREATE EXTERNAL CATALOG seeknal_iceberg
   PROPERTIES (
       "type" = "iceberg",
       "iceberg.catalog.type" = "rest",
       "iceberg.catalog.uri" = "http://lakekeeper:8181/catalog",
       "iceberg.catalog.warehouse" = "seeknal-warehouse",
       "aws.s3.access_key" = "${MINIO_ACCESS_KEY}",
       "aws.s3.secret_key" = "${MINIO_SECRET_KEY}",
       "aws.s3.endpoint" = "http://minio:9000",
       "aws.s3.enable_path_style_access" = "true"
   );
   ```

**Data flow pattern** (zero-copy lakehouse):
```
DuckDB (transform) -> PyIceberg (write) -> Lakekeeper (catalog)
                                                  ↓
StarRocks External Catalog (read) <- REST API <- Lakekeeper
         ↓
StarRocks Async MV (acceleration) -> BI Tools
```

No data copying between DuckDB and StarRocks — both read/write via shared Iceberg catalog.

#### Part B: Semantic Layer

Inspired by **dbt MetricFlow** (YAML-first, SQL compilation) and **Cube.dev** (pre-aggregation, BI integration), adapted for Seeknal's feature-store context.

**New node type**: `semantic_model` and `metric` (stored in `seeknal/semantic_models/` and `seeknal/metrics/`)

**Semantic Model** — maps a Seeknal node to business entities, dimensions, and measures:
```yaml
# seeknal/semantic_models/orders.yml
kind: semantic_model
name: orders
description: "Order fact table"
model: ref('transform.order_summary')     # Reference to any Seeknal node
default_time_dimension: ordered_at

entities:
  - name: order_id
    type: primary                          # primary | foreign | unique
  - name: customer_id
    type: foreign
  - name: product_id
    type: foreign

dimensions:
  - name: ordered_at
    type: time                             # time | categorical
    expr: ordered_at
    time_granularity: day                  # day | week | month | quarter | year
  - name: region
    type: categorical
    expr: region
  - name: status
    type: categorical
    expr: status

measures:
  - name: revenue
    expr: amount
    agg: sum                               # sum | count | count_distinct | average | min | max
    description: "Total order amount"
  - name: order_count
    expr: "1"
    agg: sum
  - name: avg_order_value
    expr: amount
    agg: average
  - name: unique_customers
    expr: customer_id
    agg: count_distinct
```

**Metric definitions** — business-level calculations composed from measures:
```yaml
# seeknal/metrics/revenue.yml
kind: metric
name: total_revenue
type: simple                               # simple | derived | ratio | cumulative
description: "Total revenue excluding refunds"
measure: revenue                           # References measure in semantic model
filter: "status != 'refunded'"             # Optional default filter

---
kind: metric
name: revenue_per_customer
type: ratio
description: "Average revenue per unique customer"
numerator: revenue
denominator: unique_customers

---
kind: metric
name: mtd_revenue
type: cumulative
description: "Month-to-date cumulative revenue"
measure: revenue
grain_to_date: month                       # Resets each month

---
kind: metric
name: revenue_growth_mom
type: derived
description: "Month-over-month revenue growth"
expr: "(current - previous) / previous"
inputs:
  - metric: total_revenue
    alias: current
  - metric: total_revenue
    alias: previous
    offset: 1 month
```

**Metric query via CLI**:
```bash
# Query metrics with dimensions and filters
seeknal query --metrics total_revenue,order_count \
  --dimensions region,ordered_at__month \
  --filter "ordered_at >= '2026-01-01'" \
  --order-by -total_revenue \
  --limit 20

# Show compiled SQL without executing
seeknal query --metrics total_revenue --dimensions region --compile

# Execute against specific connection (default: DuckDB, or StarRocks)
seeknal query --metrics total_revenue --dimensions region --connection reporting
```

**SQL compilation engine**:
1. Parse metric query (which metrics, dimensions, filters)
2. Resolve semantic models needed (metric -> measure -> semantic_model -> node)
3. Plan joins via entity relationships (foreign keys)
4. Generate optimized SQL:
   ```sql
   -- Compiled from: seeknal query --metrics total_revenue --dimensions region,ordered_at__month
   SELECT
     region,
     DATE_TRUNC('month', ordered_at) AS ordered_at__month,
     SUM(amount) AS total_revenue
   FROM (SELECT * FROM transform_order_summary WHERE status != 'refunded') AS orders
   GROUP BY region, DATE_TRUNC('month', ordered_at)
   ORDER BY total_revenue DESC
   ```
5. Execute on DuckDB (dev) or push down to StarRocks (prod)

**BI tool integration**:
- `seeknal serve-metrics --port 5433` exposes metrics via Postgres wire protocol
- Metabase/Superset/Grafana connect as a Postgres data source
- Alternatively, generate StarRocks materialized views from metric definitions:
  ```bash
  seeknal deploy-metrics --connection reporting
  # Creates StarRocks MVs for each metric, BI tools query StarRocks directly
  ```

#### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Seeknal Pipeline (DuckDB)                     │
│  sources -> transforms -> feature_groups -> aggregations        │
│                          ↓                                       │
│              Iceberg Materialization (PyIceberg + Lakekeeper)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ (shared Iceberg catalog, zero-copy)
┌──────────────────────────▼──────────────────────────────────────┐
│                    StarRocks Serving Layer                       │
│                                                                  │
│  External Catalog ──→ Iceberg tables (seeknal.*)                │
│       ↓                                                          │
│  Async Materialized Views (auto-refresh, partition-incremental) │
│       ↓                                                          │
│  CBO + Vectorized Engine (sub-second queries)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                    Semantic Layer                                 │
│                                                                  │
│  seeknal/semantic_models/*.yml   (entities, dimensions, measures)│
│  seeknal/metrics/*.yml           (simple, derived, ratio, cum.) │
│       ↓                                                          │
│  SQL Compiler (metric query -> optimized SQL)                   │
│       ↓                                                          │
│  Execution Target: DuckDB (dev) | StarRocks (prod)              │
│       ↓                                                          │
│  BI Integration: Postgres wire protocol | StarRocks MVs         │
│       ↓                                                          │
│  Metabase / Superset / Grafana / Notebooks                      │
└─────────────────────────────────────────────────────────────────┘
```

### Impact
| Persona | Value |
|---------|-------|
| Data Engineer | Full pipeline: transform (DuckDB) -> materialize (Iceberg) -> serve (StarRocks), zero-copy |
| ML Engineer | Query features at scale via StarRocks for training data extraction, consistent metric definitions for feature monitoring |
| Analytics Engineer | Self-service metrics with `seeknal query`, sub-second dashboards via StarRocks MVs, one source of truth for metric definitions |

### Complexity: HIGH (4-5 weeks total)

| Sub-feature | Effort | Dependencies |
|-------------|--------|-------------|
| Part A: StarRocks connection (pymysql/starrocks driver) | 3-4 days | profiles.yml extension |
| Part A: REPL StarRocks support | 2-3 days | Connection profile |
| Part A: Source/Exposure StarRocks node types | 1 week | Executor framework |
| Part A: Iceberg catalog setup CLI | 2-3 days | Existing Iceberg materialization |
| Part B: Semantic model YAML schema + parser | 3-4 days | DAG manifest extension |
| Part B: Metric definitions + type system | 3-4 days | Semantic model parser |
| Part B: SQL compilation engine | 1 week | Semantic graph, entity join resolution |
| Part B: `seeknal query` CLI command | 2-3 days | SQL compiler |
| Part B: BI integration (Postgres protocol or MV deploy) | 1 week | SQL compiler, StarRocks connection |

### Technical References
- **StarRocks Python**: `pip install starrocks` (v1.3.3, SQLAlchemy dialect) or `pymysql`
- **StarRocks Iceberg REST catalog**: Lakekeeper integration-tested, use `iceberg.catalog.type = "rest"`
- **StarRocks MVs on Iceberg**: Support partition-level incremental refresh (v3.1.4+), transparent query rewriting
- **Semantic layer inspiration**: dbt MetricFlow (YAML schema, metric types, SQL compilation), Cube.dev (pre-aggregation, BI serving)
- **Metric types**: simple (direct agg), derived (formula over metrics), ratio (numerator/denominator), cumulative (running total/window)

---

## Priority Matrix

| # | Feature | Impact | Complexity | Priority |
|---|---------|--------|------------|----------|
| 2 | Content-Based Fingerprinting & Incremental | Critical | High | P0 - Must have |
| 4 | Built-in Data Quality Audits | High | Medium | P0 - Must have |
| 1 | Virtual Environments & Plan/Apply | High | High | P1 - Should have |
| 8 | Change Categorization (Breaking/Non-Breaking) | High | Medium | P1 - Should have |
| 9 | Parallel Node Execution | High | Medium | P1 - Should have |
| 5 | Auto-Generated Unit Tests | Medium-High | Medium | P1 - Should have |
| 3 | Column-Level Lineage | Medium-High | Medium | P2 - Nice to have |
| 6 | SQL Transpilation (SQLGlot) | Medium | Low-Medium | P2 - Nice to have |
| 11 | StarRocks + Semantic Layer | High | High | P2 - Nice to have |
| 7 | Scheduled Runs + Interval Tracking | Medium | High | P3 - Future |
| 10 | Janitor & Lifecycle Management | Low-Medium | Low | P3 - Future |

### Recommended Implementation Order

**Phase 1 (Foundation)**: Features 2, 4 (4-5 weeks)
- Fix incremental execution (blocker for everything else)
- Add data quality audits (immediate value, low risk)

**Phase 2 (Safety & Speed)**: Features 1, 8, 9 (4-5 weeks)
- Virtual environments + plan/apply (safe deployments)
- Change categorization (builds on environment work)
- Parallel execution (performance wins)

**Phase 3 (Developer Experience)**: Features 5, 3, 6 (3-4 weeks)
- Auto-generated tests (testing story)
- Column-level lineage (understanding + impact analysis)
- SQL transpilation (multi-engine portability)

**Phase 4 (Production Platform)**: Features 11, 7, 10 (5-6 weeks)
- StarRocks + semantic layer (serving + reporting)
- Scheduled runs (production operations)
- Janitor (operational hygiene)

---

## Open Questions

1. **Environment storage**: File-based (Parquet in target/) vs database-backed (SQLite state)?
2. **Semantic layer scope**: Full MetricFlow-like system (entity graph, join planning) or lightweight metric definitions (direct SQL generation)?
3. **StarRocks driver**: Official `starrocks` SQLAlchemy dialect (richer DDL support, limited SQLAlchemy 2.0) vs `pymysql` (lightweight, no SQLAlchemy dependency)?
4. **SQLGlot depth**: Full transpilation (write any dialect) or just validation/linting?
5. **Parallel execution**: Thread pool (shared DuckDB) or process pool (isolated DuckDB instances)?
6. **Test format**: YAML fixtures (SQLMesh-style) or Python-based tests (pytest)?
7. **Audit severity**: Should default be `warn` or `error` for new audits?
8. **Interval tracking granularity**: Per-node or global pipeline level?
9. **BI serving approach**: Postgres wire protocol proxy (like Cube) vs deploying StarRocks MVs (simpler, but requires StarRocks)?
10. **Metric query target**: Always DuckDB locally, always StarRocks remotely, or smart routing based on data size?

---

## Key Decisions Made

- **All 3 personas equally weighted** — features must serve data engineers, ML engineers, and analytics engineers
- **Feature 2 (incremental) is P0** — current broken incremental execution is the biggest blocker
- **StarRocks chosen as OLAP engine** — MySQL-compatible protocol, native Iceberg REST catalog support (Lakekeeper integration-tested), async MVs with partition-level incremental refresh on Iceberg tables, and zero-copy lakehouse pattern pairs naturally with Seeknal's existing Iceberg materialization
- **Zero-copy lakehouse architecture** — DuckDB writes to Iceberg via Lakekeeper, StarRocks reads the same Iceberg tables via external catalog. No ETL between compute engines.
- **Semantic layer inspired by dbt MetricFlow + Cube** — YAML-first metric definitions (MetricFlow), SQL compilation to target engine, optional pre-aggregation via StarRocks MVs (Cube pattern)
- **Four metric types**: simple (direct aggregation), derived (formula over metrics), ratio (numerator/denominator), cumulative (running total/window) — covers 95% of analytics use cases
- **SQLGlot for transpilation** — pure Python, well-maintained, same library SQLMesh uses
- **Phase-based rollout** — foundation first (fix what's broken), then safety, then DX, then production platform
