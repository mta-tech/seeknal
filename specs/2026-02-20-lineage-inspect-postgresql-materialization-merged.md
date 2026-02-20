---
title: "Lineage & Inspect E2E + PostgreSQL Source & Materialization - Merged Implementation Plan"
type: feat
date: 2026-02-20
status: ready
merge_sources:
  - docs/plans/2026-02-20-test-lineage-inspect-iceberg-scenarios-plan.md
  - docs/plans/2026-02-20-feat-postgresql-source-materialization-plan.md
---

# Plan: Lineage & Inspect E2E + PostgreSQL Source & Materialization

## Overview

This merged plan covers two major workstreams:

**Workstream A — Lineage & Inspect E2E Test Project**: Create a dedicated test project at `~/project/self/bmad-new/seeknal-v2.1-test-lineage-inspect/` that validates `seeknal lineage` and `seeknal inspect` commands across both YAML and Python pipeline definitions, using Iceberg sources and materialization throughout. Uses existing bronze data from `atlas.ref_common_test` namespace on atlas-dev-server.

**Workstream B — PostgreSQL Source Pushdown & Multi-Target Materialization**: Add full PostgreSQL support to Seeknal: enhanced source reading with pushdown queries, PostgreSQL as a materialization target (full/incremental/upsert modes), multi-target materialization (`materializations:` plural), connection profiles, and a stackable `@materialize` Python decorator. All via DuckDB's native postgres extension — zero new dependencies.

**Workstream C — QA Medallion Pipeline E2E (Multi-Source)**: Create a comprehensive test project demonstrating a full medallion architecture (Bronze → Silver → Gold) using all three source types: CSV files, Iceberg (atlas-dev-server), and PostgreSQL (local). Tests both YAML and Python pipelines with multi-target materialization to PostgreSQL and Iceberg.

**Key Deliverables:**
- 10-node test project validating lineage/inspect across YAML + Python + Iceberg
- PostgreSQL connection factory with profile-based config
- PostgreSQL source pushdown query support (`query:` field)
- PostgreSQL materialization helper (full/incremental/upsert modes)
- Multi-target materialization dispatcher (`materializations:` plural YAML key)
- Stackable `@materialize` Python decorator
- 70+ new unit tests for PostgreSQL features
- QA medallion test project exercising CSV + Iceberg + PostgreSQL sources with multi-target materialization

**Architecture Note:**
- DuckDB remains the compute engine for all transformations
- DuckDB's `postgres` extension serves as the bridge for all PostgreSQL reads/writes (zero new dependencies)
- Binary COPY protocol for writes, automatic filter/projection pushdown for reads

## Task Description

### Workstream A: Lineage & Inspect E2E

The existing test projects test either YAML pipelines (`seeknal-v2.1-test-ref-common`) or lineage visualization (`e2e_lineage_test`), but none specifically validate the full flow:

1. `seeknal run` with mixed YAML + Python nodes + Iceberg materialization
2. `seeknal inspect` reading intermediate outputs from both YAML and Python nodes
3. `seeknal lineage` generating correct DAG visualization with cross-referenced YAML/Python nodes
4. Column-level lineage tracing through SQL transforms

The test project includes 10 nodes spanning 4 types (source, transform, feature_group, exposure), with cross-references covering all 4 patterns: YAML→YAML, YAML→Python, Python→YAML, Python→Python.

### Workstream B: PostgreSQL Source & Materialization

Seeknal's current PostgreSQL source support is basic — it `ATTACH`es and does `SELECT *`, pulling all data locally. There is no way to push SQL to PostgreSQL (e.g., filtering/aggregating before transfer) and no way to write results back to PostgreSQL. The materialization system only targets Iceberg.

Users need to:
1. Read efficiently from PostgreSQL — large tables should not be fully scanned
2. Write results to PostgreSQL — analytics tables, feature stores, or derived datasets
3. Write to multiple targets — e.g., both PostgreSQL (for serving) and Iceberg (for lakehouse)

### Workstream C: QA Medallion Pipeline E2E (Multi-Source)

After PostgreSQL features are built (Workstream B), validate the full capability set with a real-world medallion data pipeline that exercises all three source environments:

1. **CSV files** (local) — Bronze layer raw data ingestion
2. **Iceberg tables** (atlas-dev-server) — Bronze layer from lakehouse
3. **PostgreSQL** (local Docker container) — Bronze layer from OLTP database with pushdown queries

The pipeline follows a medallion architecture:
- **Bronze**: Raw data from 3 source types (CSV, Iceberg, PostgreSQL)
- **Silver**: Cleaned, enriched, joined data (both YAML and Python transforms)
- **Gold**: Aggregated business metrics
- **Materialization**: Multi-target writes to both PostgreSQL and Iceberg

## Objective

1. **Validate** that `seeknal lineage` and `seeknal inspect` work correctly across mixed YAML + Python pipelines with Iceberg materialization
2. **Add** PostgreSQL pushdown queries for efficient source reading
3. **Add** PostgreSQL as a materialization target with full/incremental/upsert modes
4. **Enable** multi-target materialization (write to both PostgreSQL and Iceberg from one node)
5. **Provide** a clean Python API (`@materialize` decorator) and YAML interface (`materializations:` list)
6. **Maintain** backward compatibility with existing `materialization:` (singular) Iceberg config
7. **Validate** all new capabilities with a real-world medallion pipeline using CSV + Iceberg + PostgreSQL sources and multi-target materialization

## Relevant Files

### Existing Files (To Modify)

| File | Purpose | Workstream |
|------|---------|------------|
| `src/seeknal/workflow/executors/source_executor.py` | PostgreSQL source loading + pushdown query | B |
| `src/seeknal/workflow/executors/transform_executor.py` | Post-execute hooks for materialization dispatch | B |
| `src/seeknal/workflow/dag.py` | YAML/Python parsing, `materializations:` plural | B |
| `src/seeknal/workflow/materialization/profile_loader.py` | Connection profiles, env var defaults | B |
| `src/seeknal/pipeline/decorators.py` | `@materialize` decorator, `@source` query param | B |
| `src/seeknal/pipeline/discoverer.py` | Python pipeline metadata propagation | B |
| `src/seeknal/connections/__init__.py` | Re-export PostgreSQL connection factory | B |
| `src/seeknal/workflow/materialization/__init__.py` | Re-export dispatcher | B |
| `tests/conftest.py` | PostgreSQL test fixtures | B |

### New Files to Create

| File | Purpose | Workstream |
|------|---------|------------|
| `src/seeknal/connections/postgresql.py` | PostgreSQL connection factory + config | B |
| `src/seeknal/workflow/materialization/postgresql.py` | PostgreSQL materialization helper | B |
| `src/seeknal/workflow/materialization/pg_config.py` | PostgreSQL materialization config model | B |
| `src/seeknal/workflow/materialization/dispatcher.py` | Multi-target materialization dispatcher | B |
| `tests/connections/test_postgresql.py` | Connection factory tests | B |
| `tests/workflow/test_source_postgresql.py` | Source pushdown tests | B |
| `tests/workflow/test_postgresql_materialization.py` | Materialization helper tests | B |
| `tests/workflow/test_materialization_dispatcher.py` | Dispatcher tests | B |
| `tests/pipeline/test_materialize_decorator.py` | Decorator tests | B |

### QA Medallion Test Project Files (Workstream C)

| File | Purpose |
|------|---------|
| `~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/.env` | PostgreSQL + Lakekeeper/MinIO credentials |
| `~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/profiles.yml` | Connection profiles (PostgreSQL + Iceberg) |
| `~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/docker-compose.yml` | Local PostgreSQL container |
| `~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/README.md` | Setup guide, test commands, expected results |
| `seeknal/data/` | CSV seed data files (Bronze layer) |
| `seeknal/sources/` | YAML sources: CSV files, Iceberg tables, PostgreSQL tables (with pushdown) |
| `seeknal/transforms/` | YAML transforms: Silver cleaning, Gold aggregation |
| `seeknal/pipelines/` | Python pipelines: Silver enrichment, Gold KPIs with `@materialize` |
| `seeknal/exposures/` | YAML exposure: final report |
| `scripts/seed_postgres.sql` | SQL to seed local PostgreSQL with bronze data |

### Test Project Files (Workstream A)

| File | Purpose |
|------|---------|
| `~/project/self/bmad-new/seeknal-v2.1-test-lineage-inspect/.env` | Lakekeeper/MinIO/Keycloak credentials |
| `~/project/self/bmad-new/seeknal-v2.1-test-lineage-inspect/profiles.yml` | Iceberg materialization config |
| `~/project/self/bmad-new/seeknal-v2.1-test-lineage-inspect/README.md` | How to run, test commands, expected results |
| `seeknal/common/sources.yml` | Shared column aliases |
| `seeknal/sources/traffic_daily.yml` | Iceberg source (YAML) |
| `seeknal/sources/subscriber_daily.yml` | Iceberg source (YAML) |
| `seeknal/transforms/clean_subscribers.yml` | Silver: subscriber cleanup (YAML) |
| `seeknal/transforms/active_traffic.yml` | Silver: join traffic + subscribers (YAML, cross-ref Python) |
| `seeknal/transforms/revenue_by_region.yml` | Gold: regional metrics (YAML) |
| `seeknal/feature_groups/subscriber_features.yml` | Feature group (YAML) |
| `seeknal/exposures/kpi_report.yml` | Exposure - file export (YAML) |
| `seeknal/pipelines/enrichment.py` | Python: cell_sites source + traffic_enriched + daily_kpi |

## Step by Step Tasks

### Phase A: Lineage & Inspect E2E Test Project

#### 1. Create test project scaffold
- **Task ID:** A-1
- **Depends On:** none
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** true (independent of Phase B)

Create the directory structure at `~/project/self/bmad-new/seeknal-v2.1-test-lineage-inspect/`:
- Copy `.env` from `seeknal-v2.1-test-ref-common/.env` (same Lakekeeper/MinIO credentials)
- Create `profiles.yml` with `atlas.lineage_test` default table prefix
- Create `seeknal/common/sources.yml` with column aliases from ref-common
- Create all directory stubs: `seeknal/sources/`, `seeknal/transforms/`, `seeknal/feature_groups/`, `seeknal/exposures/`, `seeknal/pipelines/`

**Acceptance Criteria:**
- [ ] Directory structure matches the folder structure spec
- [ ] `.env` contains valid Lakekeeper/MinIO/Keycloak credentials
- [ ] `profiles.yml` targets `atlas.lineage_test` namespace

#### 2. Create YAML source and transform nodes
- **Task ID:** A-2
- **Depends On:** A-1
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

Create YAML files:
- `seeknal/sources/traffic_daily.yml` — Iceberg source reading `atlas.ref_common_test.traffic_daily`
- `seeknal/sources/subscriber_daily.yml` — Iceberg source reading `atlas.ref_common_test.subscriber_daily`
- `seeknal/transforms/clean_subscribers.yml` — SQL: filter active, compute tenure_days. Materializes to `atlas.lineage_test.silver_clean_subscribers`
- `seeknal/transforms/active_traffic.yml` — SQL: JOIN traffic_enriched (from Python) with subscriber_daily, filter active. Uses `ref('transform.traffic_enriched')`. Materializes to `atlas.lineage_test.silver_active_traffic`
- `seeknal/transforms/revenue_by_region.yml` — SQL: GROUP BY region from active_traffic. Materializes to `atlas.lineage_test.gold_revenue_by_region`

**Acceptance Criteria:**
- [ ] All 5 YAML files created with valid syntax
- [ ] Sources reference `atlas.ref_common_test` tables
- [ ] Transforms use `ref()` named ref syntax for cross-references
- [ ] All transforms include `materialization:` config targeting `atlas.lineage_test`

#### 3. Create Python pipeline nodes
- **Task ID:** A-3
- **Depends On:** A-1
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** true (parallel with A-2)

Create `seeknal/pipelines/enrichment.py`:
- `@source(name="cell_sites", source="iceberg", table="atlas.ref_common_test.cell_sites")` with materialization
- `@transform(name="traffic_enriched")` — JOIN traffic_daily + cell_sites, adds region/technology. Materializes to `atlas.lineage_test.silver_traffic_enriched`
- `@transform(name="daily_kpi")` — Aggregate active_traffic by date+region, compute revenue/subscribers. Materializes to `atlas.lineage_test.gold_daily_kpi`

**Acceptance Criteria:**
- [ ] Python file uses `@source` and `@transform` decorators correctly
- [ ] Cross-references: Python source consumes from YAML source via `ctx.ref()`
- [ ] All transforms include materialization config

#### 4. Create feature group and exposure nodes
- **Task ID:** A-4
- **Depends On:** A-2
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

Create:
- `seeknal/feature_groups/subscriber_features.yml` — Feature group consuming clean_subscribers. Materializes to `atlas.lineage_test.fg_subscriber_features`
- `seeknal/exposures/kpi_report.yml` — File export depending on daily_kpi and revenue_by_region

**Acceptance Criteria:**
- [ ] Feature group has valid entity + features config
- [ ] Exposure references both daily_kpi and revenue_by_region as dependencies

#### 5. Create README with test commands
- **Task ID:** A-5
- **Depends On:** A-2, A-3, A-4
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

Write `README.md` documenting:
- Prerequisites (atlas-dev-server, SSH tunnel, ref-common data)
- Full test command sequence: `seeknal run`, `seeknal inspect`, `seeknal lineage`
- Expected results for each command
- Known limitations (Python column lineage not available)

**Acceptance Criteria:**
- [ ] README includes all test commands from the plan
- [ ] Documents known limitation about Python column lineage
- [ ] Includes expected output for each command

#### 6. Run and validate E2E flows
- **Task ID:** A-6
- **Depends On:** A-5
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

Execute the full test sequence:
1. `seeknal run --profile profiles.yml` — all 10 nodes execute
2. `seeknal inspect --list` — shows all 10 nodes
3. `seeknal inspect` on individual nodes — data previews work
4. `seeknal lineage --no-open` — generates HTML with 10 nodes, 4 types
5. `seeknal lineage transform.active_traffic --no-open` — focused subgraph
6. `seeknal lineage transform.revenue_by_region --column total_revenue --no-open` — column trace

**Acceptance Criteria:**
- [ ] `seeknal run --profile profiles.yml` executes all 10 nodes without errors
- [ ] All 3 Iceberg sources load data from `atlas.ref_common_test`
- [ ] All 5 transforms produce intermediate parquet files in `target/intermediate/`
- [ ] Iceberg materialization writes tables to `atlas.lineage_test` namespace
- [ ] `seeknal inspect --list` shows all 10 nodes with correct row/column counts
- [ ] `seeknal inspect source.traffic_daily` shows data preview (default 10 rows)
- [ ] `seeknal inspect transform.traffic_enriched --limit 5` shows limited preview
- [ ] `seeknal inspect transform.clean_subscribers --schema` shows column names + types
- [ ] `seeknal inspect transform.active_traffic` works (YAML node consuming Python output)
- [ ] `seeknal inspect transform.daily_kpi` works (Python node consuming YAML output)
- [ ] `seeknal lineage --no-open` generates `target/lineage.html` with 10 nodes
- [ ] HTML shows all 4 node types with correct colors
- [ ] `seeknal lineage transform.active_traffic --no-open` shows focused subgraph
- [ ] `seeknal lineage transform.revenue_by_region --column total_revenue --no-open` traces column to sources
- [ ] Cross-referenced edges (YAML→Python, Python→YAML) render correctly

---

### Phase B1: PostgreSQL Connection Factory & Profile Loader

#### 7. Create PostgreSQL connection factory
- **Task ID:** B-1
- **Depends On:** none
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** true (independent of Phase A)

Create `src/seeknal/connections/postgresql.py` (NEW FILE):
- `PostgreSQLConfig` dataclass: host, port, database, user, password, schema, sslmode, connect_timeout
- `parse_postgresql_config(params: dict) -> PostgreSQLConfig`: Parse from YAML params or profile dict
- `build_attach_string(config: PostgreSQLConfig) -> str`: Build libpq connection string for DuckDB ATTACH
- `check_postgresql_connection(config: PostgreSQLConfig) -> bool`: Test connectivity via DuckDB ATTACH + simple query
- Follow `starrocks.py` pattern (env var interpolation, URL parsing)
- **Security**: Never log passwords. Use `***` masking in error messages

Create `tests/connections/test_postgresql.py` with 15+ unit tests.

Update `src/seeknal/connections/__init__.py` to re-export.

**Acceptance Criteria:**
- [ ] `PostgreSQLConfig` dataclass with all connection params
- [ ] `build_attach_string()` produces valid libpq connection strings
- [ ] Passwords masked in all log output and error messages
- [ ] 15+ unit tests for connection factory

#### 8. Extend profile loader for connection profiles
- **Task ID:** B-2
- **Depends On:** B-1
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Extend `src/seeknal/workflow/materialization/profile_loader.py`:
- Add `connections:` top-level section to profile schema
- Add `load_connection_profile(name: str) -> dict`: Load a named connection from `connections:` section
- Env var interpolation with default values: extend regex to support `${VAR:default}` syntax
- Credential clearing after use (existing `CredentialManager` pattern)

**Acceptance Criteria:**
- [ ] `load_connection_profile("my_pg")` reads from `profiles.yml`
- [ ] `${PG_HOST:localhost}` default-value syntax works in env var interpolation
- [ ] Credentials cleared after use

#### 9. Enhance source executor with profile-based connections
- **Task ID:** B-3
- **Depends On:** B-1, B-2
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Enhance `src/seeknal/workflow/executors/source_executor.py` `_load_postgresql()`:
- Add `connection:` param support: resolve from profile, merge with inline overrides
- Refactor to use `PostgreSQLConfig` from the new connection factory

**Acceptance Criteria:**
- [ ] `params: { connection: my_pg }` resolves from profiles.yml
- [ ] Inline params override profile defaults
- [ ] Existing behavior (inline host/port/etc.) still works

---

### Phase B2: Source Pushdown Query

#### 10. Add pushdown query support to source executor
- **Task ID:** B-4
- **Depends On:** B-3
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Add `query:` support in `source_executor.py` `_load_postgresql()`:
- When `params.get("query")` is present, use `postgres_query()` instead of ATTACH:
  ```python
  con.execute(f"""
      CREATE OR REPLACE VIEW {qualified_view} AS
      SELECT * FROM postgres_query('{pg_alias}', $${query}$$)
  """)
  ```
- When `query:` is absent, keep existing ATTACH + SELECT behavior
- Validate mutual exclusion: if both `table:` and `query:` are present, raise validation error

Add pushdown query validation in `validate()`:
- Pushdown queries must start with `SELECT` or `WITH` (case-insensitive, after stripping whitespace)
- Reject DDL/DML keywords: `DROP`, `DELETE`, `TRUNCATE`, `ALTER`, `INSERT`, `UPDATE`, `CREATE`
- Parameter interpolation: support existing `ParameterResolver` for `{{ params.X }}` patterns

Handle empty results:
- If pushdown query returns 0 rows, create an empty view with correct schema
- Log info: "Pushdown query returned 0 rows for source.{name}"

Create `tests/workflow/test_source_postgresql.py` with 10+ tests.

**Acceptance Criteria:**
- [ ] `query:` field executes SQL on PostgreSQL via `postgres_query()`
- [ ] `table:` field continues working (backward compatible)
- [ ] Validation error when both `table:` and `query:` are present
- [ ] DDL/DML queries rejected with clear error message
- [ ] Empty result sets handled gracefully
- [ ] 10+ unit tests

---

### Phase B3: PostgreSQL Materialization Helper

#### 11. Create PostgreSQL materialization config model
- **Task ID:** B-5
- **Depends On:** none
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** true (parallel with B-1 through B-4)

Create `src/seeknal/workflow/materialization/pg_config.py` (NEW FILE):
- `PostgresMaterializationConfig` dataclass with fields: type, connection, table, mode (full|incremental_by_time|upsert_by_key), time_column, lookback, unique_keys, create_table, cascade
- Validation: mode-specific required fields, table format (schema.name)
- `full` mode: no additional required fields
- `incremental_by_time` mode: requires `time_column`
- `upsert_by_key` mode: requires `unique_keys`

**Acceptance Criteria:**
- [ ] Config model validates mode-specific required fields
- [ ] Table format validated as schema.name (2-part)
- [ ] Clear error messages for missing required fields

#### 12. Create PostgreSQL materialization helper
- **Task ID:** B-6
- **Depends On:** B-1, B-5
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Create `src/seeknal/workflow/materialization/postgresql.py` (NEW FILE):
- `PostgresMaterializationHelper` class following `IcebergMaterializationHelper` pattern
- Three write modes:

**`materialize_full()`**: `DROP TABLE IF EXISTS ... RESTRICT; CREATE TABLE ... AS SELECT ...`
- Uses RESTRICT by default (not CASCADE) to protect dependent objects
- `cascade: true` opt-in via config

**`materialize_incremental()`**: Time-range DELETE + INSERT
- Derive time range: `start` = MIN(time_column) minus `lookback` days, `end` = MAX(time_column)
- First run: auto-create table with `CREATE TABLE AS SELECT *`
- Subsequent runs: `BEGIN; DELETE WHERE time_col BETWEEN start AND end; INSERT INTO ... SELECT ...; COMMIT`

**`materialize_upsert()`**: Logical merge via temp table
- First run: auto-create table
- Subsequent runs: `BEGIN; CREATE TEMP __seeknal_upsert_{table}_{uuid}; DELETE ... USING temp WHERE composite_keys_match; INSERT FROM temp; DROP TEMP; COMMIT`
- Composite key support via PostgreSQL `USING` syntax

- `_table_exists()`: Check if target table exists in PostgreSQL
- `_attach_if_needed()`: ATTACH PostgreSQL if not already attached
- Error handling: ROLLBACK on failure, cleanup temp tables in finally block
- Empty results: `full` creates empty table, `incremental`/`upsert` skip (no-op, log info)

Create `tests/workflow/test_postgresql_materialization.py` with 20+ tests.

**Acceptance Criteria:**
- [ ] `materialize_full()` drops and recreates table
- [ ] `materialize_incremental()` deletes time range and inserts fresh data
- [ ] `materialize_upsert()` performs logical merge with composite key support
- [ ] First-run auto-creates table for all modes
- [ ] Transaction rollback on failure, temp table cleanup
- [ ] Empty result handling per mode
- [ ] RESTRICT by default, CASCADE opt-in
- [ ] 20+ unit tests

---

### Phase B4: Multi-Target Dispatch & YAML Support

#### 13. Extend DAG builder for `materializations:` (plural)
- **Task ID:** B-7
- **Depends On:** B-5
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Extend `src/seeknal/workflow/dag.py`:
- In `_parse_yaml_file()`: read `materializations:` (plural) key alongside `materialization:` (singular)
- Validation: if both keys present, raise error with migration message
- Normalize singular to list: `materialization: {...}` → `materializations: [{...}]`
- For backward compat: singular `materialization:` without `type:` field → treated as Iceberg
- Store normalized list in `yaml_data["materializations"]`

In `_parse_python_file()`: handle list of materialization configs from `@materialize` decorators

**Acceptance Criteria:**
- [ ] `materializations:` (plural) parsed correctly in YAML
- [ ] `materialization:` (singular) continues to work (backward compatible)
- [ ] Validation error if both keys present

#### 14. Create multi-target materialization dispatcher
- **Task ID:** B-8
- **Depends On:** B-6, B-7
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Create `src/seeknal/workflow/materialization/dispatcher.py` (NEW FILE):
- `materialize_all_targets(node, source_con, enabled_override=None) -> List[dict]`
- Routes to `IcebergMaterializationHelper` or `PostgresMaterializationHelper` based on `type:` field
- Best-effort: attempt all targets, log failures, don't rollback succeeded targets
- Return list of per-target results for metadata reporting

Update `post_execute()` in both executors:
- `source_executor.py` (~line 1010-1066): Replace `materialize_node_if_enabled()` with `materialize_all_targets()`
- `transform_executor.py` (~line 398-456): Same replacement
- Report multi-target results in `result.metadata["materializations"]`

Create `tests/workflow/test_materialization_dispatcher.py` with 15+ tests.

**Acceptance Criteria:**
- [ ] Dispatcher routes to correct helper by `type:` field
- [ ] Best-effort execution: first target failure doesn't block second
- [ ] Multi-target results reported in executor metadata
- [ ] 15+ unit tests

---

### Phase B5: Python `@materialize` Decorator

#### 15. Add stackable `@materialize` decorator
- **Task ID:** B-9
- **Depends On:** B-7
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** true (parallel with B-8)

Add `@materialize` decorator in `src/seeknal/pipeline/decorators.py`:
- Stackable: multiple decorators produce a list stored in `func._seeknal_materializations`
- Parameters: type, connection, table, mode, time_column, lookback, unique_keys, **kwargs

Update `@source` decorator: Add `query:` and `connection:` as explicit kwargs (passed through `**params`)

Update `src/seeknal/pipeline/discoverer.py`:
- After collecting `_seeknal_node` metadata, check for `_seeknal_materializations` list
- Store in node metadata as `materializations` key

Update DAG builder Python parsing:
- In `_parse_python_file()`: read `materializations` from node metadata
- Merge with existing `materialization=` param (treat as single Iceberg entry in the list)
- Handle conflict: if both `materialization=` and `@materialize` exist, combine them

Create `tests/pipeline/test_materialize_decorator.py` with 10+ tests.

**Acceptance Criteria:**
- [ ] `@materialize` is stackable — multiple decorators produce a list
- [ ] Works with `@source` and `@transform`
- [ ] Existing `materialization=` param on `@transform` continues to work
- [ ] `@materialize` + `materialization=` combined into single list
- [ ] Python pipeline metadata propagated to DAG correctly
- [ ] 10+ unit tests

---

### Phase B6: Integration Tests & Backward Compatibility

#### 16. Write integration and backward compatibility tests
- **Task ID:** B-10
- **Depends On:** B-8, B-9
- **Assigned To:** builder-pg
- **Agent Type:** general-purpose
- **Parallel:** false

Unit test infrastructure:
- Mock DuckDB `postgres` extension ATTACH/execute calls
- Fixture for `PostgresMaterializationHelper` with mock connection
- Fixture for profile loading with temp `profiles.yml`

Integration test scenarios:
- Source simple table scan with profile connection
- Source pushdown query
- Materialization full mode (create → drop+recreate)
- Materialization incremental (first-run create → subsequent delete+insert)
- Materialization upsert (first-run create → subsequent merge)
- Multi-target: PostgreSQL + mock Iceberg in one node
- Empty result handling per mode

Backward compatibility:
- Existing `materialization:` (singular) YAML still works for Iceberg
- Existing `@transform(materialization=...)` still works
- Existing PostgreSQL source without `connection:` profile still works

**Acceptance Criteria:**
- [ ] All existing tests continue passing (no regressions)
- [ ] 70+ new unit tests across all phases
- [ ] No new dependencies added
- [ ] Backward compatibility validated

---

### Phase C: QA Medallion Pipeline E2E (Multi-Source)

> **Depends on**: All Phase B tasks complete (B-10). Workstream A is independent.

#### 17. Setup local PostgreSQL and project scaffold
- **Task ID:** C-1
- **Depends On:** B-10
- **Assigned To:** builder-qa
- **Agent Type:** general-purpose
- **Parallel:** false

Create `~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/`:
- `docker-compose.yml` with PostgreSQL 15 container (port 5433 to avoid conflicts), pre-configured with `seeknal_test` database
- `scripts/seed_postgres.sql` — Create tables and insert sample bronze data:
  - `public.customers` (~100 rows): customer_id, name, email, region, created_at
  - `public.orders` (~500 rows): order_id, customer_id, product_id, amount, order_date, status
  - `public.products` (~30 rows): product_id, name, category, price
- `.env` with PostgreSQL credentials + Lakekeeper/MinIO/Keycloak credentials (copy from ref-common)
- `profiles.yml` with two connection profiles:
  ```yaml
  connections:
    local_pg:
      type: postgresql
      host: localhost
      port: 5433
      database: seeknal_test
      user: seeknal
      password: ${PG_PASSWORD:seeknal_pass}
      schema: public

  materialization:
    enabled: true
    catalog:
      # Lakekeeper config (same as ref-common)
    default_mode: append
  ```
- CSV seed files in `seeknal/data/`:
  - `traffic_events.csv` (~1000 rows): event_id, customer_id, page, action, timestamp
  - `marketing_campaigns.csv` (~50 rows): campaign_id, name, channel, start_date, end_date, budget

**Acceptance Criteria:**
- [ ] `docker compose up -d` starts PostgreSQL container successfully
- [ ] `psql` can connect and seed data runs without errors
- [ ] CSV files contain realistic sample data
- [ ] `profiles.yml` has both `connections:` and `materialization:` sections

#### 18. Create Bronze layer sources (CSV + Iceberg + PostgreSQL)
- **Task ID:** C-2
- **Depends On:** C-1
- **Assigned To:** builder-qa
- **Agent Type:** general-purpose
- **Parallel:** false

Create source nodes covering all three source types:

**CSV Sources (YAML):**
- `seeknal/sources/traffic_events.yml` — `source: csv`, reads `seeknal/data/traffic_events.csv`
- `seeknal/sources/marketing_campaigns.yml` — `source: csv`, reads `seeknal/data/marketing_campaigns.csv`

**Iceberg Sources (YAML):**
- `seeknal/sources/subscriber_daily.yml` — `source: iceberg`, reads `atlas.ref_common_test.subscriber_daily`

**PostgreSQL Sources (YAML + Python with pushdown):**
- `seeknal/sources/customers.yml` — `source: postgresql`, `table: public.customers`, `params: { connection: local_pg }`
- `seeknal/sources/orders.yml` — `source: postgresql`, with pushdown `query: "SELECT * FROM orders WHERE status = 'completed' AND order_date > '2025-01-01'"`, `params: { connection: local_pg }`

**Python PostgreSQL Source with pushdown:**
- In `seeknal/pipelines/enrichment.py`:
  ```python
  @source(name="products", source="postgresql", connection="local_pg",
          query="SELECT product_id, name, category, price FROM products WHERE price > 0")
  def products(ctx):
      return ctx.ref('source.products')
  ```

**Acceptance Criteria:**
- [ ] 2 CSV sources load local files
- [ ] 1 Iceberg source reads from atlas-dev-server
- [ ] 2 YAML PostgreSQL sources (1 table scan, 1 pushdown query) using `connection: local_pg`
- [ ] 1 Python PostgreSQL source with pushdown query
- [ ] Total: 6 Bronze sources across 3 types

#### 19. Create Silver layer transforms (YAML + Python)
- **Task ID:** C-3
- **Depends On:** C-2
- **Assigned To:** builder-qa
- **Agent Type:** general-purpose
- **Parallel:** false

Create Silver transforms that join and clean data across source types:

**YAML Transforms:**
- `seeknal/transforms/clean_orders.yml` — Filter completed orders, join with customers for region. Uses `ref('source.orders')` + `ref('source.customers')`. Materializes to both:
  ```yaml
  materializations:
    - type: postgresql
      connection: local_pg
      table: analytics.silver_orders
      mode: incremental_by_time
      time_column: order_date
    - type: iceberg
      table: atlas.medallion_test.silver_orders
  ```

- `seeknal/transforms/enrich_traffic.yml` — Join traffic_events with customers, add region. Uses `ref('source.traffic_events')` + `ref('source.customers')`. Materializes to PostgreSQL only:
  ```yaml
  materializations:
    - type: postgresql
      connection: local_pg
      table: analytics.silver_traffic
      mode: full
  ```

**Python Transforms:**
- In `seeknal/pipelines/enrichment.py`:
  ```python
  @transform(name="order_products", inputs=["source.orders", "source.products"])
  @materialize(type='postgresql', connection='local_pg',
               table='analytics.silver_order_products', mode='upsert_by_key',
               unique_keys=['order_id'])
  @materialize(type='iceberg', table='atlas.medallion_test.silver_order_products')
  def order_products(ctx):
      orders = ctx.ref('source.orders')
      products = ctx.ref('source.products')
      return ctx.duckdb.sql("""
          SELECT o.*, p.name as product_name, p.category, p.price as unit_price
          FROM orders o JOIN products p ON o.product_id = p.product_id
      """)
  ```

**Acceptance Criteria:**
- [ ] 2 YAML Silver transforms using `ref()` across source types
- [ ] 1 Python Silver transform with `@materialize` decorator (stackable, multi-target)
- [ ] Multi-target materialization: at least 2 transforms write to both PostgreSQL + Iceberg
- [ ] Incremental mode used for time-series data (orders by order_date)
- [ ] Upsert mode used for order_products (unique key: order_id)
- [ ] Full mode used for traffic enrichment

#### 20. Create Gold layer aggregations and exposure
- **Task ID:** C-4
- **Depends On:** C-3
- **Assigned To:** builder-qa
- **Agent Type:** general-purpose
- **Parallel:** false

**YAML Gold Transforms:**
- `seeknal/transforms/revenue_summary.yml` — Aggregate silver_orders by region + month. Materializes to PostgreSQL (`mode: full`) + Iceberg:
  ```yaml
  materializations:
    - type: postgresql
      connection: local_pg
      table: analytics.gold_revenue_summary
      mode: full
    - type: iceberg
      table: atlas.medallion_test.gold_revenue_summary
  ```

**Python Gold Transform:**
- In `seeknal/pipelines/enrichment.py`:
  ```python
  @transform(name="customer_lifetime_value", inputs=["transform.order_products", "source.customers"])
  @materialize(type='postgresql', connection='local_pg',
               table='analytics.gold_customer_ltv', mode='upsert_by_key',
               unique_keys=['customer_id'])
  def customer_ltv(ctx):
      op = ctx.ref('transform.order_products')
      customers = ctx.ref('source.customers')
      return ctx.duckdb.sql("""
          SELECT c.customer_id, c.name, c.region,
                 COUNT(op.order_id) as total_orders,
                 SUM(op.amount) as lifetime_value,
                 MAX(op.order_date) as last_order_date
          FROM order_products op
          JOIN customers c ON op.customer_id = c.customer_id
          GROUP BY c.customer_id, c.name, c.region
      """)
  ```

**Exposure:**
- `seeknal/exposures/business_dashboard.yml` — Depends on revenue_summary + customer_ltv

**Acceptance Criteria:**
- [ ] 1 YAML Gold transform with multi-target materialization
- [ ] 1 Python Gold transform with `@materialize` (upsert by customer_id)
- [ ] 1 exposure referencing Gold transforms
- [ ] Full medallion chain: Bronze (6 sources) → Silver (3 transforms) → Gold (2 transforms) → Exposure (1)

#### 21. Run full E2E pipeline and validate
- **Task ID:** C-5
- **Depends On:** C-4
- **Assigned To:** builder-qa
- **Agent Type:** general-purpose
- **Parallel:** false

Execute the full medallion pipeline:

```bash
cd ~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/

# 1. Start local PostgreSQL
docker compose up -d
psql -h localhost -p 5433 -U seeknal -d seeknal_test -f scripts/seed_postgres.sql

# 2. Create analytics schema in PostgreSQL
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "CREATE SCHEMA IF NOT EXISTS analytics"

# 3. Run pipeline
seeknal run --profile profiles.yml

# 4. Verify PostgreSQL materialization
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "SELECT COUNT(*) FROM analytics.silver_orders"
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "SELECT COUNT(*) FROM analytics.silver_order_products"
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "SELECT COUNT(*) FROM analytics.gold_revenue_summary"
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "SELECT COUNT(*) FROM analytics.gold_customer_ltv"

# 5. Verify inspect works
seeknal inspect --list
seeknal inspect source.customers
seeknal inspect transform.clean_orders
seeknal inspect transform.customer_lifetime_value

# 6. Verify lineage
seeknal lineage --no-open
seeknal lineage transform.revenue_summary --no-open

# 7. Re-run to test incremental/upsert modes
seeknal run --profile profiles.yml
# Verify no duplicate data in upsert tables
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "SELECT COUNT(*), COUNT(DISTINCT customer_id) FROM analytics.gold_customer_ltv"
```

**Acceptance Criteria:**
- [ ] `seeknal run --profile profiles.yml` executes all nodes (6 sources + 3 silver + 2 gold + 1 exposure = 12 nodes)
- [ ] CSV sources load local files correctly
- [ ] Iceberg source loads from atlas-dev-server
- [ ] PostgreSQL sources load via profile connection (1 table scan, 2 pushdown queries)
- [ ] Silver transforms create tables in PostgreSQL `analytics` schema
- [ ] Multi-target writes: at least 3 tables exist in both PostgreSQL and Iceberg
- [ ] `seeknal inspect` works for all node types
- [ ] `seeknal lineage` generates correct DAG with 12 nodes
- [ ] Re-run produces correct results: incremental deletes+inserts time range, upsert updates existing keys
- [ ] No duplicate rows in upsert tables after re-run

#### 22. Create README and cleanup
- **Task ID:** C-6
- **Depends On:** C-5
- **Assigned To:** builder-qa
- **Agent Type:** general-purpose
- **Parallel:** false

Write comprehensive `README.md`:
- Prerequisites (Docker, atlas-dev-server access, local PostgreSQL)
- Setup steps (docker compose, seed data, create schemas)
- Pipeline architecture diagram (medallion layers)
- Full test command sequence with expected output
- Materialization verification queries
- Cleanup commands (`docker compose down`)

**Acceptance Criteria:**
- [ ] README documents full setup and teardown
- [ ] Includes medallion architecture diagram
- [ ] All test commands documented with expected results

---

## Acceptance Criteria

### Functional Requirements — Workstream A (Lineage & Inspect E2E)
- [ ] `seeknal run --profile profiles.yml` executes all 10 nodes without errors
- [ ] All 3 Iceberg sources load data from `atlas.ref_common_test`
- [ ] All 5 transforms produce intermediate parquet files in `target/intermediate/`
- [ ] Iceberg materialization writes tables to `atlas.lineage_test` namespace
- [ ] Both YAML and Python nodes interoperate (cross-references resolve correctly)
- [ ] `seeknal inspect --list` shows all 10 nodes with correct row/column counts
- [ ] `seeknal inspect` on individual nodes shows data previews
- [ ] `seeknal lineage --no-open` generates `target/lineage.html` with 10 nodes
- [ ] HTML shows all 4 node types with correct colors
- [ ] Focused lineage subgraph works for individual nodes
- [ ] Column trace works for YAML transform chains
- [ ] Cross-referenced edges (YAML→Python, Python→YAML) render correctly

### Functional Requirements — Workstream B (PostgreSQL)
- [ ] PostgreSQL source with simple table scan + profile connection
- [ ] PostgreSQL source with pushdown query (`query:` field)
- [ ] PostgreSQL materialization: full mode
- [ ] PostgreSQL materialization: incremental_by_time mode
- [ ] PostgreSQL materialization: upsert_by_key mode (composite keys)
- [ ] Multi-target materialization (PostgreSQL + Iceberg in one node)
- [ ] Connection profiles in `profiles.yml` with env var defaults
- [ ] Python `@materialize` decorator (stackable)
- [ ] Backward compatibility: singular `materialization:` still works

### Functional Requirements — Workstream C (QA Medallion E2E)
- [ ] Local PostgreSQL container starts and seeds data successfully
- [ ] CSV sources (2) load local files
- [ ] Iceberg source (1) loads from atlas-dev-server
- [ ] PostgreSQL sources (3) load via profile connection, including pushdown queries
- [ ] Silver transforms (3) produce cleaned/enriched data
- [ ] Gold transforms (2) produce aggregated metrics
- [ ] Multi-target materialization: tables exist in both PostgreSQL and Iceberg
- [ ] All 3 materialization modes tested: full, incremental_by_time, upsert_by_key
- [ ] Both YAML and Python pipelines with `@materialize` decorator work
- [ ] Re-run produces correct results (no duplicates in upsert, correct incremental range)
- [ ] `seeknal inspect` and `seeknal lineage` work for all 12 nodes

### Non-Functional Requirements
- [ ] Passwords never appear in logs or error messages
- [ ] Pushdown queries validated (SELECT/WITH only, no DDL)
- [ ] Transaction safety for all materialization modes
- [ ] First-run auto-creates tables for incremental/upsert modes

### Quality Gates
- [ ] 70+ new unit tests across PostgreSQL phases
- [ ] All existing tests continue passing (no regressions)
- [ ] No new dependencies added (DuckDB postgres extension only)

## Team Orchestration

As the team lead, you have access to powerful tools for coordinating work across multiple agents. You NEVER write code directly - you orchestrate team members using these tools.

### Task Management Tools

**TaskCreate** - Create tasks in the shared task list:

```typescript
TaskCreate({
  subject: "Create PostgreSQL connection factory",
  description: "Create src/seeknal/connections/postgresql.py with PostgreSQLConfig dataclass...",
  activeForm: "Creating PostgreSQL connection factory"
})
```

**TaskUpdate** - Update task status, assignment, or dependencies:

```typescript
TaskUpdate({
  taskId: "1",
  status: "in_progress",
  owner: "builder-pg"
})
```

### Task Dependencies

```
Phase A (parallel with Phase B):
  A-1: Scaffold test project → no deps
  A-2: YAML nodes → blockedBy: [A-1]
  A-3: Python nodes → blockedBy: [A-1], parallel with A-2
  A-4: Feature group + exposure → blockedBy: [A-2]
  A-5: README → blockedBy: [A-2, A-3, A-4]
  A-6: Run & validate → blockedBy: [A-5]

Phase B (parallel with Phase A):
  B-1: Connection factory → no deps
  B-2: Profile loader → blockedBy: [B-1]
  B-3: Source executor profile → blockedBy: [B-1, B-2]
  B-4: Pushdown query → blockedBy: [B-3]
  B-5: PG config model → no deps (parallel with B-1)
  B-6: PG materialization helper → blockedBy: [B-1, B-5]
  B-7: DAG builder materializations → blockedBy: [B-5]
  B-8: Multi-target dispatcher → blockedBy: [B-6, B-7]
  B-9: @materialize decorator → blockedBy: [B-7], parallel with B-8
  B-10: Integration tests → blockedBy: [B-8, B-9]

Phase C (after Phase B):
  C-1: Setup local PostgreSQL + scaffold → blockedBy: [B-10]
  C-2: Bronze sources (CSV + Iceberg + PG) → blockedBy: [C-1]
  C-3: Silver transforms (YAML + Python) → blockedBy: [C-2]
  C-4: Gold aggregations + exposure → blockedBy: [C-3]
  C-5: Run full E2E + validate → blockedBy: [C-4]
  C-6: README + cleanup → blockedBy: [C-5]
```

### Team Members

#### builder-test
- **Name:** builder-test
- **Role:** Test project builder (Workstream A)
- **Agent Type:** general-purpose
- **Resume:** true
- **Responsibilities:** Create the lineage & inspect E2E test project, write YAML/Python pipeline nodes, validate E2E flows

#### builder-pg
- **Name:** builder-pg
- **Role:** PostgreSQL feature builder (Workstream B)
- **Agent Type:** general-purpose
- **Resume:** true
- **Responsibilities:** Implement PostgreSQL connection factory, source pushdown, materialization helper, multi-target dispatcher, `@materialize` decorator, all unit tests

#### builder-qa
- **Name:** builder-qa
- **Role:** QA engineer — medallion E2E test project (Workstream C)
- **Agent Type:** general-purpose
- **Resume:** true
- **Responsibilities:** Create medallion architecture test project with CSV + Iceberg + PostgreSQL sources, validate all materialization modes (full/incremental/upsert), verify multi-target writes, run full E2E pipeline

## Dependencies & Prerequisites

### External Dependencies

| Dependency | Version | Purpose | Risk |
|------------|---------|---------|------|
| DuckDB postgres extension | Built-in | PostgreSQL reads/writes via binary COPY | Medium — test early with target PG version |
| Atlas-dev-server | Running | Lakekeeper/MinIO/Keycloak for Iceberg | High — pipeline fails if services down |
| `atlas.ref_common_test` tables | Populated | Bronze data for E2E sources | Medium — rerun ref-common project to repopulate |
| Network access to `172.19.0.9` | Required | SSH tunnel or VPN to atlas-dev-server | Required for Workstream A |

### Internal Dependencies

| Dependency | Status | Notes |
|------------|--------|-------|
| Named refs (`ref()`) | Built | `transform_executor.py:_resolve_named_refs()` |
| Python pipeline discovery | Built | `pipeline/discoverer.py` |
| Iceberg materialization | Built | `materialization/yaml_integration.py` |
| `seeknal lineage` command | Built | `src/seeknal/dag/visualize.py` + CLI |
| `seeknal inspect` command | Built | CLI at `main.py:3002` |

## Risk Analysis & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| DuckDB postgres extension limitations | Medium | High | Test early with target PG version. Fallback: psycopg2 for edge cases |
| Type mapping DuckDB ↔ PostgreSQL | Medium | Medium | Test with common types. Document unsupported types |
| Backward compat breakage | Low | High | Extensive tests for singular `materialization:` key |
| Connection limit exhaustion | Low | Medium | Cache ATTACH per profile name |
| Atlas-dev-server downtime | Medium | High | Document manual repopulation steps |
| Column lineage fails for YAML `transform:` key | Low | Medium | Verify `_build_manifest_from_dag()` maps `transform` → `sql` |
| Python transforms show no column lineage | Known | Low | Document as known limitation in README |

## Critical Design Decisions

**D1: Time range for incremental_by_time** — `start` = MIN(time_column) minus `lookback` days; `end` = MAX(time_column). If 0 rows → skip (no-op).

**D2: First-run behavior (incremental/upsert)** — Auto-create table with `CREATE TABLE AS SELECT *`. Log info message.

**D3: materializations: vs materialization: conflict** — If both keys present → validation error with migration message. Singular without `type:` → normalized as `[{"type": "iceberg", ...}]`.

**D4: Pushdown query safety** — Must start with SELECT or WITH. Reject DDL/DML.

**D5: Multi-target failure policy** — Best-effort: attempt all targets, log failures, don't rollback succeeded.

**D6: DROP TABLE default** — RESTRICT by default. `cascade: true` opt-in.

## Validation Commands

```bash
# Run all existing tests (regression check)
pytest

# Run PostgreSQL connection tests
pytest tests/connections/test_postgresql.py

# Run PostgreSQL source tests
pytest tests/workflow/test_source_postgresql.py

# Run PostgreSQL materialization tests
pytest tests/workflow/test_postgresql_materialization.py

# Run dispatcher tests
pytest tests/workflow/test_materialization_dispatcher.py

# Run decorator tests
pytest tests/pipeline/test_materialize_decorator.py

# E2E lineage test (requires atlas-dev-server)
cd ~/project/self/bmad-new/seeknal-v2.1-test-lineage-inspect/
seeknal run --profile profiles.yml
seeknal inspect --list
seeknal lineage --no-open

# E2E medallion pipeline test (requires atlas-dev-server + local PostgreSQL)
cd ~/project/self/bmad-new/seeknal-v2.1-test-medallion-e2e/
docker compose up -d
psql -h localhost -p 5433 -U seeknal -d seeknal_test -f scripts/seed_postgres.sql
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "CREATE SCHEMA IF NOT EXISTS analytics"
seeknal run --profile profiles.yml
seeknal inspect --list
seeknal lineage --no-open
# Verify PostgreSQL tables
psql -h localhost -p 5433 -U seeknal -d seeknal_test -c "SELECT COUNT(*) FROM analytics.gold_customer_ltv"
# Re-run to test incremental/upsert
seeknal run --profile profiles.yml
docker compose down
```

## Notes

- Workstreams A and B are fully independent and can run in parallel
- Workstream C depends on Workstream B being complete (needs PostgreSQL features)
- Workstream A requires access to atlas-dev-server; Workstream B is purely local (unit tests with mocks)
- Workstream C requires both atlas-dev-server AND a local PostgreSQL Docker container
- Python column lineage is a known limitation — Python transforms don't expose SQL in metadata
- The `@materialize` decorator deprecates the `materialization=` parameter on `@transform` but both coexist
- The medallion E2E test (Workstream C) exercises all 3 materialization modes: full, incremental_by_time, upsert_by_key

---

## Checklist Summary

### Phase A: Lineage & Inspect E2E Test Project
- [x] A-1: Create test project scaffold
- [x] A-2: Create YAML source and transform nodes
- [x] A-3: Create Python pipeline nodes
- [x] A-4: Create feature group and exposure nodes
- [x] A-5: Create README with test commands
- [x] A-6: Run and validate E2E flows

### Phase B1: Connection Factory & Profile Loader
- [x] B-1: Create PostgreSQL connection factory
- [x] B-2: Extend profile loader for connection profiles
- [x] B-3: Enhance source executor with profile-based connections

### Phase B2: Source Pushdown Query
- [x] B-4: Add pushdown query support to source executor

### Phase B3: PostgreSQL Materialization Helper
- [x] B-5: Create PostgreSQL materialization config model
- [x] B-6: Create PostgreSQL materialization helper

### Phase B4: Multi-Target Dispatch & YAML Support
- [x] B-7: Extend DAG builder for `materializations:` (plural)
- [x] B-8: Create multi-target materialization dispatcher

### Phase B5: Python `@materialize` Decorator
- [x] B-9: Add stackable `@materialize` decorator

### Phase B6: Integration Tests
- [x] B-10: Write integration and backward compatibility tests

### Phase C: QA Medallion Pipeline E2E (Multi-Source) 🔵
- [x] C-1: Setup local PostgreSQL and project scaffold
- [x] C-2: Create Bronze layer sources (CSV + Iceberg + PostgreSQL)
- [x] C-3: Create Silver layer transforms (YAML + Python)
- [x] C-4: Create Gold layer aggregations and exposure
- [x] C-5: Run full E2E pipeline and validate
- [x] C-6: Create README and cleanup
