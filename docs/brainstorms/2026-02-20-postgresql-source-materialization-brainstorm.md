# PostgreSQL Source & Materialization Support

**Date**: 2026-02-20
**Status**: Brainstorm
**Author**: Claude Code + User

## What We're Building

Add full PostgreSQL support to Seeknal as both a **data source** (with optional pushdown queries) and a **materialization target** (with full, incremental, and upsert modes). This applies to both YAML pipelines and Python pipelines.

### Core Capabilities

1. **PostgreSQL Source (Enhanced)**: Keep existing simple table scan, add optional `query:` field for pushdown SQL that executes directly on PostgreSQL via `postgres_query()` — only results cross the network.

2. **PostgreSQL Materialization**: Three write modes:
   - **full**: DROP + CREATE TABLE AS (complete replacement)
   - **incremental_by_time**: DELETE time range + INSERT (for time-series data)
   - **upsert_by_key**: DELETE matching keys + INSERT (logical merge)

3. **Multi-Target Materialization**: A single node can materialize to multiple targets (e.g., both PostgreSQL and Iceberg) in one run.

4. **Connection Profiles**: PostgreSQL connections defined in `profiles.yml` (reusable), with optional per-node overrides.

## Why This Approach

### DuckDB-Native Bridge (Zero New Dependencies)

We use DuckDB's `postgres` extension as the sole bridge for all reads and writes. This means:

- **No `psycopg2` dependency** — DuckDB handles everything via its native postgres extension
- **Binary COPY protocol** for writes — 30-50% less data transfer vs text mode
- **Automatic pushdown** — DuckDB pushes filters, projections, and limits to PostgreSQL automatically
- **Parallel scanning** — DuckDB parallelizes reads using PostgreSQL's TID scan
- **Consistent architecture** — Same pattern as existing Iceberg materialization (DuckDB extension-based)

**Trade-off**: No native PostgreSQL `MERGE` syntax. Upserts use DELETE+INSERT in a transaction, which achieves the same result. This is exactly what SQLMesh does for PostgreSQL < 15.

### Inspiration from SQLMesh

Key patterns borrowed from SQLMesh:
- **DELETE+INSERT for incremental**: Delete the time range, then insert fresh data. Optional `lookback` for late-arriving data.
- **Logical merge for upserts**: Temp table + DELETE matching keys + INSERT all from temp.
- **Multi-engine via DuckDB catalogs**: DuckDB as compute engine, PostgreSQL as attached catalog for storage.
- **No custom pushdown implementation**: Rely on DuckDB's native postgres extension pushdown.

## Key Decisions

### 1. Source: Dual Mode (Simple + Pushdown)

**Default**: `table:` field creates a view via `ATTACH` + `SELECT * FROM pg.schema.table` (existing behavior).

**New**: Optional `query:` field runs SQL directly on PostgreSQL via `postgres_query()`:

```yaml
# Simple (existing)
kind: source
name: users
source: postgresql
table: public.users
params:
  connection: my_pg

# Pushdown (new)
kind: source
name: active_users
source: postgresql
query: "SELECT * FROM users WHERE active = true AND created_at > '2025-01-01'"
params:
  connection: my_pg
```

When `query:` is present, Seeknal uses `postgres_query()` instead of `ATTACH`. This ensures the entire SQL runs on PostgreSQL — the user controls exactly what crosses the network.

### 2. Materialization: New `materializations:` List

New plural `materializations:` key supports multiple targets per node:

```yaml
kind: transform
name: user_stats
inputs:
  - ref: source.users
transform: |
  SELECT region, COUNT(*) as user_count
  FROM ref('source.users')
  GROUP BY region
materializations:
  - type: postgresql
    connection: my_pg
    table: analytics.user_stats
    mode: full
  - type: iceberg
    table: atlas.analytics.user_stats
    mode: append
```

**Backward compatibility**: Existing singular `materialization:` (without `type:` field) is treated as Iceberg (current behavior). Both formats are accepted.

### 3. Three Write Modes

#### `full` — Complete Table Replacement
```yaml
materializations:
  - type: postgresql
    connection: my_pg
    table: analytics.user_stats
    mode: full
    create_table: true  # default: true
```
Implementation: `DROP TABLE IF EXISTS ... CASCADE; CREATE TABLE ... AS SELECT ...`

#### `incremental_by_time` — Time-Range DELETE + INSERT
```yaml
materializations:
  - type: postgresql
    connection: my_pg
    table: analytics.daily_events
    mode: incremental_by_time
    time_column: event_date
    lookback: 2  # days, optional, default 0
```
Implementation: `DELETE WHERE time_col BETWEEN start AND end; INSERT INTO ... SELECT ...`

#### `upsert_by_key` — Logical Merge
```yaml
materializations:
  - type: postgresql
    connection: my_pg
    table: analytics.user_profiles
    mode: upsert_by_key
    unique_keys:
      - user_id
```
Implementation: Create temp table → DELETE matching keys → INSERT all from temp → DROP temp.

### 4. Connection Profiles in `profiles.yml`

```yaml
# ~/.seeknal/profiles.yml (or local profiles.yml)
connections:
  my_pg:
    type: postgresql
    host: ${PG_HOST:localhost}
    port: ${PG_PORT:5432}
    database: ${PG_DATABASE:mydb}
    user: ${PG_USER:postgres}
    password: ${PG_PASSWORD}
    schema: public          # default schema
    connect_timeout: 10     # optional
    sslmode: prefer         # optional

  analytics_pg:
    type: postgresql
    host: analytics-db.internal
    port: 5432
    database: analytics
    user: seeknal_writer
    password: ${ANALYTICS_PG_PASSWORD}
```

Nodes reference connections by name:
```yaml
params:
  connection: my_pg  # References profiles.yml
```

Or override inline:
```yaml
params:
  connection: my_pg
  host: override-host.internal  # Overrides profile value
```

### 5. Python Pipeline API: Stackable `@materialize` Decorator

```python
from seeknal.pipeline.decorators import source, transform, materialize

@source(type='postgresql', connection='my_pg', table='public.users')
def users(ctx):
    return ctx.ref('source.users')

# With pushdown query
@source(type='postgresql', connection='my_pg',
        query="SELECT * FROM users WHERE active = true")
def active_users(ctx):
    return ctx.ref('source.active_users')

@transform(inputs=['source.users'])
@materialize(type='postgresql', connection='my_pg',
             table='analytics.user_stats', mode='full')
@materialize(type='iceberg', table='atlas.analytics.user_stats',
             mode='append')
def user_stats(ctx):
    df = ctx.ref('source.users')
    return df.groupby('region').agg(user_count=('user_id', 'count'))
```

The `@materialize` decorator is stackable — each invocation adds a target. During `post_execute()`, all materialization targets are processed sequentially.

### 6. Architecture: Where Code Lives

| Component | Location | Notes |
|-----------|----------|-------|
| PostgreSQL connection manager | `src/seeknal/connections/postgresql.py` | New file. Connection factory + DuckDB ATTACH helper |
| Source pushdown | `src/seeknal/workflow/executors/source_executor.py` | Enhance `_load_postgresql()` with `query:` support |
| Materialization helper | `src/seeknal/workflow/materialization/postgresql.py` | New file. `PostgresMaterializationHelper` class |
| Materialization config | `src/seeknal/workflow/materialization/config.py` | Add `PostgresMaterializationConfig` model |
| Profile loader | `src/seeknal/workflow/materialization/profile_loader.py` | Add `connections:` section parsing |
| Multi-target dispatch | `src/seeknal/workflow/materialization/yaml_integration.py` | New `materialize_all_targets()` function |
| `@materialize` decorator | `src/seeknal/pipeline/decorators.py` | New decorator |
| YAML parsing | `src/seeknal/workflow/dag.py` | Parse `materializations:` (plural) key |

## Open Questions

_None — all key decisions resolved during brainstorming._

## Out of Scope

- **PostgreSQL as compute engine**: We don't send transforms to run on PostgreSQL. DuckDB remains the compute engine.
- **Connection pooling**: DuckDB manages its own postgres connection pool. No custom pooling needed.
- **Schema migrations**: Seeknal doesn't manage PostgreSQL DDL (ALTER TABLE, etc.). Users manage their own schemas.
- **`MERGE` syntax**: PostgreSQL 15+ supports MERGE, but we use DELETE+INSERT for simplicity and PG version compatibility.
- **Streaming/CDC**: No real-time change data capture. This is batch-oriented.

## References

- [DuckDB PostgreSQL Extension](https://duckdb.org/docs/stable/core_extensions/postgres) — binary COPY, pushdown, parallel scan
- [SQLMesh PostgreSQL Integration](https://sqlmesh.readthedocs.io/en/stable/integrations/engines/postgres/) — materialization strategies
- [SQLMesh Multi-Engine Guide](https://sqlmesh.readthedocs.io/en/stable/guides/multi_engine/) — DuckDB + PostgreSQL pattern
- Existing Seeknal Iceberg materialization: `src/seeknal/workflow/materialization/`
