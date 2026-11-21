# Seeknal Serving G1: SQL Routing Core

## Purpose

Seeknal Serving operates as a SQL gateway that accepts standard PostgreSQL SQL statements from BI tools, applications, and agents, and intelligently routes them to the most suitable execution engine while rewriting the statement into that engine's native SQL dialect:

- **StarRocks**: High-performance analytical processing over large tables (aggregations, multi-table joins, window functions, and large scans).
- **Cube SQL API**: Slice-and-dice semantic queries over predefined Cube cubes and views (semantic layer, pre-aggregations).
- **PostgreSQL (`postgres`) & Lakebase (`lakebase`)**: Postgres-wire operational stores (`is_postgres_wire`) for indexed point lookups (`point`), selective indexed range/equality queries (`filter`), and—when configured writable (`mode: read_write` on `postgres`, or default `endpoint_type: read_write` on `lakebase`)—transactional writes.
- **DuckDB**: Fast columnar engine (local file or `iceberg_direct`) for analytical scans, aggregations, and lightweight queries.

The G1 milestone delivers the routing core as a pure Python library. It performs structural AST analysis using `sqlglot`, determines engine feasibility and routing policies, and transpiles table references and SQL syntax without establishing network connections or running queries.

---

## Placement Configuration

The placement model maps logical tables and cubes to physical database engines and defines routing policies. Configuration is loaded via YAML (`Placement.from_yaml`) or Python mappings (`Placement.from_dict`).

### Schema Example

```yaml
engines:
  sr:   {kind: starrocks, dialect: starrocks}
  cube: {kind: cube, dialect: postgres}
  lb:   {kind: lakebase, dialect: postgres}
  duck: {kind: duckdb, dialect: duckdb}

tables:
  claims.claim:
    keys: [claim_id]
    row_count: 120000000
    serve:
      sr: lakekeeper.claims.claim
      lb: {name: public.claim, max_lag_seconds: 300}
  ref.province:
    keys: [province_code]
    row_count: 38
    serve:
      sr: lakekeeper.ref.province
      lb: public.province
      duck: province
  claims.patient_identity:
    keys: [patient_id]
    row_count: 500000
    pii: true
    serve:
      lb: public.patient_identity

cubes:
  claims_cube:
    engine: cube
    measures: [total_amount, claim_count]
    dimensions: [province, idrg_code, admit_month]
    pii: false  # Optional boolean; if true, 'cube' must be listed in policy.pii_engines

policy:
  point_lookup_max_keys: 100
  analytics_min_rows: 1000000
  small_scan_order: ["lakebase", "starrocks", "duckdb"]
  analytics_order: ["starrocks", "duckdb"]
  pii_engines: []
  # Optional policy-level PII table list (folded into TableEntry.pii=True / CubeEntry.pii=True):
  # pii_tables: [claims.patient_identity]
```

### Key Principles

- **Logical Name Resolution**: Lookup is case-insensitive. Logical names can be qualified (`claims.claim`). References match entries with the exact same qualification after lower-casing; no default schema is guessed.
- **Engines**: Must have one of the valid kinds: `starrocks`, `cube`, `lakebase`, `postgres`, or `duckdb`. Postgres-wire engines accept optional `mode` (`read_only` or `read_write`, or `endpoint_type` as alias): `postgres` defaults to `read_only` (`is_writable == False`), while `lakebase` defaults to `read_write` (`is_writable == True`). Note that `storage_class` is declared on `connections.<name>`, never inside `placement.engines`.
- **Serves**: A table serve specifies the target engine and physical table name (either as a string or a mapping containing `name`, optional `max_lag_seconds`, and optional `snapshot_id`).
- **Access Metadata**: Tables may declare `access` (`TableAccess`: `pattern`, `keys`, `filterable`, `range_filterable`, `prefix_filterable`, `sortable`, `require_filter`, `max_rows`) and `tokenized: true|false`; see [access-patterns.md](access-patterns.md).
- **Validation**: `PlacementError` is raised with a message identifying the offending entry if unknown engine kinds are supplied, undefined engines are referenced, cubes use non-cube engines, tables lack serves, names are empty, or logical names collide case-insensitively.

---

## Routing Rules and Policies

The router evaluates incoming statements through an ordered pipeline of rules, picking the first matching rule:
1. `hint` (explicit argument or leading `/* seeknal:engine=... */` comment)
2. `write`
3. `semantic`
4. `point_lookup`
5. `filter`
6. `analytics`
7. `small_scan`

### 1. `write`
- **Condition**: Mutation statements (`INSERT`, `UPDATE`, `DELETE`, or `MERGE`).
- **Engine Selection**: A feasible **writable** Postgres-wire engine (`engine.is_writable`, preferring `postgres` with `mode: read_write` before `lakebase`, then placement order). Writes to tables served only on read-only Postgres-wire engines or non-Postgres engines raise `RoutingError` before any connection is opened.
- **Example Statement**:
  ```sql
  INSERT INTO ref.province (province_code) VALUES ('ON')
  ```
- **Selected Engine**: `lb` (Lakebase).
- **Reason**: Transactional mutations require a writable Postgres-wire engine.

### 2. `semantic`
- **Condition**: The statement references a semantic cube defined in `cubes`.
- **Engine Selection**: The cube's hosting engine (Cube SQL API).
- **Example Statement**:
  ```sql
  SELECT total_amount, province FROM claims_cube
  ```
- **Selected Engine**: `cube`.
- **Reason**: Semantic cube queries are directed to Cube SQL API for semantic metric resolution and pre-aggregations.

### 3. `point_lookup`
- **Condition**: A `SELECT` query meeting all criteria:
  - References exactly one table with at least one defined key.
  - Contains no aggregations, `GROUP BY`, window functions, or joins (1-level plain-projection derived tables are transparent).
  - Every key column of the table is constrained by sargable equality (`key = <val>`) or `IN (<val1>, ...)` conjuncts ($\le$ `max_in_list`).
  - The product of allowable key values is $\le$ `point_lookup_max_keys` (default: 100).
- **Engine Selection**: A feasible Postgres-wire engine, preferring `postgres` over `lakebase` on ties, then placement order. If none is feasible, falls through to subsequent rules.
- **Example Statement**:
  ```sql
  SELECT * FROM claims.claim WHERE claim_id = 42
  ```
- **Selected Engine**: `lb` (or `pg` when a `postgres` source also serves the table).
- **Reason**: `"filters claim_id by 1 value on table claims.claim"`.

### 4. `filter`
- **Condition**: A `SELECT` query with no aggregate functions, `GROUP BY`, window functions, or `DISTINCT`, when:
  - A feasible Postgres-wire source serves every referenced table; and
  - At least one top-level sargable conjunct constrains a `filterable`, `range_filterable` (`>`, `>=`, `<`, `<=`, `BETWEEN`), or `prefix_filterable` (`LIKE 'prefix%'`) column of a referenced table (within `max_in_list`).
- **Engine Selection**: A feasible Postgres-wire engine, preferring `postgres` before `lakebase` on ties, then placement order. Aggregates and `DISTINCT` queries on mirrored tables skip `filter` and continue to `analytics`.

### 5. `analytics`
- **Condition**:
  - The query contains any aggregate function, `GROUP BY`, window function, join, or `DISTINCT`; **OR**
  - Any referenced table has an unknown `row_count` or `row_count` $\ge$ `analytics_min_rows` (default: 1,000,000).
- **Engine Selection**: The first feasible engine whose kind appears earliest in `analytics_order` (default: `("starrocks", "duckdb")`).
- **Example Statements**:
  ```sql
  SELECT count(*) FROM ref.province
  ```
  ```sql
  SELECT * FROM claims.claim WHERE status = 'pending'
  ```
- **Selected Engine**: `sr` (StarRocks).
- **Reason**: Heavy aggregations and large table scans are routed to StarRocks or DuckDB for columnar execution.

### 6. `small_scan`
- **Condition**: Statements that do not match prior rules (e.g., non-keyed scans over small reference tables).
- **Engine Selection**: The first feasible engine matching `small_scan_order` (default: `("lakebase", "starrocks", "duckdb")`, where `"lakebase"` matches `"postgres"` first and then `"lakebase"`).
- **Example Statement**:
  ```sql
  SELECT * FROM ref.province
  ```
- **Selected Engine**: `lb` (or `pg`).
- **Reason**: Lightweight scans over small reference tables run efficiently on Postgres-wire or DuckDB without analytical cluster overhead.

### Rule Fallback Preference
If a matched rule's preferred engine order yields no feasible engine, the router evaluates remaining rules' preference orders before raising `RoutingError`.

---

## Query Rewriting

When an engine is selected, the query is rewritten for that specific target:

1. **Table Qualification & Physical Name Replacement**:
   - Each table reference is replaced with that engine's configured physical name (e.g., `claims.claim` $\to$ `lakekeeper.claims.claim` on StarRocks or `public.claim` on Lakebase).
   - If the query defines an alias (`FROM claims.claim AS c`), that alias is preserved (`lakekeeper.claims.claim AS c`).
   - If no alias was specified, the original last name segment is appended as an alias (`lakekeeper.claims.claim AS claim`) ensuring qualified column references such as `claim.amount` continue to resolve correctly.
2. **Cube Names**:
   - Cube names are left as written without physical substitution.
3. **Hint Comment Removal**:
   - Leading hint comments are stripped from the statement.
4. **Dialect Rendering**:
   - The AST is rendered using sqlglot's target dialect (e.g., `dialect="starrocks"` or `dialect="postgres"`).

---

## Engine Hints

Routing decisions can be explicitly overridden using hints.

### 1. Function Argument
```python
router.route("SELECT * FROM ref.province", hint="duck")
```

### 2. Leading SQL Comment
Whitespace-tolerant leading comment at the beginning of the SQL string:
```sql
/* seeknal:engine=duck */
SELECT * FROM ref.province
```

### Hint Validation
Hints are strictly validated. If a hint specifies an unknown engine or an infeasible engine (e.g., an engine that does not replicate the referenced table or violates staleness constraints), a `RoutingError` is raised immediately. Hints are never ignored silently.

---

## Staleness Constraints

Tables replicated asynchronously can specify `max_lag_seconds` in their serve definition:

```yaml
tables:
  claims.claim:
    serve:
      lb: {name: public.claim, max_lag_seconds: 300}
```

When routing with `max_staleness_seconds`:
```python
router.route("SELECT * FROM claims.claim WHERE claim_id = 42", max_staleness_seconds=10)
```
- The router checks each engine's `max_lag_seconds` for all referenced tables.
- Because Lakebase has 300s lag ($> 10\text{s}$), it is rejected with a reason detailing staleness.
- The router falls through to the next feasible engine (StarRocks, where lag is fresh/`None`), choosing `sr` under the `analytics` rule.

---

## PII Placement and Cross-Engine Query Isolation

Tables and semantic cubes containing personally identifiable information can be marked with `pii: true` directly (`tables.<table>.pii: true`, `cubes.<cube>.pii: true`), via static `placement.pii_tables` / `placement.policy.pii_tables` (which fold into `TableEntry.pii = True` and `CubeEntry.pii = True`), or carried through catalog discovery (`lakebase.synced_tables.<table>.pii: true` or `discovery.pii_tables`).

### Placement Invariants (`PlacementError` / Startup Refusal)

1. **Single Dedicated Serve**: Every table with `pii: true` must declare **exactly one** `serve` (`len(serves) == 1`). Declaring two or more serves for a PII table is rejected at placement load and startup validation.
2. **Static `pii_tables` & `policy.pii_tables` Folding**: Listing a table or cube in top-level `pii_tables` or `policy.pii_tables` sets `pii=True` on that `TableEntry` / `CubeEntry` so all PII placement and routing invariants apply. Referencing an unknown table raises `PlacementError`.
3. **DuckDB Prohibition Unless Explicitly Approved**: A `pii: true` table cannot be served on any engine of `kind: duckdb` (including both `local_file` and `iceberg_direct` profiles) unless that specific engine name is listed in `policy.pii_engines` (or `discovery.pii_engines`).
4. **PII Cubes Require Explicit `policy.pii_engines` Approval**: A cube with `pii: true` is refused unless its `engine` is explicitly listed in `policy.pii_engines`.
5. **Validated `policy.pii_engines`**: `policy.pii_engines` defaults to `()` and must be a list/tuple of non-empty engine names declared in `engines`. Referencing an unknown engine raises `PlacementError`.

### Cross-Engine Mixing Guard (`pii_cross_engine`, HTTP 422)

Before any backend engine is called, `check_pii_query_mixing` and `Router` enforce strict engine isolation on every query referencing a `pii: true` table or cube:

- Let `pii_engine` be the sole engine serving the `pii: true` table (or `pii: true` cube).
- Every other table referenced in the statement **must also be served on `pii_engine`** (including when multiple `pii: true` tables appear in the same query).
- Mixing a `pii: true` cube with any table, referencing cubes across different Cube engines when any cube has `pii: true`, or supplying an engine hint (`hint` argument or `/* seeknal:engine=... */` comment) other than `pii_engine` is rejected.
- Violations raise a typed `RoutingError` (`PiiCrossEngineError`) with `.code = "pii_cross_engine"`, mapped to **HTTP 422** before any engine adapter is invoked—even when an engine hint is supplied.

---

## Out of Scope for G1

The following capabilities are intentionally out of scope for the G1 routing core and are deferred to later milestones:

1. **Engine Execution & Network Drivers**: No network connections, connection pools, or direct engine query dispatching.
2. **Authentication & Authorization**: No user session credentials, RBAC/ABAC verification, or database permissions enforcement.
3. **Semantic Query Transformation**: No automatic rewriting of base-table aggregate queries into semantic Cube measures.
4. **Cost-Based Plan Estimation**: Routing decisions are structural and heuristic-driven based on AST inspection and static placement metadata, rather than dynamic runtime cost models or cluster load metrics.
