# Seeknal Serving G1: SQL Routing Core

## Purpose

Seeknal Serving operates as a SQL gateway that accepts standard PostgreSQL SQL statements from BI tools, applications, and agents, and intelligently routes them to the most suitable execution engine while rewriting the statement into that engine's native SQL dialect:

- **StarRocks**: High-performance analytical processing over large tables (aggregations, multi-table joins, window functions, and large scans).
- **Cube SQL API**: Slice-and-dice semantic queries over predefined Cube cubes and views (semantic layer, pre-aggregations).
- **Lakebase** (PostgreSQL): Transactional workloads, including data mutations (writes) and indexed point lookups by primary key.
- **DuckDB**: Fast, in-process engine used as a local fallback for development, testing, and lightweight scans.

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

cubes:
  claims_cube:
    engine: cube
    measures: [total_amount, claim_count]
    dimensions: [province, idrg_code, admit_month]

policy:
  point_lookup_max_keys: 100
  analytics_min_rows: 1000000
  small_scan_order: ["lakebase", "starrocks", "duckdb"]
  analytics_order: ["starrocks", "duckdb"]
```

### Key Principles

- **Logical Name Resolution**: Lookup is case-insensitive. Logical names can be qualified (`claims.claim`). References match entries with the exact same qualification after lower-casing; no default schema is guessed.
- **Engines**: Must have one of the valid kinds: `starrocks`, `cube`, `lakebase`, or `duckdb`.
- **Serves**: A table serve specifies the target engine and physical table name (either as a string or a mapping containing `name` and optional `max_lag_seconds`).
- **Validation**: `PlacementError` is raised with a message identifying the offending entry if unknown engine kinds are supplied, undefined engines are referenced, cubes use non-cube engines, tables lack serves, names are empty, or logical names collide case-insensitively.

---

## Routing Rules and Policies

The router evaluates incoming statements through an ordered pipeline of rules, picking the first matching rule.

### 1. `write`
- **Condition**: Mutation statements (`INSERT`, `UPDATE`, `DELETE`, or `MERGE`).
- **Engine Selection**: The first feasible engine of kind `lakebase`. If none is available, raises `RoutingError`.
- **Example Statement**:
  ```sql
  INSERT INTO ref.province (province_code) VALUES ('ON')
  ```
- **Selected Engine**: `lb` (Lakebase).
- **Reason**: Transactional mutations require transactional consistency provided by Lakebase.

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
  - Contains no aggregations, `GROUP BY`, window functions, joins, or subqueries.
  - Every key column of the table is constrained in the outermost `WHERE` clause by top-level equality (`key = <val>`) or `IN (<val1>, ...)` conjuncts.
  - The product of allowable key values is $\le$ `point_lookup_max_keys` (default: 100).
- **Engine Selection**: The first feasible engine of kind `lakebase`. If Lakebase is infeasible (or rejected due to staleness), falls through to subsequent rules (`analytics` or `small_scan`).
- **Example Statement**:
  ```sql
  SELECT * FROM claims.claim WHERE claim_id = 42
  ```
- **Selected Engine**: `lb` (Lakebase).
- **Reason**: `"filters claim_id by 1 value on table claims.claim"`.

### 4. `analytics`
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
- **Reason**: Heavy aggregations and large table scans are routed to StarRocks for distributed OLAP execution.

### 5. `small_scan`
- **Condition**: Statements that do not match prior rules (e.g., non-keyed scans over small reference tables).
- **Engine Selection**: The first feasible engine matching `small_scan_order` (default: `("lakebase", "starrocks", "duckdb")`).
- **Example Statement**:
  ```sql
  SELECT * FROM ref.province
  ```
- **Selected Engine**: `lb` (Lakebase).
- **Reason**: Lightweight scans over small reference tables run efficiently on Lakebase or DuckDB without analytical cluster overhead.

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

## Out of Scope for G1

The following capabilities are intentionally out of scope for the G1 routing core and are deferred to later milestones:

1. **Engine Execution & Network Drivers**: No network connections, connection pools, or direct engine query dispatching.
2. **Authentication & Authorization**: No user session credentials, RBAC/ABAC verification, or database permissions enforcement.
3. **Semantic Query Transformation**: No automatic rewriting of base-table aggregate queries into semantic Cube measures.
4. **Cost-Based Plan Estimation**: Routing decisions are structural and heuristic-driven based on AST inspection and static placement metadata, rather than dynamic runtime cost models or cluster load metrics.
