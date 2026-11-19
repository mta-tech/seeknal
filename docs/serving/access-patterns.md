# Seeknal Serving: Table Access Patterns and Guardrails

## Overview

Seeknal Serving classifies tables into three read access patterns and enforces AST-level guardrails before engine execution so concurrent operational queries stay indexed and bounded while analytical scans and aggregations route to columnar engines:

| Pattern | Intended Store | Default `require_filter` | Purpose |
|---|---|---|---|
| `point` | Postgres-wire (`postgres` or `lakebase`) | `true` | Primary/unique key probes ($\le$ `point_lookup_max_keys`) or indexed secondary filter lookups. Requires non-empty `keys`. |
| `filter` | Postgres-wire (`postgres` or `lakebase`) | `true` | Indexed equality, `IN`, range, `BETWEEN`, or prefix `LIKE` range scans with indexed `ORDER BY`. |
| `scan` | DuckDB / StarRocks (or small reference tables on Postgres-wire) | `false` | Full scans and analytical aggregations. |

Untokenized `pii: true` tables without an explicit `access` block implicitly enforce `require_filter: true` (`point` when `keys` are declared, otherwise `filter`). Tables marked `tokenized: true` hold no plaintext PII and are exempt from PII placement and implicit filter rules.

---

## Placement Schema

`access` metadata can be declared per table in `placement` (or derived via catalog introspection and overridden per table):

```yaml
engines:
  pg_report: {kind: postgres, dialect: postgres, mode: read_only}
  lake:      {kind: duckdb, dialect: duckdb}

tables:
  cbn.src_pii.customer:
    pii: true
    serve:
      pg_report: src_pii.customer
    access:
      pattern: point
      keys: [customer_id]
      filterable: [customer_id, msisdn_hash]
      sortable: [customer_id]
      max_rows: 50

  cbn.rpt.rpt_txn_detail:
    row_count: 25000000
    serve:
      pg_report: rpt.rpt_txn_detail
      lake: lake.rpt.rpt_txn_detail
    access:
      pattern: filter
      keys: [txn_id]
      filterable: [customer_id, as_of_date, branch_code]
      range_filterable: [as_of_date]
      prefix_filterable: [merchant_code]
      sortable: [as_of_date, txn_id]
      require_filter: true
      max_rows: 5000

policy:
  point_lookup_max_keys: 100
  require_filter_min_rows: 100000
  max_in_list: 1000
  max_offset: 10000
  guard_mode: enforce  # enforce | warn
```

### Key Inheritance

- If `tables.<name>.keys` is omitted and `access.keys` is provided, `TableEntry.keys` inherits `access.keys`.
- If `access.keys` is omitted and `tables.<name>.keys` is provided, `access.keys` inherits `TableEntry.keys`.
- `pattern: point` requires effective `keys` to be non-empty; otherwise `PlacementError` is raised.

---

## Guardrail Rules (`guardrails.py`)

`check_guardrails()` runs **after routing** and **before `enforce()`** on the client SQL AST. Grant `row_filters` are injected later by `enforce()`, so row-level security predicates never count as the client's selective filter.

`effective_max_rows` applies to **all** engines:

```text
effective_max_rows = min(grant.max_rows, request_max_rows, table.access.max_rows for each referenced table)
```

The rules below apply when the routed target engine is a Postgres-wire source (`postgres` or `lakebase`):

### 1. Sargable Top-Level Conjuncts
Only top-level `AND` conjuncts on **bare** columns against non-null literals or parameters (`$n` / `?`) count as sargable:

- `col = literal|$n` (`op="eq"`, `value_count=1`)
- `col IN (v1, v2, ...)` with no subquery and `len <= policy.max_in_list` (`op="in"`)
- `col > | >= | < | <= literal|$n` (`op="range"`) and `col BETWEEN low AND high` (`op="between"`), valid on `range_filterable` columns
- `col LIKE 'prefix%'` matching `^([^%_\\]|\\[%_\\])+%$` (`op="prefix_like"`), valid on `prefix_filterable` columns
- `OR` of equalities/`IN` lists where **every** branch constrains the **same** bare column with `total_values <= policy.max_in_list` (`op="or_eq"`)

Disqualified forms (never count as sargable):
- Columns wrapped in `CAST` or functions (`CAST(id AS text) = '1'`, `id::text = '1'`, `lower(id) = 'x'`)
- `IS NULL`, `IS NOT NULL`, `= NULL`
- Mixed or tautological `OR` predicates (`id = 1 OR true`, `id = 1 OR other = 2`)
- Leading/infix wildcards (`LIKE '%abc'`, `LIKE 'a_b%'`) or `ILIKE`

### 2. `require_filter` (`filter_required`, HTTP 422)
Every referenced table requiring a filter must be satisfied by at least one of:
- **Point table (`pattern: point`)**: every key in `access.keys` constrained by `eq`/`in`/`or_eq` with product $\le$ `policy.point_lookup_max_keys` (and each $\le$ `policy.max_in_list`), **or** a non-key column constrained via `filterable` / `range_filterable` / `prefix_filterable`.
- **Filter table (`pattern: filter`)**: at least one valid sargable conjunct on `filterable`, `range_filterable`, or `prefix_filterable` (or full key lookup $\le$ `point_lookup_max_keys`).
- **Same-source equi-join propagation**: joined via bare column equality (`t1.c1 = t2.c2` in `JOIN ... ON`, `USING`, or top-level `WHERE`) onto a column in `t2`'s `filterable` or `keys`, where `t1` is itself filtered.

### 3. Derived Tables (`filter_required`, HTTP 422)
- **1 level** of plain-projection derived table (`SELECT cols FROM base_table`, with no aggregation, `GROUP BY`, window, `DISTINCT`, `UNION`, `LIMIT`/`OFFSET`, or nested subquery) is **transparent**—outer predicates and `ORDER BY` resolve through projection aliases to base columns (supporting BI virtual datasets).
- **2+ levels** of subquery nesting in `FROM` over a table requiring a filter is rejected with `filter_required` (422).

### 4. Indexed `ORDER BY` (`sort_not_indexed`, HTTP 422)
When a query has no `GROUP BY` and references any `point` or `filter` table, every `ORDER BY` item must resolve to a bare column in that table's `access.sortable` (or `access.keys`). Sorting by unindexed columns, expressions (`ORDER BY lower(col)`), or ordinal positions (`ORDER BY 1`) returns `sort_not_indexed` (422).

### 5. Bounded `OFFSET` (`offset_too_large`, HTTP 422)
Any `OFFSET` exceeding `policy.max_offset` (default `10000`), negative `OFFSET`, or non-integer literal `OFFSET` returns `offset_too_large` (422). Use keyset pagination for deep paging.

### 6. Rollout Mode (`guard_mode: warn`)
Setting `policy.guard_mode: warn` records the guardrail rejection metric and returns `GuardrailResult(outcome="warn", ...)` (setting `PreparedQuery.guard = "warn"`) without rejecting the query. Default `guard_mode: enforce` raises `GuardrailError` with the typed 422 error code.
