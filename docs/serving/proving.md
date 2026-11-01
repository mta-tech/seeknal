# Seeknal Serving: Proving Correctness, Guardrails, and Dialect Parity

## 1. Overview & Motivation

Seeknal Serving accepts client SQL parsed in a signed `sql_dialect`, enforces security and privacy policies (column allowlists, row filters, column masking, and row caps), rewrites table references for the target engine, and renders target-engine SQL.

To provide defence-in-depth and operational confidence:
- **Round-Trip Guard:** Validates that target-rendered SQL parses cleanly in the target dialect and references only tables and columns checked during policy enforcement, preventing literal-to-identifier escaping vulnerabilities.
- **NULL Ordering Transpilation:** Resolves semantic ordering divergence between StarRocks/MySQL and PostgreSQL/DuckDB, making default null ordering explicit when transpiling across dialects.
- **Grant Token Payload Typing:** Introduces `require_grant_typ` to enforce payload claims `typ: "grant"` while maintaining backward compatibility during Catalog upgrades.
- **Differential Harness:** Runs test suites against multiple DB-API connections (offline DuckDB and live StarRocks/PostgreSQL dev instances) to verify identical query semantics.
- **Load Harness:** Concurrently exercises the HTTP `/v1/query` endpoint with pre-signed grant tokens, reporting latency percentiles (p50, p95, p99) and error counts grouped by routing rule.

---

## 2. Round-Trip Guard (`service.py`)

When client SQL is transpiled into the target engine's dialect, complex string literals (e.g. backslashes, dollar quotes `$$`, PostgreSQL `E'..'` strings, doubled quotes, unicode, or embedded comments) could theoretically break out of literal boundaries if a dialect renderer escapes them incorrectly.

To eliminate this vulnerability before any prepared query is returned:
1. **Re-parsing:** Immediately after producing `rendered_sql` with `statement.sql(dialect=target_dialect)`, the SQL string is re-parsed with `sqlglot.parse(rendered_sql, read=target_dialect)`.
2. **Single-Statement Integrity:** Re-parsing must yield exactly one valid statement AST; multiple statements or parse failures raise `EnforcementError("rendered SQL does not round-trip to the checked statement")`.
3. **Table & Column Subset Check:** All referenced physical table names and column names in the re-parsed AST (case-insensitive) must be a subset of the tables and columns present in the enforced tree. If an unexpected column or table appears, execution is refused.
4. **Literal-to-Identifier Guard:** String literals in the enforced AST are collected and compared to the re-parsed AST. If any string literal value is lost and an unfamiliar identifier appears in the re-parsed AST, execution is refused.
5. **Parameter Markers:** Unique parameter markers (`__SEEKNAL_PARAM_\d+__`) are ignored during the round-trip comparison.

```python
# Invoked internally by ServingPipeline.prepare()
_check_round_trip(enforced.statement, rendered_sql, target_dialect)
```

---

## 3. NULL Ordering Semantics

Database engines differ in their default sorting behavior for `NULL` values when `NULLS FIRST` or `NULLS LAST` is not explicitly declared:

| Engine / Dialect | ASC Default | DESC Default | Sqlglot Model |
| :--- | :--- | :--- | :--- |
| **StarRocks / MySQL** | `NULLS FIRST` | `NULLS LAST` | `nulls_are_small` |
| **PostgreSQL** | `NULLS LAST` | `NULLS FIRST` | `nulls_are_large` |
| **DuckDB** | `NULLS LAST` | `NULLS LAST` | `nulls_are_last` |

### Explicit Transpilation Rules

When the query source `sql_dialect` is `"starrocks"` or `"mysql"`, and the target engine dialect is `"postgres"` or `"duckdb"`:
- Every `ORDER BY` key lacking an explicit `NULLS FIRST` or `NULLS LAST` clause has its source dialect default made explicit:
  - `ASC` (or unspecified direction) $\to$ `NULLS FIRST`
  - `DESC` $\to$ `NULLS LAST`
- Keys that already specify an explicit `NULLS FIRST` or `NULLS LAST` ordering are preserved unchanged.
- For all other dialect pairs (e.g. `postgres` $\to$ `starrocks`, `postgres` $\to$ `duckdb`, `duckdb` $\to$ `postgres`, or `starrocks` $\to$ `starrocks`), implicit ordering is preserved without adding synthetic clauses or `CASE WHEN` expressions.

```sql
-- Client SQL in StarRocks dialect:
SELECT val FROM items ORDER BY val ASC, score DESC

-- Rendered for Postgres or DuckDB:
SELECT val FROM items ORDER BY val ASC NULLS FIRST, score DESC NULLS LAST
```

---

## 4. Grant Payload Typing (`require_grant_typ`) & Safe Rollout

The Atlas Data Catalog issues signed ES256 compact JWS tokens. To tighten token verification while ensuring zero downtime during service upgrades, token verification supports an optional `require_grant_typ` check:

- `verify_grant_token(..., require_grant_typ: bool = False)`
- `ServingPipeline(..., require_grant_typ=False)` passes this parameter through.

### Validation Rules
1. **Payload `typ: "grant"`:** Always accepted regardless of the `require_grant_typ` setting.
2. **Payload `typ: "cancel"`:** Always rejected for query execution (`GrantTokenError("Cancel token cannot be used to run a query")`).
3. **When `require_grant_typ=True`:** The payload claim `typ` is strictly required and must equal `"grant"`. Tokens missing `typ` or containing other types are rejected.
4. **Header `typ` Rules:** Unchanged. Grants require header `typ: "JWT"`; cancel tokens accept `JWT` or `cancel`.

### Rollout Strategy
1. **Phase 1 (Backward-compatible default):** Deploy Seeknal Serving with `require_grant_typ=False` (the default). Serving accepts both legacy tokens without a `typ` payload claim and new tokens containing `typ: "grant"`.
2. **Phase 2 (Catalog Update):** Update Atlas Data Catalog to include `"typ": "grant"` in all issued query grant JWT payloads.
3. **Phase 3 (Strict Enforcement):** Enable `require_grant_typ=True` in Seeknal Serving pipeline configuration. Any token without `typ: "grant"` is refused.

---

## 5. Differential Harness (`seeknal.serving.proving.differential`)

The differential execution harness verifies query semantic parity by running the same test cases across multiple DB-API database connections and comparing the normalised output.

### Result Normalisation (`default_normalise`)
To ensure reliable comparison across differing driver types:
- `Decimal` $\to$ string with fixed scale (`f"{val:.2f}"`)
- `datetime.datetime` $\to$ ISO-8601 string converted to naive UTC
- `datetime.date` / `datetime.time` $\to$ ISO-8601 string
- `float` $\to$ rounded to 9 significant digits (`float(f"{val:.9g}")`)
- `bytes` $\to$ lowercase hexadecimal string
- `None`, integers, strings, booleans $\to$ canonical Python primitives
- Rows are automatically sorted using a `None`-safe sort key unless the case specifies `ordered=True`.

### Synthetic `claims` Table & Default Pitfalls

`default_cases()` executes against a standard synthetic table seeded via `setup_synthetic_claims_table(conn)`:
1. `null_ordering`: ASC / DESC null placement in sort keys.
2. `text_range_and_collation`: String comparison ranges and case collation.
3. `typed_literal_casts`: `NUMERIC(10,2)` scale and `VARCHAR(n)` casting.
4. `date_timestamp_tz`: UTC and timezone offset interpretation.
5. `alias_case`: Preservation and case sensitivity of output column aliases.
6. `table_qualified_order_by`: Ordering by `claims.claim_id` versus projection aliases.
7. `select_star_column_order`: Deterministic projection order for wildcard queries.
8. `int64_boundary_values`: Extremes (`9223372036854775807`, `-9223372036854775807`).

### Running the Harness

Offline testing uses in-memory DuckDB connections:
```python
import duckdb
from seeknal.serving.proving.differential import (
    run_differential,
    default_cases,
    setup_synthetic_claims_table,
)

con1 = duckdb.connect()
con2 = duckdb.connect()
setup_synthetic_claims_table(con1)
setup_synthetic_claims_table(con2)

results = run_differential(default_cases(), {"duck1": lambda: con1, "duck2": lambda: con2})
for r in results:
    print(r.name, "Matched:", r.matched)
```

To run integration tests against real dev servers:
```bash
export SEEKNAL_DIFF_STARROCKS_URL="mysql://root@dev-sr:9030/test"
export SEEKNAL_DIFF_POSTGRES_URL="postgresql://user:pass@dev-pg:5432/test"
uv run pytest tests/serving/test_differential_harness.py
```

---

## 6. Load Harness (`seeknal.serving.proving.load`)

The load testing harness sends concurrent `POST /v1/query` requests using `httpx` and `asyncio`, groups results by routing rule, and measures latency percentiles.

### Python API
```python
from seeknal.serving.proving.load import run_load

report = await run_load(
    url="http://localhost:8000",
    cases=[
        {"name": "lookup", "sql": "SELECT id FROM users WHERE id = 1", "token": token1},
        {"name": "aggregate", "sql": "SELECT count(*) FROM users", "token": token2},
    ],
    concurrency=4,
    duration=10.0,
)

print(report.format_summary())
```

### CLI Invocation
```bash
python -m seeknal.serving.proving.load \
  --url http://dev-serving:8000 \
  --token-file grants.json \
  --cases cases.yaml \
  --concurrency 8 \
  --duration 30
```

Sample output:
```text
Load Test Report (30.00s, 2450 requests, 0 errors):
  Rule: analytics        count=1200   errors=0             p50=12.40ms p95=24.50ms p99=32.10ms
  Rule: point_lookup     count=1250   errors=0             p50=3.20ms  p95=6.80ms  p99=9.50ms
```
