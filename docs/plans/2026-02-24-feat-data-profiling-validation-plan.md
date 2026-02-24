---
title: "feat: Data Profiling & Profile-Based Validation"
type: feat
status: active
date: 2026-02-24
origin: docs/brainstorms/2026-02-24-data-profiling-validation-brainstorm.md
deepened: 2026-02-24
---

# feat: Data Profiling & Profile-Based Validation

## Enhancement Summary

**Deepened on:** 2026-02-24
**Sections enhanced:** 6
**Research agents used:** DuckDB Profiling SQL, Expression Parser, Pattern Recognition, Security Sentinel, Code Simplicity, DuckDB Solution Docs

### Key Improvements

1. **Simplified output schema** — Dropped `column_type` column (4 columns instead of 5). Type info is derivable from metric names and not needed for threshold checks.
2. **Single-pass DuckDB SQL strategy** — Concrete SQL patterns for aggregation using UNION ALL with proper CAST to avoid HUGEINT issues.
3. **Regex-based expression parser** — ~30-line implementation using compiled regex and frozen dataclass, no dynamic code paths.
4. **Security hardening** — Column name validation via `validate_column_name()`, expression parser uses strict regex (no string interpolation), profile name sanitization for path safety.
5. **Collapsed to 2 implementation phases** — Phase 1: ProfileExecutor + draft/apply integration. Phase 2: profile_check rule type + integration tests.
6. **DuckDB-specific gotchas addressed** — HUGEINT casting for COUNT/SUM, `approx_quantile` for percentiles, `epoch_second` for freshness computation.

### New Considerations Discovered

- `execute_preview()` in `executor.py` is a **separate code path** from the executor's dry-run flag — both need handling
- DuckDB's `SUMMARIZE` command exists but lacks column filtering and produces wide-format output — manual UNION ALL is better
- DuckDB `approx_quantile` uses T-Digest algorithm (O(N), acceptable accuracy for profiling thresholds)
- Standalone `duckdb.connect()` per execute (same as RuleExecutor) — no shared connection

## Overview

Add a new `kind: profile` node type that computes statistical profiles from upstream data (row_count, avg, stddev, null metrics, distinct counts, top values, freshness), outputs results as a long-format parquet table, and feeds into a new `profile_check` rule type for soda-style threshold validation.

```
source.products
  |
  +---> profile.products_stats       (computes statistics)
  |         |
  |         +---> rule.products_quality  (checks thresholds)
  |
  +---> transform.clean_products     (normal pipeline)
```

## Problem Statement / Motivation

Data quality checks in seeknal currently operate at the row level (null checks, range checks, uniqueness) via rule nodes. There is no mechanism to compute **aggregate statistics** about a dataset and validate them against thresholds — e.g., "warn if avg price exceeds $500" or "fail if row count drops to zero."

Soda-core solves this by separating metric computation from check evaluation. This plan brings the same pattern to seeknal using native DuckDB, integrated into the existing DAG, draft/apply workflow, and REPL.

## Proposed Solution

### New `kind: profile` Node

A first-class node type that reads any upstream node's intermediate parquet, auto-detects column types via DuckDB `DESCRIBE`, computes type-appropriate metrics, and writes a long-format stats parquet.

**Minimal YAML (auto-detect all columns):**
```yaml
kind: profile
name: products_stats
inputs:
  - ref: source.products
```

**With column filter and params:**
```yaml
kind: profile
name: products_stats
inputs:
  - ref: source.products
profile:
  columns: [price, quantity, category]
  params:
    max_top_values: 10   # default: 5
```

### Enhanced Rule: `type: profile_check`

Extends the existing RuleExecutor to support a new rule type that reads profile output and evaluates soda-style threshold expressions.

**Declarative checks:**
```yaml
kind: rule
name: products_quality
inputs:
  - ref: profile.products_stats
rule:
  type: profile_check
  checks:
    # Table-level
    - metric: row_count
      fail: "= 0"
      warn: "< 100"

    # Column-level
    - column: price
      metric: avg
      warn: "between 10 and 500"

    - column: price
      metric: null_percent
      warn: "> 5"
      fail: "> 20"

    - column: category
      metric: distinct_count
      fail: "< 2"
```

**SQL escape hatch:**
```yaml
kind: rule
name: products_quality_custom
inputs:
  - ref: profile.products_stats
rule:
  type: custom
  sql: |
    SELECT column_name, metric, value
    FROM input_0
    WHERE (metric = 'null_percent' AND CAST(value AS DOUBLE) > 20)
       OR (metric = 'row_count' AND CAST(value AS DOUBLE) = 0)
```

## Technical Approach

### Output Schema: Long-Format Stats Parquet

Every profile produces a parquet at `target/intermediate/profile_<name>.parquet` with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `column_name` | VARCHAR | Source column name, or `_table_` for table-level metrics |
| `metric` | VARCHAR | Canonical metric name (see table below) |
| `value` | VARCHAR | Stringified metric value (enables uniform schema across types) |
| `detail` | VARCHAR | Additional context (JSON for top_values, NULL for simple metrics) |

> **Research Insight:** `column_type` was dropped from the original 5-column schema. Column type is derivable from the metric names themselves (only numeric columns have `avg`/`stddev`, only timestamps have `freshness_hours`). This simplifies the schema and reduces surface area.

### Canonical Metric Names

| Metric | Applies To | Description | Value Format |
|--------|------------|-------------|--------------|
| `row_count` | `_table_` | Total row count | Integer string |
| `null_count` | All columns | Count of NULL values | Integer string |
| `null_percent` | All columns | Percentage of NULLs (0-100) | Float string |
| `distinct_count` | All columns | Count of distinct non-NULL values | Integer string |
| `min` | Numeric, Timestamp | Minimum value | Stringified value |
| `max` | Numeric, Timestamp | Maximum value | Stringified value |
| `avg` | Numeric | Arithmetic mean | Float string |
| `sum` | Numeric | Sum of values | Float string |
| `stddev` | Numeric | Standard deviation | Float string |
| `p25` | Numeric | 25th percentile (approx) | Float string |
| `p50` | Numeric | Median / 50th percentile (approx) | Float string |
| `p75` | Numeric | 75th percentile (approx) | Float string |
| `top_values` | String, Boolean | Most frequent values | NULL (detail has JSON) |
| `freshness_hours` | Timestamp | Hours since max timestamp | Float string |

**`top_values` detail format (JSON):**
```json
[{"value": "Electronics", "count": 400, "percent": 40.0}, {"value": "Apparel", "count": 300, "percent": 30.0}]
```

> **Research Insight:** Use `approx_quantile` (T-Digest algorithm) for p25/p50/p75 — O(N) performance, acceptable accuracy for profiling thresholds. DuckDB's exact `QUANTILE_CONT` is slower and unnecessary for validation use cases.

### DuckDB SQL Strategy

#### Research Insights

**Single-pass aggregation pattern** — build one large UNION ALL query per column type:

```sql
-- Table-level
SELECT '_table_' AS column_name, 'row_count' AS metric,
       CAST(CAST(COUNT(*) AS BIGINT) AS VARCHAR) AS value,
       NULL AS detail
FROM input_data

UNION ALL

-- Numeric column: price
SELECT 'price', 'avg', CAST(AVG(price) AS VARCHAR), NULL FROM input_data
UNION ALL
SELECT 'price', 'stddev', CAST(STDDEV(price) AS VARCHAR), NULL FROM input_data
UNION ALL
SELECT 'price', 'null_count',
       CAST(CAST(SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS VARCHAR),
       NULL FROM input_data
UNION ALL
SELECT 'price', 'null_percent',
       CAST(100.0 * SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS VARCHAR),
       NULL FROM input_data
UNION ALL
SELECT 'price', 'p25',
       CAST(approx_quantile(price, 0.25) AS VARCHAR),
       NULL FROM input_data
UNION ALL
SELECT 'price', 'p50',
       CAST(approx_quantile(price, 0.5) AS VARCHAR),
       NULL FROM input_data
```

**Key DuckDB gotchas (from documented solutions):**

- `COUNT(*)` and `SUM()` return HUGEINT — always `CAST(... AS BIGINT)` before `CAST AS VARCHAR`
- Use `approx_quantile(col, 0.25)` not `PERCENTILE_CONT` for performance
- Freshness: `CAST(epoch_second(CURRENT_TIMESTAMP - MAX(col)) / 3600.0 AS VARCHAR)` — use `epoch_second()` not INTERVAL arithmetic

**Top-N values query (separate per string column):**

```sql
SELECT 'category' AS column_name, 'top_values' AS metric,
       NULL AS value,
       CAST(list(json_object('value', val, 'count', cnt, 'percent', pct))
            AS VARCHAR) AS detail
FROM (
    SELECT category AS val,
           CAST(COUNT(*) AS BIGINT) AS cnt,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM input_data
    WHERE category IS NOT NULL
    GROUP BY category
    ORDER BY cnt DESC
    LIMIT 5
)
```

#### Type Classification

Auto-detect column types from DuckDB `DESCRIBE` output:

```python
NUMERIC_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
                 "FLOAT", "DOUBLE", "DECIMAL", "UTINYINT", "USMALLINT",
                 "UINTEGER", "UBIGINT"}
TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
                   "TIMESTAMPTZ", "TIMESTAMP_S", "TIMESTAMP_MS",
                   "TIMESTAMP_NS", "DATE"}
STRING_TYPES = {"VARCHAR", "TEXT", "CHAR", "BPCHAR", "UUID"}
SKIP_TYPES = {"BLOB", "BYTEA"}  # Skip with warning
# BOOLEAN → treated as string for top_values
```

### Check Expression Grammar

Profile check threshold expressions use a simple comparison syntax:

```
<operator> <value>
```

Supported operators:
- `>`, `>=`, `<`, `<=`, `=`, `!=`
- `between <low> and <high>` (inclusive)

Examples: `"> 5"`, `"= 0"`, `"< 100"`, `"between 10 and 500"`

The expression is evaluated against the metric's `value` field (cast to DOUBLE). No boolean combinators (AND/OR) — each check is independent.

#### Research Insight: Expression Parser Implementation

~30 lines, regex-based, frozen dataclass. No dynamic code paths.

```python
import re
from dataclasses import dataclass

@dataclass(frozen=True)
class ThresholdExpr:
    operator: str  # ">", ">=", "<", "<=", "=", "!=", "between"
    value: float
    high: float | None = None  # Only for "between"

    def evaluate(self, actual: float) -> bool:
        match self.operator:
            case ">" : return actual > self.value
            case ">=": return actual >= self.value
            case "<" : return actual < self.value
            case "<=": return actual <= self.value
            case "=" : return actual == self.value
            case "!=": return actual != self.value
            case "between":
                return self.value <= actual <= self.high
        return False

_BETWEEN_RE = re.compile(
    r"^\s*between\s+([-+]?\d+(?:\.\d+)?)\s+and\s+([-+]?\d+(?:\.\d+)?)\s*$", re.I
)
_COMPARE_RE = re.compile(
    r"^\s*(>=|<=|!=|>|<|=)\s*([-+]?\d+(?:\.\d+)?)\s*$"
)

def parse_threshold(expr: str) -> ThresholdExpr:
    m = _BETWEEN_RE.match(expr)
    if m:
        return ThresholdExpr("between", float(m.group(1)), float(m.group(2)))
    m = _COMPARE_RE.match(expr)
    if m:
        return ThresholdExpr(m.group(1), float(m.group(2)))
    raise ValueError(f"Invalid threshold expression: {expr!r}")
```

### Security Requirements

Based on security review findings:

1. **Column name injection** — Use `validate_column_name()` from `seeknal/validation.py` for all column names from YAML `profile.columns`. DuckDB `DESCRIBE` output column names should also be validated before interpolation into SQL.

2. **Expression parser injection** — The regex-based parser above is inherently safe: it only matches numeric literals and fixed operator tokens. Never use string interpolation or dynamic code for expression evaluation.

3. **Profile name path traversal** — Sanitize profile name before constructing `target/intermediate/profile_<name>.parquet` path. Use the same `_sanitize_run_id()` pattern from `state.py`: strip `../`, `/`, `\\` sequences.

4. **Integer overflow** — `CAST(COUNT(*) AS BIGINT)` prevents HUGEINT propagation to parquet (Iceberg has no HUGEINT type).

5. **top_values JSON injection** — Use `json_object()` DuckDB function (auto-escapes) rather than string concatenation for JSON construction.

6. **Metric value type safety** — All metric values stored as VARCHAR. The expression parser casts to DOUBLE only during evaluation, with try/except around the cast.

### Edge Case Behaviors

| Scenario | Behavior |
|----------|----------|
| Empty table (0 rows) | Produce profile with `row_count=0`. All column metrics set to `value=NULL`. |
| All-NULL column | `null_count` = row_count, `null_percent` = 100. Numeric stats (avg, stddev, etc.) = NULL. `distinct_count` = 0. |
| Column in `columns:` filter doesn't exist | Warning logged, column skipped, profiling continues for existing columns. |
| BLOB/binary column | Skipped with warning. Only basic metrics (null_count, null_percent). |
| Single-row table | stddev = NULL (undefined for n=1). All other metrics computed normally. |
| Boolean column | Treated as string for top_values. Numeric metrics skipped. |

### Architecture

```
                        +-----------------------+
                        | src/seeknal/dag/      |
                        | manifest.py           |
                        | + NodeType.PROFILE    |
                        +-----------+-----------+
                                    |
                        +-----------+-----------+
                        | src/seeknal/workflow/  |
                        | dag.py                |
                        | + KIND_MAP["profile"] |
                        +-----------+-----------+
                                    |
              +---------------------+---------------------+
              |                                           |
  +-----------+-----------+               +---------------+---------------+
  | workflow/executors/   |               | workflow/executors/            |
  | profile_executor.py   |               | rule_executor.py              |
  | (NEW)                 |               | + "profile_check" type        |
  | @register_executor    |               | + _execute_profile_check()    |
  | (NodeType.PROFILE)    |               +-------------------------------+
  +---+-------+-----------+
      |       |
      |   +---+-----------------------+
      |   | DuckDB DESCRIBE + stats   |
      |   | UNION ALL query           |
      |   +---------------------------+
      |
  +---+---------------------------+
  | target/intermediate/          |
  | profile_<name>.parquet        |
  | (long-format stats)           |
  +-------------------------------+
```

### Implementation Phases

#### Phase 1: ProfileExecutor + CLI Integration

Register the new node type, create the ProfileExecutor, and wire into draft/apply/dry-run.

**Tasks:**

- [x] **Add `NodeType.PROFILE`** to enum in `src/seeknal/dag/manifest.py:18-31`
  - Add `PROFILE = "profile"` between MODEL and RULE
- [x] **Add KIND_MAP entry** in `src/seeknal/workflow/dag.py:181-192`
  - Add `"profile": NodeType.PROFILE`
- [x] **Create `profile_executor.py`** at `src/seeknal/workflow/executors/profile_executor.py`
  - `@register_executor(NodeType.PROFILE)` decorator
  - `validate()`: check `inputs` present (at least 1), validate `profile.columns` with `validate_column_name()`, validate `profile.params.max_top_values` is positive int
  - `execute()`: standalone `duckdb.connect()` → load input parquet → DESCRIBE → type classify → build UNION ALL stats query → write parquet
  - Dry-run: return sample stats (first 5 columns, basic metrics only)
  - Edge cases: empty table, all-null columns, non-existent columns in filter
  - Sanitize profile name for output path (strip `../`, `/`, `\\`)
- [x] **Register executor import** in `src/seeknal/workflow/executors/__init__.py:47-68`
  - Add `from seeknal.workflow.executors.profile_executor import ProfileExecutor`
  - Add to `__all__`
- [x] **Create draft template** at `src/seeknal/workflow/templates/profile.yml.j2`
- [x] **Update `draft.py`** at `src/seeknal/workflow/draft.py:20-54`
  - Add `"profile": "profile"` to `NODE_TYPES`
  - Add `"profile": "profile.yml.j2"` to `TEMPLATE_FILES`
- [x] **Add profile schema** to `src/seeknal/workflow/validators.py`
  - Add `"profile"` key to `SCHEMAS` dict with required fields: `kind`, `name`, `inputs`
  - Optional fields: `profile.columns`, `profile.params`
- [x] **Add dry-run preview** in `src/seeknal/workflow/executor.py`
  - Add `elif kind == "profile":` branch in `execute_preview()` (separate code path from executor dry-run flag)
- [x] **Verify `apply` routing** — `get_target_path()` in `src/seeknal/workflow/apply.py` should produce `seeknal/profiles/<name>.yml` via existing `{kind}s/` convention
- [x] **Create unit tests** at `tests/workflow/test_profile_executor.py`
  - Test validate() with valid/invalid configs
  - Test execute() with numeric, string, timestamp, boolean, mixed-type tables
  - Test edge cases: empty table, all-null column, column filter, non-existent column
  - Test dry-run mode
  - Test output parquet schema matches spec (4 columns)
  - Test column name validation rejects malicious inputs
  - Test profile name sanitization

**Success criteria:** `pytest tests/workflow/test_profile_executor.py` passes. Full `draft -> dry-run -> apply -> run` workflow works for profile nodes.

#### Phase 2: Profile Check Rule Type + Integration

Extend RuleExecutor with `profile_check` type and verify end-to-end flow.

**Tasks:**

- [x] **Add `profile_check` to valid types** in `src/seeknal/workflow/executors/rule_executor.py:195`
  - Add `"profile_check"` to the `valid_rule_types` list
- [x] **Implement `_execute_profile_check()`** method in RuleExecutor
  - Read profile parquet from input ref
  - Parse each check: `{metric, column (optional), warn (optional), fail (optional)}`
  - For each check:
    - Query profile parquet: `SELECT value FROM input_0 WHERE column_name = ? AND metric = ?`
    - Parse threshold expression using `parse_threshold()` (regex-based, ~30 lines)
    - Evaluate: cast value to DOUBLE via try/except, apply `ThresholdExpr.evaluate()`
    - Record result: pass / warn / fail
  - Aggregate results:
    - Any `fail` -> `ExecutionStatus.FAILED` (pipeline halts downstream)
    - Any `warn` (no fail) -> `ExecutionStatus.SUCCESS` with warning metadata
    - All pass -> `ExecutionStatus.SUCCESS`
- [x] **Add expression parser** as helper in rule_executor.py (~30 lines)
  - `ThresholdExpr` frozen dataclass + `parse_threshold()` function
  - Regex patterns: `_BETWEEN_RE`, `_COMPARE_RE`
  - Never use string interpolation or dynamic code for evaluation
- [x] **Handle NULL metric values** — if the profile metric is NULL:
  - `fail:` with NULL metric -> FAIL (conservative)
  - `warn:` with NULL metric -> WARN
- [x] **Update rule validation** — validate `checks:` list structure: each check needs `metric` (required), `column` (optional), at least one of `warn` or `fail`
- [x] **Verify REPL access** — profile parquets at `target/intermediate/profile_<name>.parquet` auto-registered by `_auto_register_project()` in `src/seeknal/cli/repl.py`
- [x] **Verify lineage** — profile nodes appear in DAG visualization at `src/seeknal/dag/visualize.py`
- [x] **Create unit tests** at `tests/workflow/test_rule_profile_check.py`
  - Test expression parsing for all operators
  - Test check evaluation: pass, warn, fail scenarios
  - Test multiple checks with mixed results
  - Test NULL metric handling
  - Test table-level checks (no column specified)
  - Test invalid expressions
- [x] **Integration test** — end-to-end: create source + profile + rule, run pipeline, verify profile stats, verify rule pass/fail

**Success criteria:** A rule with `type: profile_check` correctly evaluates threshold expressions against a profile parquet. Full pipeline `source -> profile -> rule` works end-to-end. Profile stats queryable in REPL.

## System-Wide Impact

### Interaction Graph

```
seeknal draft profile -> draft.py -> NODE_TYPES/TEMPLATE_FILES -> profile.yml.j2
seeknal dry-run       -> executor.py -> execute_preview() -> profile branch
seeknal apply         -> apply.py -> get_target_path() -> seeknal/profiles/
seeknal run           -> dag.py -> KIND_MAP -> DAGRunner -> ExecutorRegistry -> ProfileExecutor
                        -> rule_executor.py -> _execute_profile_check()
seeknal repl          -> repl.py -> _auto_register_project() -> profile parquets
```

### Error Propagation

- ProfileExecutor validation errors -> `ExecutorValidationError` -> DAGRunner logs + skips
- ProfileExecutor execution errors -> `ExecutorExecutionError` -> DAGRunner marks FAILED + skips downstream
- Profile check `fail:` -> RuleExecutor returns FAILED status -> DAGRunner skips downstream nodes
- Profile check `warn:` -> RuleExecutor returns SUCCESS with warning metadata -> pipeline continues
- Expression parse errors -> `ValueError` -> wrapped in `ExecutorExecutionError` with clear message

### State Lifecycle Risks

- Profile parquet overwrites on each run (no versioning). Safe — same as all intermediate parquets.
- Profile fingerprint should re-run when upstream data changes. Use standard `upstream_hash` from `NodeFingerprint` — when upstream node re-runs and its fingerprint changes, the profile's upstream_hash changes too, triggering re-computation.

### API Surface Parity

- Draft/dry-run/apply: profile follows same pattern as source/transform/rule — no new CLI commands needed
- Python decorator support (`@profile`): **out of scope** for this plan — YAML only

## Acceptance Criteria

### Functional Requirements

- [ ] `seeknal draft profile <name>` generates a valid profile YAML template
- [ ] `seeknal dry-run draft_profile_<name>.yml` shows sample statistics preview
- [ ] `seeknal apply draft_profile_<name>.yml` moves file to `seeknal/profiles/`
- [ ] `seeknal run` executes profile nodes, writing long-format stats parquet
- [ ] Profile auto-detects column types and computes appropriate metrics
- [ ] Profile respects `columns:` filter and `params.max_top_values`
- [ ] Profile handles edge cases: empty table, all-null columns, non-existent columns
- [ ] `type: profile_check` rule evaluates threshold expressions against profile output
- [ ] Rule check supports operators: `>`, `>=`, `<`, `<=`, `=`, `!=`, `between`
- [ ] `fail:` threshold breach halts downstream pipeline nodes
- [ ] `warn:` threshold breach logs warning, pipeline continues
- [ ] Profile stats queryable in REPL: `SELECT * FROM profile_<name>`

### Security Requirements

- [ ] Column names validated via `validate_column_name()` before SQL interpolation
- [ ] Expression parser uses strict regex — no string interpolation or dynamic code
- [ ] Profile name sanitized for output path construction
- [ ] `CAST(COUNT(*) AS BIGINT)` used to prevent HUGEINT in parquet output

### Testing Requirements

- [ ] Unit tests for ProfileExecutor: validation, execution, edge cases, dry-run
- [ ] Unit tests for profile_check rule type: expression parsing, evaluation, NULL handling
- [ ] Integration test: source -> profile -> rule end-to-end pipeline
- [ ] Security tests: malicious column names, path traversal in profile names

## Dependencies & Risks

| Risk | Mitigation |
|------|------------|
| DuckDB HUGEINT from COUNT(*)/SUM() | Always CAST to BIGINT/DOUBLE (documented in CLAUDE.md) |
| Wide tables (500+ columns) produce large stats parquet | Acceptable for v1; future: sampling or column limit |
| Expression grammar too simple for complex checks | SQL escape hatch covers advanced cases |
| Profile caching incorrectly skips when data changes | Standard upstream_hash fingerprint handles this |
| Column name SQL injection | Validate all column names via `validate_column_name()` |
| Profile name path traversal | Sanitize name: strip `../`, `/`, `\\` sequences |

## Scope Boundaries

**In scope:** ProfileExecutor, long-format parquet output (4 columns), profile_check rule type, draft/apply/dry-run, REPL access, edge case handling, security validation.

**Out of scope:** Historical profile drift, anomaly detection, profile visualization, sampling, Python decorator support, materialization of profiles to external stores.

## Files to Create/Modify

| File | Action | Phase |
|------|--------|-------|
| `src/seeknal/dag/manifest.py` | Modify: add `PROFILE = "profile"` to NodeType | 1 |
| `src/seeknal/workflow/dag.py` | Modify: add `"profile"` to KIND_MAP | 1 |
| `src/seeknal/workflow/executors/profile_executor.py` | **Create**: ProfileExecutor | 1 |
| `src/seeknal/workflow/executors/__init__.py` | Modify: import + register ProfileExecutor | 1 |
| `src/seeknal/workflow/templates/profile.yml.j2` | **Create**: draft template | 1 |
| `src/seeknal/workflow/draft.py` | Modify: add "profile" to NODE_TYPES + TEMPLATE_FILES | 1 |
| `src/seeknal/workflow/validators.py` | Modify: add "profile" schema | 1 |
| `src/seeknal/workflow/executor.py` | Modify: add profile branch in execute_preview() | 1 |
| `tests/workflow/test_profile_executor.py` | **Create**: unit tests | 1 |
| `src/seeknal/workflow/executors/rule_executor.py` | Modify: add `profile_check` type + expression parser | 2 |
| `tests/workflow/test_rule_profile_check.py` | **Create**: rule check tests + integration tests | 2 |

## Sources & References

### Origin

- **Brainstorm document:** [docs/brainstorms/2026-02-24-data-profiling-validation-brainstorm.md](../brainstorms/2026-02-24-data-profiling-validation-brainstorm.md) — Key decisions: separate profile node (not inline), all 4 metric categories, long-format parquet, soda-style checks + SQL escape hatch, halt-on-fail behavior, auto-detect with optional filter, configurable top_values (default 5).

### Internal References

- RuleExecutor pattern: `src/seeknal/workflow/executors/rule_executor.py`
- Executor registry: `src/seeknal/workflow/executors/base.py:524-690`
- DAG builder KIND_MAP: `src/seeknal/workflow/dag.py:181-192`
- Draft system: `src/seeknal/workflow/draft.py:20-54`
- Column validation: `src/seeknal/validation.py` (`validate_column_name()`)
- Path sanitization: `src/seeknal/workflow/state.py` (`_sanitize_run_id()`)
- DuckDB HUGEINT gotcha: `docs/solutions/duckdb-sql/hugeint-to-bigint-cast-for-iceberg.md`
- GROUP BY completeness: `docs/solutions/duckdb-sql/group-by-completeness-for-aggregations.md`
- Timestamp CAST gotcha: `docs/solutions/python-pipeline/duckdb-cast-timestamp-arithmetic.md`

### Research Findings

- DuckDB `SUMMARIZE` exists but produces wide-format output and lacks column filtering — manual UNION ALL preferred
- DuckDB `approx_quantile` uses T-Digest (O(N), acceptable accuracy for profiling)
- `epoch_second()` is the correct function for timestamp-to-seconds conversion in DuckDB
- `json_object()` DuckDB function auto-escapes values, safe for top_values JSON construction
