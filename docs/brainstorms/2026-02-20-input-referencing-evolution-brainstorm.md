---
date: 2026-02-20
topic: input-referencing-evolution
---

# Evolving Input References: From `input_0` to Named Refs

## What We're Exploring

The current YAML transform input referencing has three layers:

1. **Legacy**: `input_0`, `input_1` positional (must match `inputs:` list order)
2. **Current**: `ref('source.sales')` named syntax (resolves to `input_0` internally)
3. **Python**: `ctx.ref("source.sales")` returns DataFrame directly (no positional step)

The question: can we eliminate the positional `input_0` intermediate and go directly from `ref()` to named DuckDB views — like dbt does?

## Current Flow (YAML)

```
User writes:                ref('source.sales')
  ↓ _resolve_named_refs()
Becomes:                    input_0
  ↓ _resolve_input_refs()
Registered as:              CREATE VIEW input_0 AS SELECT * FROM read_parquet('...')
```

The `ref()` → `input_0` step is purely a string replacement. Then `input_0` is a DuckDB view.

## How dbt Does It

In dbt, `{{ ref('orders') }}` compiles directly to the materialized table name:

```sql
-- dbt model: order_enriched.sql
SELECT o.*, c.name
FROM {{ ref('orders') }} o          -- becomes: schema.orders
JOIN {{ ref('customers') }} c       -- becomes: schema.customers
  ON o.customer_id = c.customer_id
```

Key differences from Seeknal:
- **No positional refs** — everything is named
- **Implicit dependency tracking** — deps inferred from `ref()` calls in SQL
- **Jinja templating** — `{{ ref() }}` is a Python function call at compile time
- **Resolves to actual table/schema** — not an intermediate view name

## Proposed Approach: Direct Named Views

### Option A: Skip `input_0`, resolve `ref()` directly to views (Recommended)

```
User writes:                ref('source.sales')
  ↓ single resolution step
Registered as:              CREATE VIEW source_sales AS SELECT * FROM read_parquet('...')
SQL becomes:                SELECT ... FROM source_sales s
```

The `ref('source.sales')` gets replaced with a sanitized view name `source_sales` and the DuckDB view is created pointing to the upstream parquet.

**YAML example:**
```yaml
kind: transform
name: sales_enriched
inputs:
  - ref: source.sales
  - ref: source.products
  - ref: source.regions
transform: |
  SELECT
    s.sale_id, p.name AS product_name, r.region_name
  FROM ref('source.sales') s
  JOIN ref('source.products') p ON s.product_id = p.product_id
  JOIN ref('source.regions') r ON s.region = r.region
```

After resolution:
```sql
SELECT
  s.sale_id, p.name AS product_name, r.region_name
FROM source_sales s
JOIN source_products p ON s.product_id = p.product_id
JOIN source_regions r ON s.region = r.region
```

**Pros:**
- Simpler resolution (one step instead of two)
- SQL is more readable after resolution (view names are descriptive)
- Consistent with Python pipeline (`ctx.ref()` already works this way)
- `input_0` backward compat can remain as a secondary alias

**Cons:**
- View names derived from node IDs might conflict with user tables (mitigated by prefixing)
- Slightly more complex view registration logic

### Option B: dbt-style implicit dependency inference

Allow transforms to omit the `inputs:` list entirely and infer dependencies from `ref()` calls in SQL:

```yaml
kind: transform
name: sales_enriched
# No inputs: list needed!
transform: |
  SELECT s.*, p.name
  FROM ref('source.sales') s
  JOIN ref('source.products') p ON s.product_id = p.product_id
```

The DAG builder would parse the SQL for `ref()` calls and auto-generate the dependency edges.

**Pros:**
- Eliminates redundancy between `inputs:` and `ref()` calls
- Closer to dbt's developer experience
- Less error-prone (can't have mismatched inputs/refs)

**Cons:**
- Requires SQL parsing at DAG build time (currently only done at execution time)
- Less explicit — harder to see dependencies without reading SQL
- Common config `{{ }}` must be resolved before `ref()` parsing
- Breaks validation: can't pre-check inputs before execution

### Option C: Named inputs dictionary (SQLMesh-style)

Allow the `inputs:` list to assign explicit aliases:

```yaml
kind: transform
name: sales_enriched
inputs:
  s: ref: source.sales
  p: ref: source.products
  r: ref: source.regions
transform: |
  SELECT s.sale_id, p.name, r.region_name
  FROM s
  JOIN p ON s.product_id = p.product_id
  JOIN r ON s.region = r.region
```

The alias (`s`, `p`, `r`) becomes the DuckDB view name directly.

**Pros:**
- User controls the SQL identifier — no surprises
- Clean SQL (no `ref()` calls in the SQL at all)
- Already partially supported via dict-format `inputs:` in current code

**Cons:**
- Different YAML syntax from current list format
- Aliases must be valid SQL identifiers (no dots, hyphens)
- Requires users to define aliases upfront

## Comparison Matrix

| Aspect | Current (`input_0`) | A: Direct Views | B: Implicit Deps | C: Named Dict |
|--------|-------------------|-----------------|-------------------|---------------|
| Readability | Poor | Good | Good | Best |
| Explicitness | High | High | Low | High |
| Backward compat | N/A | Full | Breaking | Partial |
| Implementation | Done | Small change | Large change | Medium change |
| dbt similarity | Low | Medium | High | Medium |
| Python parity | Low | High | Medium | Medium |

## Key Decision

**Recommend Option A** as the next step: resolve `ref()` directly to named DuckDB views instead of going through `input_0`. This is:
- Minimal change (modify `_resolve_named_refs` and `_resolve_input_refs`)
- Fully backward compatible (`input_0` still works as alias)
- Consistent with Python pipeline behavior
- Foundation for Option B later if wanted

**Option C** is also attractive for teams that want short, clean SQL. It can be added alongside Option A since dict-format inputs are already partially supported.

## Python Pipeline Status

The Python pipeline (`@source`, `@transform` decorators) is **working end-to-end**:
- `ctx.ref("source.name")` returns a DataFrame, DuckDB auto-scans local variable
- No `input_0` concept needed — the variable name IS the reference
- All 11 nodes of the medallion pipeline verified (3 sources + 8 transforms)
- Both YAML and Python produce equivalent DAG structures (verified by parity tests)

The Python pipeline already demonstrates Option A's approach: named references resolve directly to data objects.

## Next Steps

1. Implement Option A: modify `_resolve_named_refs` to generate view names directly
2. Keep `input_0` aliases for backward compat
3. Consider Option C as a future enhancement for users who prefer explicit aliases
4. Document the `ref()` syntax as the primary approach in user-facing docs
