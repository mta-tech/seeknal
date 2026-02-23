---
date: 2026-02-19
topic: named-ref-input-syntax
---

# Named `ref()` Input References for Transforms

## What We're Building

Replace the positional `input_0`/`input_1` convention with named `ref('source.sales')` references in transform SQL. This makes transforms self-documenting and reorder-safe.

**Before:**
```yaml
inputs:
  - ref: source.sales
  - ref: source.products
transform: |
  SELECT s.*, p.name
  FROM input_0 s
  JOIN input_1 p ON s.product_id = p.product_id
```

**After:**
```yaml
inputs:
  - ref: source.sales
  - ref: source.products
transform: |
  SELECT s.*, p.name
  FROM ref('source.sales') s
  JOIN ref('source.products') p ON s.product_id = p.product_id
```

## Key Decisions

- **Backward compatible**: `input_0`/`input_1` still works as fallback
- **Full qualified names required**: `ref('source.sales')` not `ref('sales')` — zero ambiguity, grep-able, matches inputs list 1:1
- **Both single and double quotes**: `ref('source.sales')` and `ref("source.sales")`
- **Validation**: Each `ref()` in SQL must match an entry in the `inputs` list
- **Python pipelines unaffected**: `ctx.ref("source.sales")` already uses this pattern

## Implementation Scope

### Files to modify:
1. `src/seeknal/workflow/executors/transform_executor.py` — `_resolve_input_refs()`: add regex to find `ref('...')` and replace with DuckDB view names
2. `src/seeknal/workflow/executor.py` — `execute_transform()`: same regex for dry-run preview
3. `docs/reference/yaml-schema.md` — document new syntax
4. `docs/building-blocks/transforms.md` — update examples

### Resolution strategy:
1. Parse SQL for `ref('kind.name')` patterns
2. For each match, look up the corresponding input in the inputs list
3. Create a DuckDB view with a safe name (e.g., `_ref_source_sales`)
4. Replace `ref('source.sales')` in SQL with the view name
5. Also create `input_N` views for backward compatibility

### Regex pattern:
```python
REF_PATTERN = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
```

## Open Questions

- Should `ref()` work for nodes NOT in the inputs list? (e.g., referencing a sibling transform directly) — Suggest NO for now, keep it strict.
- Error message when `ref('source.X')` doesn't match any input — should show available refs.

## Next Steps

-> Plan and implement
