---
summary: Validate and preview a pipeline file without executing
read_when: You want to validate a draft or check for errors before running
related:
  - draft
  - apply
  - run
---

# seeknal dry-run

Validate a YAML or Python pipeline file and preview its execution without
actually running it. This performs comprehensive validation including syntax,
schema, and dependency checks.

## Synopsis

```bash
seeknal dry-run [OPTIONS] FILE_PATH
```

## Description

The `dry-run` command performs comprehensive validation of a pipeline file:

1. **YAML syntax validation** with line numbers for errors
2. **Schema validation** for required fields and types
3. **Dependency validation** to ensure refs point to existing nodes
4. **Preview execution** with sample data (optional)

This is useful for validating draft files before applying them or checking
for errors before a full pipeline run.

## Options

| Option | Description |
|--------|-------------|
| `FILE_PATH` | Path to YAML or Python pipeline file |
| `--limit`, `-l` | Row limit for preview (default: 10) |
| `--timeout`, `-t` | Query timeout in seconds (default: 30) |
| `--schema-only`, `-s` | Validate schema only, skip execution preview |

## Examples

### Validate and preview a draft file

```bash
seeknal dry-run draft_feature_group_user_behavior.yml
```

### Preview with 5 rows

```bash
seeknal dry-run draft_source_orders.yml --limit 5
```

### Schema validation only (no execution)

```bash
seeknal dry-run draft_transform.yml --schema-only
```

### Longer timeout for slow queries

```bash
seeknal dry-run draft_source_large.yml --timeout 60
```

## Validation Output

```
Validating: draft_source_orders.yml

Syntax: OK
Schema: OK
Dependencies: OK

Preview (first 10 rows):
┌──────────┬────────────┬───────────┐
│ order_id │ customer_id│ order_date│
├──────────┼────────────┼───────────┤
│ 1        │ 101        │ 2024-01-15│
│ 2        │ 102        │ 2024-01-16│
└──────────┴────────────┴───────────┘

Validation passed.
```

## See Also

- [seeknal draft](draft.md) - Generate template files
- [seeknal apply](apply.md) - Apply file to production
- [seeknal run](run.md) - Execute pipeline
