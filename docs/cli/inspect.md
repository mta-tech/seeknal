---
summary: Inspect intermediate output of previously executed nodes
read_when: You want to debug pipeline failures by examining upstream outputs
related:
  - run
  - dry-run
  - audit
---

# seeknal inspect

Inspect the intermediate output of a previously executed node. Reads from the
cached parquet files written by `seeknal run`. Useful for debugging pipeline
failures by examining upstream outputs.

## Synopsis

```bash
seeknal inspect [OPTIONS] [NODE_ID]
```

## Description

The `inspect` command allows you to examine the output of any node that was
executed in a previous pipeline run. This is invaluable for debugging:

- Verify upstream data is correct
- Check schema and data types
- Identify data quality issues
- Understand pipeline flow

## Options

| Option | Description |
|--------|-------------|
| `NODE_ID` | Node ID to inspect (e.g., `source.products`, `transform.sales_enriched`) |
| `--limit`, `-l` | Row limit for preview (default: 10) |
| `--schema`, `-s` | Show schema (columns + types) only |
| `--list` | List all available intermediate outputs |
| `--project-path` | Project directory (default: current directory) |

## Examples

### List all available outputs

```bash
seeknal inspect --list
```

### Inspect a source node

```bash
seeknal inspect source.products
```

### Inspect a transform with more rows

```bash
seeknal inspect transform.sales_enriched --limit 20
```

### Show only the schema

```bash
seeknal inspect transform.sales_enriched --schema
```

### Inspect a feature group

```bash
seeknal inspect feature_group.user_features
```

## Output Example

```
Inspecting: source.products (target/cache/source/products.parquet)

Schema:
  product_id    VARCHAR
  name          VARCHAR
  category      VARCHAR
  price         DOUBLE

Sample (10 rows):
┌────────────┬──────────────────┬──────────┬───────┐
│ product_id │ name             │ category │ price │
├────────────┼──────────────────┼──────────┼───────┤
│ P001       │ Widget A         │ widgets  │ 29.99 │
│ P002       │ Widget B         │ widgets  │ 49.99 │
└────────────┴──────────────────┴──────────┴───────┘

Total rows: 100
```

## See Also

- [seeknal run](run.md) - Execute pipeline
- [seeknal dry-run](dry-run.md) - Validate and preview
- [seeknal audit](audit.md) - Run data quality audits
