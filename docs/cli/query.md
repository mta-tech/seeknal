---
summary: Query metrics from the semantic layer
read_when: You want to query business metrics using metric definitions
related:
  - repl
  - audit
---

# seeknal query

Query metrics from the semantic layer. Compiles metric definitions into SQL,
executes against DuckDB, and returns formatted results.

## Synopsis

```bash
seeknal query --metrics METRICS [OPTIONS]
```

## Description

The `query` command allows you to query business metrics defined in your
semantic layer. It compiles metric definitions (from `seeknal/metrics/`) into
SQL and executes them against cached data.

Metrics can include:
- Simple aggregations (sum, count, avg)
- Time-based calculations
- Multi-dimensional analysis

## Options

| Option | Description |
|--------|-------------|
| `--metrics` | Comma-separated metric names (required) |
| `--dimensions` | Comma-separated dimensions (e.g., `region,ordered_at__month`) |
| `--filter` | SQL filter expression |
| `--order-by` | Order by columns (prefix `-` for DESC) |
| `--limit` | Maximum rows to return (default: 100) |
| `--compile` | Show generated SQL without executing |
| `--format` | Output format: `table`, `json`, `csv` (default: `table`) |
| `--project-path` | Project directory (default: current directory) |

## Examples

### Query a single metric

```bash
seeknal query --metrics total_revenue
```

### Query multiple metrics with dimensions

```bash
seeknal query --metrics total_revenue,order_count --dimensions region
```

### Query with time dimension

```bash
seeknal query --metrics total_revenue --dimensions ordered_at__month
```

### Query with filter

```bash
seeknal query --metrics total_revenue --filter "region = 'US'"
```

### Query with ordering

```bash
seeknal query --metrics total_revenue --dimensions region --order-by -total_revenue
```

### Show generated SQL

```bash
seeknal query --metrics total_revenue --compile
```

### Output as JSON

```bash
seeknal query --metrics total_revenue --format json
```

## Defining Metrics

Metrics are defined in YAML files under `seeknal/metrics/`:

```yaml
kind: metric
name: total_revenue
description: Total revenue from all orders
type: sum
measure: sales.price
model: sales_model
```

## See Also

- [seeknal repl](repl.md) - Start interactive SQL REPL
- [seeknal audit](audit.md) - Run data quality audits
