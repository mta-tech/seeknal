---
summary: Start interactive SQL REPL or run one-shot queries
read_when: You want to interactively explore and query your data sources
related:
  - query
  - entity
  - consolidate
---

# seeknal repl

Start an interactive SQL REPL (Read-Eval-Print Loop) for querying data across
multiple sources using DuckDB as a unified query engine. Supports one-shot
queries via `--exec/-e` for scripting and CI integration.

## Synopsis

```bash
seeknal repl [OPTIONS]
```

## Description

The `repl` command launches an interactive SQL shell that allows you to:

- Connect to PostgreSQL, MySQL, SQLite, StarRocks databases
- Query local parquet and CSV files
- Explore schemas and tables
- Run ad-hoc SQL queries
- Execute one-shot queries for scripting

This is useful for data exploration, debugging, and prototyping queries
before adding them to your pipeline.

When launched from a Seeknal project directory (containing `seeknal_project.yml`),
the REPL automatically registers your pipeline data at startup:

1. **Intermediate parquets** from `target/intermediate/` are registered as views
2. **Consolidated entity parquets** from `target/feature_store/` as `entity_{name}` views
3. **PostgreSQL connections** from `profiles.yml` are attached read-only
4. **Iceberg catalogs** from `profiles.yml` are attached via REST API

Each phase runs independently — a PostgreSQL timeout won't prevent parquet
files from being available. A startup banner shows registration counts and
last pipeline run date.

## Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--profile` | PATH | None | Path to profiles.yml for connections |
| `--env` | TEXT | None | Load data from a virtual environment instead of production |
| `--exec`, `-e` | TEXT | None | Execute SQL query and exit (non-interactive). Use `-` for stdin |
| `--format`, `-f` | TEXT | `table` | Output format for `--exec`: `table`, `json`, `csv` |
| `--output`, `-o` | PATH | None | Export `--exec` results to file (.csv, .json, .parquet) |
| `--limit` | INT | None | Limit number of result rows for `--exec` |

## Examples

### Start the REPL

```bash
seeknal repl
```

### Query a virtual environment

```bash
seeknal repl --env dev
```

### One-shot queries

```bash
# Execute a query and exit
seeknal repl -e "SELECT * FROM source_transactions LIMIT 5"

# Export results as JSON
seeknal repl -e "SELECT * FROM transform_orders_cleaned" --format json

# Export results to CSV file
seeknal repl -e "SELECT * FROM transform_orders_cleaned" --output results.csv

# Export to Parquet
seeknal repl -e "SELECT * FROM transform_orders_cleaned" --output results.parquet

# Limit rows
seeknal repl -e "SELECT * FROM transform_orders_cleaned" --limit 50

# Read SQL from stdin
echo "SELECT COUNT(*) FROM source_transactions" | seeknal repl -e -
cat query.sql | seeknal repl -e -
```

### REPL commands

```
seeknal> .connect mydb                   (saved source)
seeknal> .connect postgres://user:pass@host/db
seeknal> .connect /path/to/data.parquet
seeknal> SELECT * FROM source_transactions LIMIT 10
seeknal> .tables
seeknal> .quit
```

## Auto-Registration and View Naming

When run inside a project directory, the REPL auto-registers pipeline outputs
as DuckDB views. View names use the **type-prefixed** convention matching the
intermediate parquet filenames:

```
{kind}_{name}
```

Examples:

| Pipeline node | REPL view name |
|---------------|----------------|
| `source.transactions` | `source_transactions` |
| `transform.orders_cleaned` | `transform_orders_cleaned` |
| `feature_group.customer_features` | `feature_group_customer_features` |
| `second_order_aggregation.region_metrics` | `second_order_aggregation_region_metrics` |

This prevents collisions between nodes of different types with the same name
(e.g., `source_products` vs `transform_products`).

Consolidated entity views use the `entity_` prefix:

| Entity | REPL view name |
|--------|----------------|
| `customer` | `entity_customer` |

Entity views contain struct-namespaced columns from each feature group.

## Project-Aware Mode

When run inside a project directory, the REPL prints a startup banner and
auto-registers available data sources:

```
Seeknal REPL v2.2.0  |  project: my_project
  parquet views : 5
  entity views  : 1
  pg connections: 2
  iceberg tables: 8
  last run      : 2026-02-20 14:32:01

seeknal>
```

Use `--profile` to point at a specific profiles file if it is not in the
project root or the default location (`~/.seeknal/profiles.yml`):

```bash
seeknal repl --profile ./profiles-dev.yml
```

## REPL Commands

| Command | Description |
|---------|-------------|
| `.connect <source>` | Connect to a database or file |
| `.tables` | List available tables and views |
| `.sources` | List registered pipeline sources |
| `.schema <table>` | Show table column details |
| `.duckdb` | Switch back to DuckDB query mode |
| `.quit` | Exit the REPL |
| `.help` | Show available commands |

## Managing Sources

Sources can be managed separately:

```bash
# Add a source
seeknal source add mydb --url postgres://user:pass@host/db

# List sources
seeknal source list

# Remove a source
seeknal source remove mydb
```

## Supported Data Sources

- PostgreSQL
- MySQL
- SQLite
- StarRocks
- Parquet files
- CSV files
- JSON files

## See Also

- [seeknal query](query.md) - Query metrics from semantic layer
- [seeknal entity](entity.md) - Manage consolidated entities
- [seeknal consolidate](consolidate.md) - Trigger entity consolidation
