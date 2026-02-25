---
summary: Start interactive SQL REPL for querying data
read_when: You want to interactively explore and query your data sources
related:
  - query
  - source
---

# seeknal repl

Start an interactive SQL REPL (Read-Eval-Print Loop) for querying data across
multiple sources using DuckDB as a unified query engine.

## Synopsis

```bash
seeknal repl [--profile PATH]
```

## Description

The `repl` command launches an interactive SQL shell that allows you to:

- Connect to PostgreSQL, MySQL, SQLite databases
- Query local parquet and CSV files
- Explore schemas and tables
- Run ad-hoc SQL queries

This is useful for data exploration, debugging, and prototyping queries
before adding them to your pipeline.

When launched from a Seeknal project directory (containing `seeknal_project.yml`),
the REPL automatically registers your pipeline data at startup:

1. **Parquet files** from `target/cache/` are registered as queryable views
2. **PostgreSQL connections** from `profiles.yml` are attached read-only
3. **Iceberg catalogs** from `profiles.yml` are attached via REST API

Each phase runs independently — a PostgreSQL timeout won't prevent parquet
files from being available. A startup banner shows registration counts and
last pipeline run date.

## Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--profile` | PATH | None | Path to profiles.yml for auto-registration |

## Examples

### Start the REPL

```bash
seeknal repl
```

### REPL commands

```
seeknal> .connect mydb                   (saved source)
seeknal> .connect postgres://user:pass@host/db
seeknal> .connect /path/to/data.parquet
seeknal> SELECT * FROM db0.users LIMIT 10
seeknal> .tables
seeknal> .quit
```

## Project-Aware Mode

When run inside a project directory, the REPL prints a startup banner and
auto-registers available data sources:

```
Seeknal REPL v2.1.0  |  project: my_project
  parquet views : 5
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
| `.tables` | List available tables |
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
- [seeknal source](source.md) - Manage data sources
