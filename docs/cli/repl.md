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
seeknal repl
```

## Description

The `repl` command launches an interactive SQL shell that allows you to:

- Connect to PostgreSQL, MySQL, SQLite databases
- Query local parquet and CSV files
- Explore schemas and tables
- Run ad-hoc SQL queries

This is useful for data exploration, debugging, and prototyping queries
before adding them to your pipeline.

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

## REPL Commands

| Command | Description |
|---------|-------------|
| `.connect <source>` | Connect to a database or file |
| `.tables` | List available tables |
| `.schema <table>` | Show table schema |
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
- Parquet files
- CSV files
- JSON files

## See Also

- [seeknal query](query.md) - Query metrics from semantic layer
- [seeknal source](source.md) - Manage data sources
