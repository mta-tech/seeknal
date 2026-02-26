---
summary: Execute YAML/Python pipeline with incremental runs and parallel execution
read_when: You want to run a data transformation pipeline
related:
  - plan
  - validate
  - repl
---

# seeknal run

Execute the DAG-based pipeline defined by YAML and Python files in the `seeknal/` directory.

## Synopsis

```bash
seeknal run [OPTIONS]
```

## Description

Executes the pipeline DAG built from YAML and Python files in the `seeknal/` directory.
Supports incremental runs with change detection, parallel execution, interval tracking,
and configurable error handling.

After execution, feature groups with the same entity are automatically consolidated
into per-entity views in `target/feature_store/`.

## Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be executed without running |
| `--full`, `-f` | Run all nodes regardless of state (ignore incremental cache) |
| `--nodes`, `-n` | Run specific nodes only (repeatable) |
| `--types`, `-t` | Filter by node types (e.g., `transform,feature_group`) |
| `--exclude-tags` | Skip nodes with these tags |
| `--continue-on-error` | Continue execution after failures |
| `--retry`, `-r` | Number of retries for failed nodes (default: 0) |
| `--show-plan`, `-p` | Show execution plan without running |
| `--parallel` | Execute independent nodes in parallel |
| `--max-workers` | Maximum parallel workers (default: 4) |
| `--materialize/--no-materialize` | Enable/disable materialization (overrides node config) |
| `--env` | Run in isolated virtual environment |
| `--date` | Override date parameter (YYYY-MM-DD) |
| `--run-id` | Custom run ID for parameterization |
| `--start` | Start timestamp for interval execution (ISO format or YYYY-MM-DD) |
| `--end` | End timestamp for interval execution |
| `--backfill` | Execute backfill for all missing intervals in the date range |
| `--restate` | Process restatement intervals marked for reprocessing |
| `--profile` | Path to profiles.yml for connections |

## Examples

### Run changed nodes only (incremental)

```bash
seeknal run
```

### Run all nodes (full refresh)

```bash
seeknal run --full
```

### Dry run

```bash
seeknal run --dry-run
```

### Show execution plan

```bash
seeknal run --show-plan
```

### Run specific nodes

```bash
seeknal run --nodes transform.clean_data --nodes feature_group.user_features
```

### Run only transforms and feature groups

```bash
seeknal run --types transform,feature_group
```

### Continue on error

```bash
seeknal run --continue-on-error
```

### Retry failed nodes

```bash
seeknal run --retry 2
```

### Run in parallel

```bash
seeknal run --parallel
seeknal run --parallel --max-workers 8
```

### Run in a virtual environment

```bash
seeknal run --env dev
seeknal run --env dev --parallel
```

### Interval tracking

```bash
# Run with specific interval
seeknal run --start 2024-01-01 --end 2024-01-31

# Backfill missing intervals
seeknal run --backfill --start 2024-01-01 --end 2024-01-31
```

## Entity Consolidation

After `seeknal run`, feature groups that share the same `entity` are automatically
consolidated into a unified per-entity view:

```
target/
├── intermediate/
│   ├── feature_group_customer_features.parquet
│   └── feature_group_product_preferences.parquet
└── feature_store/
    └── customer/
        ├── features.parquet          ← merged entity view
        └── _entity_catalog.json      ← catalog metadata
```

Use `seeknal entity list` and `seeknal entity show` to inspect consolidated entities.

## See Also

- [seeknal plan](plan.md) - Preview changes and execution plan
- [seeknal validate](validate.md) - Validate configuration
- [seeknal entity](entity.md) - Inspect consolidated entities
- [seeknal consolidate](consolidate.md) - Manual entity consolidation
