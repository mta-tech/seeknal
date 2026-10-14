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
| `--date` | Run as if today were this date (YYYY-MM-DD); see [Parameters](#parameters) |
| `--run-id` | Custom run ID for parameterization |
| `--params` | JSON object of custom parameters, e.g. `'{"region": "EU"}'` |
| `--start-date` / `--end-date` | Values for `{{ start_date }}` / `{{ end_date }}` (YYYY-MM-DD) |
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

## Parameters

`{{ }}` placeholders are filled in when the DAG is built:

- **YAML nodes:** `params:` values, `transform:` SQL, and a source's `table:`
  (pushdown queries live in `params.query`, so they are covered too).
- **Python nodes:** decorator keyword arguments, e.g.
  `@transform(run_day="{{ today }}")`, read with `ctx.get_param("run_day")`.
  A source's `table=` is also templated.

| Placeholder | Value |
|---|---|
| `{{ today }}`, `{{ today(-1) }}`, `{{ yesterday }}` | Today / offset in days |
| `{{ month_start }}`, `{{ month_start(-1) }}`, `{{ year_start }}` | First day of the (offset) month / year |
| `{{ date }}` | The `--date` value (only set when `--date` is given) |
| `{{ run_date }}` | The `--date` value, or the run's start timestamp |
| `{{ start_date }}`, `{{ end_date }}` | `--start-date` / `--end-date` |
| `{{ <key> }}` | A key from `--params` or the node's `params:` (Python nodes: declared keys only) |
| `{{ env:VAR\|default }}` | Environment variable |
| `{{ run_id }}` | `--run-id`, or a generated UUID |

`--date` sets "today" for the whole run: every date function counts from it, so
`seeknal run --date 2026-10-12` makes `{{ today(-1) }}` resolve to `2026-10-11`.

A CLI value replaces a declared parameter with the same name. Declare defaults in
`params:` (YAML) or as decorator arguments (Python), and override them per run:

```yaml
kind: transform
name: daily_orders
inputs:
  - ref: source.orders
params:
  run_day: "{{ today }}"
  region: EU
transform: |
  SELECT * FROM ref('source.orders')
  WHERE CAST(order_date AS DATE) = CAST('{{ run_day }}' AS DATE)
    AND region = '{{ region }}'
```

```bash
seeknal run                                              # run_day = today, region = EU
seeknal run --date 2026-10-12 --params '{"region": "US"}'
```

A node re-runs automatically when its resolved values change, so `--full` is not
needed for a new `--date`.

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
