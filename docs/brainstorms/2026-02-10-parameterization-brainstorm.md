---
date: 2026-02-10
topic: parameterization
---

# YAML and Python Script Parameterization

## What We're Building

A parameterization system for Seeknal YAML pipelines and Python scripts that enables:

1. **Date/Time parameters** - `{{today}}`, `{{yesterday}}`, `{{month_start}}` for scheduled runs
2. **Environment variables** - `{{env:VAR_NAME|default}}` for environment-specific configs
3. **Runtime parameters** - `{{run_id}}`, `{{run_date}}` for pipeline execution context
4. **CLI override support** - `seeknal run --date 2025-02-10` to override defaults

**Syntax:** Jinja-style `{{ }}` in YAML files

**Python integration:** Helper function `get_param()` for explicit parameter access

## Why This Approach

### DAG Builder Integration (Approach B)

We chose to integrate parameter resolution into the DAG builder because:

- **Deep context access** - Can access `context.project_id`, workspace, and full Seeknal state
- **Type-aware resolution** - Parameters resolved during node construction, not just string replacement
- **CLI override support** - Natural integration with existing `seeknal run` command
- **Consistent with existing patterns** - Extends current date parameter handling in flows

### Jinja-Style Syntax

We chose `{{ }}` syntax because:

- **Familiar** - Used by Airflow, dbt, and other data tools
- **Distinct from YAML** - Won't conflict with existing `${VAR}` env var syntax in config
- **Function support** - Enables `{{today()}}`, `{{env('VAR')}}` style calls

### Helper Function for Python

We chose `get_param()` over environment variables because:

- **Explicit** - Clear which parameters are being used
- **Clean** - No pollution of `os.environ` namespace
- **Type-safe** - Helper can handle type conversion (int, bool, etc.)
- **Self-documenting** - Easy to see all parameters used in the script

### Hybrid Resolution

We chose CLI flags + auto-resolution because:

- **Defaults work out-of-the-box** - `{{today}}` just works for daily pipelines
- **Override when needed** - `seeknal run --date 2025-01-01` for backfills
- **Testing friendly** - Can inject specific values for reproducible tests

## Key Decisions

### 1. Built-in Parameter Functions

```yaml
# Date/time functions
{{today}}              # 2025-02-10
{{today(-1)}}          # 2025-02-09 (yesterday)
{{today(-7)}}          # 2025-02-03 (week ago)
{{month_start}}        # 2025-02-01
{{month_start(-1)}}    # 2025-01-01 (last month)
{{year_start}}         # 2025-01-01

# Environment variables
{{env:API_KEY}}                    # Throws if not set
{{env:API_KEY|default_value}}      # Uses default if not set

# Runtime context
{{run_id}}             # UUID for this pipeline run
{{run_date}}           # Execution timestamp
{{project_id}}         # Current project ID
{{workspace_path}}     # Current workspace path
```

### 2. YAML Usage Examples

**Source with dynamic path:**
```yaml
kind: source
name: daily_events
description: Events from today's partition
source: parquet
params:
  path: "data/events/{{today}}/*.parquet"
```

**Transform with date filter:**
```yaml
kind: transform
name: aggregate_daily
description: Daily sales aggregation
transform: |
  SELECT
    date,
    SUM(amount) as total_sales
  FROM source.raw_sales
  WHERE date = '{{today}}'
  GROUP BY 1
params:
  run_date: "{{today}}"
```

**Environment-specific config:**
```yaml
kind: source
name: database_export
source: postgres
params:
  host: "{{env:PG_HOST|localhost}}"
  port: "{{env:PG_PORT|5432}}"
  database: "{{env:PG_DATABASE}}"
```

### 3. Python Script Integration

**YAML file:**
```yaml
kind: transform
name: process_data
params:
  run_date: "{{today}}"
  batch_size: "{{env:BATCH_SIZE|1000}}"
  region: "{{env:REGION|us-east-1}}"
```

**Python script:**
```python
from seeknal import transform, get_param

@transform("process_data")
def my_transform(df):
    # Explicit parameter access
    run_date = get_param("run_date")
    batch_size = get_param("batch_size", type=int)
    region = get_param("region", default="us-east-1")

    print(f"Processing {region} for {run_date}")
    return df.filter(df.region == region)
```

### 4. CLI Override Examples

```bash
# Use default (today's date)
seeknal run

# Override date for backfill
seeknal run --date 2025-01-15

# Override multiple parameters
seeknal run --date 2025-01-15 --run-id custom-run-123

# Dry run to see resolved values
seeknal run --dry-run --date 2025-01-15
```

### 5. Implementation Location

**New module:** `src/seeknal/workflow/parameters/`

```
src/seeknal/workflow/parameters/
├── __init__.py
├── resolver.py          # ParameterResolver class
├── functions.py         # Built-in parameter functions
└── helpers.py           # get_param() helper for Python
```

**Integration point:** `src/seeknal/workflow/dag.py`

```python
# In build_node_from_yaml():
resolver = ParameterResolver(
    context=context,
    cli_overrides=cli_args
)
node.params = resolver.resolve(raw_data.get('params', {}))
```

**CLI enhancement:** `src/seeknal/cli/main.py`

```python
# Add to run command:
@app.command()
def run(
    ...
    date: Optional[str] = typer.Option(None, "--date", help="Override date parameter"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Custom run ID"),
    ...
):
```

## Open Questions

1. **Date format**: Should `{{today}}` return `YYYY-MM-DD` (ISO) or be configurable? Likely ISO for consistency.

2. **Custom functions**: Should users be able to register custom parameter functions? Deferred for future (YAGNI).

3. **Parameter validation**: Should we validate resolved parameter types (e.g., ensure `batch_size` is int)? Yes, in `get_param()` with `type` argument.

4. **Dry-run mode**: Should `--dry-run` show resolved YAML without executing? Useful for debugging.

5. **Parameter scope**: Are parameters global (all nodes) or node-specific? Node-specific in `params:` section is clearer.

## Next Steps

→ `/workflows:plan` for implementation details covering:
- `ParameterResolver` class implementation
- Built-in parameter functions (today, env, run_id, etc.)
- `get_param()` helper function
- CLI argument integration
- Testing strategy for parameter resolution
