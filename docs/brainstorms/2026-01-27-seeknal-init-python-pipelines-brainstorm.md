---
date: 2026-01-27
topic: seeknal-init-python-pipelines
---

# Seeknal Init & Python Pipeline Support

## What We're Building

Three interconnected features to enhance Seeknal's developer experience:

1. **Enhanced `seeknal init`** - dbt-style project scaffolding with profiles support
2. **Python Pipeline API** - Decorator-based API with full access to Seeknal's builder pattern
3. **uv Integration** - Execute Python pipelines using Astral's uv for dependency isolation

## Why This Approach

### Project Initialization (dbt-style)

We chose a dbt-inspired structure because:
- Familiar to data engineers already using dbt
- Clear separation: `seeknal_project.yml` for project config, `profiles.yml` for credentials
- Local `profiles.yml` (gitignored) keeps secrets out of version control while remaining simple
- Pre-created directory structure (`sources/`, `transforms/`, `pipelines/`) guides users

### Python API (Decorator + Builder)

We chose decorators with builder pattern access because:
- Decorators provide clean registration syntax (`@source`, `@transform`, `@feature_group`)
- Full access to Seeknal's transformer/aggregator library inside function bodies
- Context object (`ctx`) provides dependency injection for:
  - `ctx.ref("kind.name")` - Get upstream DataFrame
  - `ctx.duckdb` - DuckDB connection (default engine)
  - `ctx.config` - Profile configuration
- Supports external libraries via PEP 723 inline script metadata
- Mix SQL and programmatic transforms naturally

### Execution via uv (Required)

We chose to require uv because:
- Modern Python tooling, fast dependency resolution
- Inline script dependencies (PEP 723) eliminate need for pyproject.toml
- Each pipeline can have isolated dependencies
- Simple execution: `uv run pipeline.py`

## Key Decisions

### 1. Project Structure

```
my_project/
├── seeknal_project.yml      # Project name, profile reference
├── profiles.yml             # Credentials, engine config (gitignored)
├── .gitignore               # Auto-generated with profiles.yml
├── seeknal/
│   ├── sources/             # YAML source definitions
│   ├── transforms/          # YAML transforms
│   ├── feature_groups/      # YAML feature groups
│   ├── models/              # YAML models
│   ├── pipelines/           # Python pipeline scripts (*.py)
│   └── templates/           # Custom Jinja templates (optional)
└── target/
    └── manifest.json        # Unified manifest (YAML + Python nodes)
```

**Rationale**: `pipelines/` directory dedicated to Python files provides clear separation while unified manifest enables cross-referencing between YAML and Python nodes.

### 2. Python API Syntax

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "requests"]
# ///

from seeknal.pipeline import source, transform, feature_group
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb import transformers as T

@source(name="users", table="data/users.csv", source="csv")
def users(): pass

@transform(name="clean_users")
def clean_users(ctx):
    df = ctx.ref("source.users")
    result = (
        DuckDBTask()
        .add_input(dataframe=df)
        .add_stage(transformer=T.SQL("SELECT * FROM __THIS__ WHERE active"))
        .transform(ctx.duckdb)
    )
    return result

@feature_group(name="user_features", entity="user")
def user_features(ctx):
    df = ctx.ref("transform.clean_users")
    # Use aggregators, transformers, or raw SQL
    return ctx.duckdb.sql("SELECT user_id, COUNT(*) as cnt FROM df GROUP BY 1").df()
```

**Rationale**: Decorators handle node registration; function body has full power of Seeknal's existing task/transformer/aggregator APIs.

### 3. Default Engine: DuckDB

Context provides DuckDB connection by default (`ctx.duckdb`) because:
- Lightweight, no JVM required
- Great for development and small-to-medium datasets
- Faster iteration cycle than Spark
- Spark can be added via `ctx.spark` if configured in profile

### 4. Unified CLI Execution

```bash
seeknal run                              # Runs all (YAML + Python)
seeknal run --nodes transform.clean_users  # Specific node
seeknal run --types python               # Only Python pipelines
```

**Rationale**: Same command, auto-detects node source. Under the hood, Python files execute via `uv run`.

### 5. Enhanced Draft Command

```bash
# YAML (default)
seeknal draft transform my_transform

# Python
seeknal draft transform my_transform --python

# Python with dependencies
seeknal draft transform my_transform --python --deps pandas,requests
```

Python draft template includes:
- PEP 723 inline dependencies block
- Decorator skeleton
- Commented examples showing both SQL and builder patterns
- Helpful docstring explaining `ctx` methods

### 6. Unified Manifest

Python pipeline nodes compile into `target/manifest.json` alongside YAML nodes:
- Enables `ref()` between Python and YAML nodes
- Single source of truth for DAG
- Consistent behavior for `seeknal run` regardless of node source

### 7. Materialization in Python Pipelines

Materialization is configured via typed dataclasses in the decorator parameter:

```python
from seeknal.pipeline import feature_group
from seeknal.pipeline.materialization import (
    Materialization,
    OfflineConfig,
    OnlineConfig,
    IcebergTable,
)

@feature_group(
    name="user_features",
    entity="user",
    materialization=Materialization(
        offline=OfflineConfig(format="parquet"),
        table=IcebergTable(
            namespace="curated",
            name="user_features",
        ),
    ),
)
def user_features(ctx):
    df = ctx.ref("transform.user_activity")
    return ctx.duckdb.sql("""
        SELECT user_id, COUNT(*) as transaction_count
        FROM df GROUP BY 1
    """).df()
```

**Materialization Dataclasses:**

```python
from dataclasses import dataclass
from typing import Optional, Literal

@dataclass
class OfflineConfig:
    format: Literal["parquet", "delta", "iceberg"] = "parquet"
    partition_by: Optional[list[str]] = None

@dataclass
class OnlineConfig:
    enabled: bool = True
    ttl_seconds: Optional[int] = None

@dataclass
class IcebergTable:
    namespace: str
    name: str
    catalog: str = "atlas"  # default from profile

@dataclass
class Materialization:
    offline: Optional[OfflineConfig] = None
    online: Optional[OnlineConfig] = None
    table: Optional[IcebergTable] = None
```

**Rationale**: Typed dataclasses provide IDE autocompletion and validation while mirroring the YAML structure for consistency.

## Open Questions

1. **Spark support in Python pipelines**: Should `ctx.spark` be available when profile enables Spark? Likely yes, but needs implementation planning.

2. **Hot reloading**: Should `seeknal run --watch` detect Python file changes and re-execute? Nice-to-have for development.

3. **Type hints for ctx**: Should we provide a typed Protocol for the context object for IDE autocompletion?

4. **Python pipeline testing**: How should users test individual transform functions? Provide test utilities?

## Next Steps

→ `/workflows:plan` for implementation details covering:
- `seeknal init` command enhancement
- `seeknal.pipeline` module with decorators
- Python file discovery and manifest compilation
- uv execution integration
- Draft template generation
