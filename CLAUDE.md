# Seeknal Project Guide for Claude Code

## Project Overview

**Seeknal** is an all-in-one platform for data and AI/ML engineering, providing feature store capabilities and data transformation pipelines. It abstracts away complexity in data transformation and ML feature engineering.

- **Language**: Python 3.11+
- **Main Dependencies**: DuckDB, PySpark, Typer (CLI), SQLModel, Pydantic
- **Database**: SQLite (default) or Turso (production)
- **Architecture**: Multi-engine data processing (DuckDB + Spark)

## Project Structure

```
src/seeknal/
├── cli/                    # Typer-based CLI commands
│   └── main.py            # CLI entry point with all commands
├── featurestore/          # Feature store core
│   ├── feature_group.py   # FeatureGroup, Materialization models
│   ├── featurestore.py    # Feature store operations
│   └── duckdbengine/      # DuckDB-specific implementations
├── tasks/                 # Data transformation tasks
│   ├── base.py           # Base task interface
│   ├── duckdb/           # DuckDB task implementations
│   └── sparkengine/      # Spark task implementations
├── feature_validation/    # Feature validation framework
│   ├── validators.py     # Validation logic
│   └── models.py         # Validation configuration models
├── flow.py               # Data pipeline (Flow) orchestration
├── entity.py             # Entity definitions (join keys)
├── project.py            # Project management
├── workspace.py          # Workspace context management
├── context.py            # Global context and configuration
├── request.py            # Database request layer
├── validation.py         # SQL injection prevention
└── utils/
    └── path_security.py  # Secure path validation

docs/                      # Comprehensive documentation
├── api/                  # API reference docs
├── examples/             # Usage examples
└── getting-started-comprehensive.md

tests/                     # pytest test suite
├── cli/                  # CLI command tests
├── e2e/                  # End-to-end tests
└── featurestore/         # Feature store tests
```

## Key Concepts

### 1. **Project & Workspace**
- Every operation requires a project context
- Use decorators: `@require_workspace`, `@require_project`
- Projects are stored in SQLite/Turso database

### 2. **Flow (Data Pipeline)**
- Represents transformation pipelines
- Can mix Spark and DuckDB tasks
- Defined via: `Flow(name, input, tasks, output)`
- Tasks are executed sequentially

### 3. **Feature Groups**
- Container for related features
- Has entity (join keys), materialization config
- Automatically versioned on schema changes
- Supports offline and online stores

### 4. **Materialization**
- **Offline**: Batch processing for training data
- **Online**: Low-latency serving for inference
- Point-in-time joins prevent data leakage

### 5. **Version Management**
- Feature groups are automatically versioned
- CLI commands: `version list`, `version show`, `version diff`
- Can materialize specific versions (rollback capability)

## Important Patterns & Conventions

### Decorators
```python
@require_workspace  # Ensures workspace context exists
@require_project    # Ensures project is selected
@require_saved      # Ensures object is saved to database
```

### CLI Structure
- Built with Typer
- Main app in `cli/main.py`
- Sub-commands use `app.add_typer()`
- Version management is a separate Typer app group

### Database Operations
- Use `request.py` layer for all DB operations
- Models defined with SQLModel
- Context provides database connection: `context.con`

### Security

#### SQL Injection Prevention
- Always use `validate_sql_value()` and `validate_column_name()` from `validation.py`
- Never concatenate user input into SQL queries

#### Path Security
- Use `path_security.is_insecure_path()` to validate file paths
- Never use `/tmp`, `/var/tmp`, or world-writable directories
- Default secure path: `~/.seeknal/`

#### Path Sanitization
- Always sanitize user input used in file paths to prevent path traversal attacks
- Remove dangerous sequences (`../`, `..\\`, `/`, `\\`) before path construction
- Validate base paths at initialization time

**Correct:**
```python
def _sanitize_run_id(self, run_id: str) -> str:
    sanitized = re.sub(r'\.\.[/\\]', '', run_id)  # Remove ../ or ..\
    sanitized = re.sub(r'[/\\]', '', sanitized)   # Remove remaining slashes
    sanitized = re.sub(r'\.\.', '', sanitized)     # Remove any remaining ..
    return sanitized

def __init__(self, base_path: Path):
    if is_insecure_path(str(base_path)):
        raise ValueError(
            f"Insecure base path detected: '{base_path}'. "
            "Use a secure location such as ~/.seeknal/state"
        )
```

**Incorrect:**
```python
def _get_run_state_path(self, run_id: str) -> Path:
    return self.base_path / run_id  # VULNERABLE to path traversal
```

**Rationale:** Prevents attackers from escaping the base directory using `../` sequences.

### Validation Patterns

#### Parameter Name Validation
- Validate parameter names using regex to ensure they follow safe naming conventions
- Parameter names must be valid Python identifiers (alphanumeric + underscores, starting with letter/underscore)

**Correct:**
```python
PARAM_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

if not PARAM_NAME_PATTERN.match(name):
    raise ValueError(
        f"Invalid parameter name '{name}'. "
        f"Parameter names must be alphanumeric with underscores only."
    )
```

**Incorrect:**
```python
# No validation - any name accepted
value = os.environ.get(env_name)
```

**Rationale:** Prevents confusion and potential security issues from invalid parameter names.

#### Date Parsing with Range Validation
- Always validate dates from user input with reasonable bounds
- Set year range limits (e.g., 2000-2100 for data engineering)
- Provide clear error messages that include the parameter name

**Correct:**
```python
def parse_date_safely(date_str: str, param_name: str = "date") -> datetime:
    dt = datetime.fromisoformat(date_str)
    if dt.year < 2000 or dt.year > 2100:
        raise ValueError(f"{param_name}: Year {dt.year} is out of valid range (2000-2100)")
    if dt > datetime.now() + timedelta(days=365):
        raise ValueError(f"{param_name}: Date too far in future")
    return dt
```

**Incorrect:**
```python
dt = datetime.fromisoformat(date_str)  # No validation - accepts invalid dates
```

**Rationale:** Prevents errors from unreasonable dates and provides clear error messages.

#### Consistent Type Conversion
- Accept multiple string representations for boolean values for user-friendliness
- Centralize type conversion logic in a dedicated module

**Correct:**
```python
def convert_to_bool(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.lower().strip()
        if normalized in ('true', '1', 'yes', 'on'):
            return True
        if normalized in ('false', '0', 'no', 'off'):
            return False
    return bool(value)
```

**Incorrect:**
```python
# Only 'true'/'false' accepted
if value == 'true':
    return True
elif value == 'false':
    return False
else:
    raise ValueError(f"Invalid boolean: {value}")
```

**Rationale:** Users may expect different boolean representations - accepting common variants improves UX.

### API Design Patterns

#### Reserved Parameter Name Handling
- When user-provided names could collide with system names, emit warnings
- Ensure system values always take precedence over user parameters

**Correct:**
```python
RESERVED_PARAM_NAMES = {"run_id", "run_date", "project_id", "workspace_path"}

if key in RESERVED_PARAM_NAMES:
    warnings.warn(
        f"Parameter '{key}' collides with reserved system name. "
        f"System value will take precedence. Use a different name."
    )
```

**Incorrect:**
```python
# No validation - silent override
resolved[key] = cli_params[key]
```

**Rationale:** Maintains backward compatibility while alerting users to potential issues.

### Code Quality Patterns

#### Avoid Python Built-in Shadowing
- Never use Python built-in names (type, list, dict, id, input) as variable or parameter names

**Correct:**
```python
def get_param(name: str, param_type: Optional[Type[T]] = None) -> Any:
    if param_type is not None:
        return convert_to_type(value, param_type)
```

**Incorrect:**
```python
def get_param(name: str, type: Optional[Type[T]] = None) -> Any:
    if type is not None:
        return convert_to_type(value, type)  # 'type' shadows built-in
```

**Rationale:** Shadowing built-ins causes confusion and can hide bugs.

#### Shared Type Conversion Module
- Centralize type conversion logic in a dedicated module to ensure consistency
- Avoid duplicating conversion logic across multiple files

**Correct:**
```python
# type_conversion.py
def convert_to_type(value: Any, target_type: Type) -> Any:
    if target_type == bool:
        return convert_to_bool(value)
    # ... other types

# Used in both resolver and helpers
from .type_conversion import convert_to_type
```

**Incorrect:**
```python
# Duplicated in resolver.py
def _type_convert(self, value: str) -> Any:
    if value.lower() in ('true', 'false'):
        return value == 'true'

# Duplicated differently in helpers.py
def get_param(name, type=None):
    if type == bool:
        return bool(value)  # Different logic!
```

**Rationale:** Single source of truth ensures consistent behavior and easier maintenance.

### Testing Patterns
- Use pytest fixtures from `conftest.py`
- Mock database operations
- E2E tests in `tests/e2e/`
- CLI tests use `typer.testing.CliRunner`

## Recent Major Features (Jan 2026)

1. **Feature Group Versioning** (#1405, #1407)
   - `list_versions()`, `get_version()`, `compare_versions()` methods
   - CLI: `seeknal version list/show/diff`
   - Supports version-specific materialization

2. **Feature Validation Framework** (#1395)
   - Validators in `feature_validation/validators.py`
   - CLI: `seeknal validate-features`
   - Configurable validation modes (warn/fail)

3. **Complete Table Deletion** (#1401, #1402, #1403)
   - Delete feature groups with storage cleanup
   - CLI: `seeknal delete feature-group <name>`
   - Handles both offline and online stores

4. **API Reference Documentation** (#1393)
   - Comprehensive API docs in `docs/api/`
   - Examples in `docs/examples/`

5. **Comprehensive Getting Started Guide** (#1392, #1399)
   - `docs/getting-started-comprehensive.md`
   - DuckDB and Spark quickstarts

## Common Tasks

### Adding a New CLI Command
1. Edit `src/seeknal/cli/main.py`
2. Define command with Typer decorator: `@app.command()`
3. Use helper functions: `_echo_success()`, `_echo_error()`, `_echo_info()`
4. Add tests in `tests/cli/test_*.py`

### Adding a New Feature Group Method
1. Edit `src/seeknal/featurestore/feature_group.py`
2. Add decorators if needed (`@require_workspace`, `@require_saved`)
3. Use `context.project_id` for project context
4. Update `request.py` for any new database operations
5. Add tests in `tests/test_feature_group.py`

### Adding a New Validation Rule
1. Create validator in `feature_validation/validators.py`
2. Inherit from `BaseValidator`
3. Implement `validate()` method
4. Register in validation config
5. Add tests in `tests/test_feature_validators.py`

### Adding Security Validation
1. Use `path_security.is_insecure_path()` for path validation
2. Sanitize user input with regex before using in file paths
3. Use `parse_date_safely()` for date validation from CLI/user input
4. Validate parameter names with `PARAM_NAME_PATTERN` regex
5. Use `convert_to_type()` from `type_conversion.py` for type conversion

### Modifying Database Schema
1. Update models in `models.py`
2. Update request layer in `request.py`
3. Consider migration path for existing databases
4. Update tests

## Code Style & Quality

- **Formatting**: Black (line length 100)
- **Type Hints**: Use extensively
- **Docstrings**: Google style for public APIs
- **Testing**: Aim for high coverage on new features
- **Error Handling**: Custom exceptions in `exceptions/`

## Working with Engines

### Spark Engine (`tasks/sparkengine/`)
- Pure PySpark implementation (no JVM required)
- Transformers, aggregators, loaders, extractors
- Uses `delta-spark` and `pyspark` packages

### DuckDB Engine (`tasks/duckdb/`)
- **Native Python implementation** - No JVM required
- **Preferred for new features** - Easier to develop and debug
- **Faster for small-to-medium datasets** - <100M rows
- **Storage format**: Parquet + JSON metadata
- **Module**: `src/seeknal/featurestore/duckdbengine/`
- **Demo notebook**: `duckdb_feature_store_demo.ipynb` (73K real data rows)

#### DuckDB vs Spark

| Aspect | DuckDB | Spark |
|--------|--------|-------|
| **Setup** | Pure Python, pip install | Requires JVM, Spark installation |
| **Memory** | Lightweight, in-process | High memory footprint |
| **Storage** | Parquet + JSON metadata | Delta Lake format |
| **Performance** | Fast for <100M rows | Optimized for big data |
| **Cost** | Lower compute costs | Higher infrastructure costs |
| **Use case** | Single-node, dev/test | Distributed, production-scale |

#### DuckDB Feature Store API

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
    FeatureLookup,
    Materialization,
)

# Create feature group (identical API to Spark)
fg = FeatureGroupDuckDB(
    name="my_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(event_time_col="timestamp"),
    project="my_project"
)

# Works with Pandas DataFrames
fg.set_dataframe(pd_df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))

# Historical features with point-in-time joins
lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))

# Online serving
online_table = hist.serve(name="my_features_online")
features = online_table.get_features(keys=[{"user_id": "123"}])
```

#### Migration from Spark to DuckDB

Only **2 line changes** needed:

1. Import path: `.duckdbengine.feature_group` instead of `.feature_group`
2. DataFrame type: Pandas instead of Spark

**Before (Spark)**:
```python
from seeknal.featurestore.feature_group import FeatureGroup
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("my_data")
```

**After (DuckDB)**:
```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB
import pandas as pd

df = pd.read_parquet("my_data.parquet")
```

Everything else (API, features, materialization) is **identical**!

#### DuckDB Performance Benchmarks

Based on real dataset (73,194 rows × 35 columns):
- **Write**: 0.08s (897K rows/sec)
- **Read**: 0.02s (3.6M rows/sec)
- **Point-in-time join**: <0.5s

#### When to Use DuckDB

✅ **Use DuckDB when**:
- Dataset <100M rows
- Single-node deployment
- Development/testing environment
- Rapid prototyping
- Cost-sensitive deployment
- Team prefers pure Python

❌ **Use Spark when**:
- Dataset >100M rows
- Distributed processing required
- Existing Spark infrastructure
- Need Delta Lake features

## Configuration

- Main config: `config.toml`
- Environment vars: `.env` file
- Key env vars:
  - `SEEKNAL_BASE_CONFIG_PATH`: Base directory
  - `SEEKNAL_USER_CONFIG_PATH`: User config file
  - `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`: For Turso

## Git Workflow

- Main branch: `main`
- Auto-claude worktrees: `.worktrees/` directory
- Integration command: `seeknal integrate` (custom command)
- Recent integrations tracked in git history

## Documentation

- **README.md**: Installation and quick start
- **docs/getting-started-comprehensive.md**: Full tutorial
- **docs/api/**: API reference
- **docs/examples/**: Code examples
- Always update docs when adding features

## Known Issues & Gotchas

1. **Spark Engine**: Pure PySpark implementation (no JVM required for transformers)
2. **Path Security**: Always validate paths with `path_security.py` and sanitize user input
3. **SQL Injection**: Use validation functions from `validation.py`
4. **Context Required**: Most operations need workspace/project context
5. **Version Tracking**: Schema changes auto-create new versions
6. **Date Validation**: Always use `parse_date_safely()` for user-provided dates
7. **Parameter Names**: Must follow Python identifier syntax (no hyphens or special chars)
8. **Reserved Names**: Avoid using `run_id`, `run_date`, `project_id`, `workspace_path` as parameter names

## Testing Before Commit

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_feature_group.py

# Run CLI tests
pytest tests/cli/

# Run E2E tests
pytest tests/e2e/
```

## Useful Commands

```bash
# List all CLI commands
seeknal --help

# Initialize project
seeknal init --name my_project

# List feature groups
seeknal list feature-groups

# Materialize features
seeknal materialize <fg_name> --start-date 2024-01-01

# Version management
seeknal version list <fg_name>
seeknal version show <fg_name> --version 1
seeknal version diff <fg_name> --from 1 --to 2

# Validate features
seeknal validate-features <fg_name> --mode fail

# Delete feature group
seeknal delete feature-group <fg_name>
```

## When Making Changes

1. **Check Context**: Understand recent work from git history and session context
2. **Read Tests**: Tests show expected behavior
3. **Update Docs**: Keep documentation in sync
4. **Follow Patterns**: Use existing decorators, error handling, CLI patterns
5. **Security First**:
   - Validate all inputs at initialization (fail fast with clear errors)
   - Sanitize user input used in file paths
   - Use shared validation modules (type_conversion.py, validation.py, path_security.py)
   - Test with malicious inputs (path traversal, invalid dates, special characters)
6. **Version Awareness**: Schema changes create new versions automatically

## Resources

- **Repository**: https://github.com/mta-tech/seeknal
- **Releases**: https://github.com/mta-tech/seeknal/releases
- **Documentation**: `docs/` directory
- **Examples**: `docs/examples/` and demo notebooks

---

**Last Updated**: February 2026 based on recent development context (Security and Validation patterns)

# Instructions MUST FOLLOW when work

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
