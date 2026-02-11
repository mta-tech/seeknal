# Implementation Plan: YAML and Python Script Parameterization

**Date:** 2025-02-10
**Based on:** `docs/brainstorms/2026-02-10-parameterization-brainstorm.md`
**Feature Name:** `parameterization`

---

## Overview

Add parameterization system to Seeknal YAML pipelines and Python scripts, enabling dynamic value substitution for scheduled runs, environment-specific configs, and runtime parameters.

**Key Syntax:** Jinja-style `{{ }}` in YAML files
**Python Integration:** `get_param()` helper function
**Resolution:** CLI flags + auto-resolution with sensible defaults

---

## Architecture

### Approach: DAG Builder Integration (Approach B)

Parameters will be resolved during node construction in `DAGBuilder._parse_yaml_file()`, giving full access to Seeknal context.

**Why this approach:**
- Deep context access (project_id, workspace, etc.)
- CLI override integration is natural
- Consistent with existing date parameter patterns
- Type-aware resolution (not just string replacement)

---

## Files to Create

```
src/seeknal/workflow/parameters/
├── __init__.py           # Public API exports
├── resolver.py           # ParameterResolver class
├── functions.py          # Built-in parameter functions
└── helpers.py            # get_param() for Python scripts
```

## Files to Modify

| File | Changes |
|------|---------|
| `src/seeknal/workflow/dag.py` | Add parameter resolution in `_parse_yaml_file()` |
| `src/seeknal/cli/main.py` | Add CLI flags: `--date`, `--run-id`, etc. |
| `src/seeknal/workflow/executors/base.py` | Extend `ExecutionContext` with resolved params |
| `src/seeknal/workflow/executors/__init__.py` | Export `get_param` helper |

---

## Implementation Tasks

### Task 1: Create Parameter Functions Module

**File:** `src/seeknal/workflow/parameters/functions.py`

```python
"""Built-in parameter functions for YAML parameterization.

Functions support the syntax:
- {{today}}           -> 2025-02-10
- {{today(-1)}}       -> 2025-02-09
- {{month_start}}     -> 2025-02-01
- {{env:VAR|default}} -> os.environ.get("VAR", "default")
- {{run_id}}          -> UUID
- {{run_date}}        -> ISO timestamp
"""

from datetime import datetime, date
from typing import Any, Optional
import os
import uuid

def today(offset_days: int = 0) -> str:
    """Return today's date in ISO format.

    Args:
        offset_days: Days offset from today (negative for past)

    Returns:
        ISO formatted date string (YYYY-MM-DD)
    """
    from datetime import timedelta
    d = date.today() + timedelta(days=offset_days)
    return d.isoformat()

def month_start(months_offset: int = 0) -> str:
    """Return first day of current/offset month.

    Args:
        months_offset: Months offset (negative for past months)

    Returns:
        ISO formatted date string (YYYY-MM-DD)
    """
    from datetime import timedelta
    import calendar
    today_date = date.today()

    # Calculate target month/year
    year = today_date.year
    month = today_date.month + months_offset

    # Handle year rollover
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1

    return date(year, month, 1).isoformat()

def year_start(years_offset: int = 0) -> str:
    """Return first day of current/offset year."""
    today_date = date.today()
    return date(today_date.year + years_offset, 1, 1).isoformat()

def env_var(var_name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default.

    Args:
        var_name: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default

    Raises:
        ValueError: If var_name not set and no default provided
    """
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        raise ValueError(f"Environment variable '{var_name}' not set and no default provided")
    return value

# Built-in function registry
FUNCTION_REGISTRY = {
    "today": today,
    "yesterday": lambda: today(-1),
    "month_start": month_start,
    "year_start": year_start,
    "env": env_var,
}
```

**Acceptance Criteria:**
- [ ] `today()` returns current date in ISO format
- [ ] `today(-1)` returns yesterday's date
- [ ] `month_start()` returns first day of current month
- [ ] `month_start(-1)` returns first day of last month
- [ ] `env_var("VAR", "default")` returns default if VAR not set
- [ ] `env_var("VAR")` raises ValueError if VAR not set and no default

---

### Task 2: Create Parameter Resolver

**File:** `src/seeknal/workflow/parameters/resolver.py`

```python
"""Parameter resolution for YAML pipeline configuration.

Parses and resolves {{ }} parameter expressions in YAML node configs.
Supports CLI overrides and context-aware resolution.
"""

import re
from typing import Any, Dict, Optional
from pathlib import Path

from seeknal.context import context
from .functions import FUNCTION_REGISTRY


class ParameterResolver:
    """Resolve {{ }} parameters in YAML configurations.

    Supports:
    - Date/time functions: {{today}}, {{month_start}}
    - Environment variables: {{env:VAR|default}}
    - Runtime context: {{run_id}}, {{run_date}}, {{project_id}}
    - CLI overrides (passed via kwargs)
    """

    # Pattern to match {{ variable }} or {{ function(args) }}
    PARAM_PATTERN = re.compile(r'\{\{\s*([^}]+)\s*\}\}')

    def __init__(
        self,
        cli_overrides: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ):
        """Initialize resolver.

        Args:
            cli_overrides: Parameter values from CLI flags
            run_id: Custom run ID (or auto-generated)
        """
        self.cli_overrides = cli_overrides or {}
        self.run_id = run_id or str(uuid.uuid4())
        self.run_date = datetime.now().isoformat()

        # Build context values
        self._context_values = {
            "run_id": self.run_id,
            "run_date": self.run_date,
        }

        # Add Seeknal context values if available
        try:
            if hasattr(context, 'project_id'):
                self._context_values["project_id"] = context.project_id
            if hasattr(context, 'workspace_path'):
                self._context_values["workspace_path"] = str(context.workspace_path)
        except Exception:
            pass  # Context may not be initialized

    def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve all parameters in a dictionary.

        Args:
            params: Dictionary with potential {{ }} expressions

        Returns:
            Dictionary with all {{ }} expressions resolved
        """
        if not params:
            return {}

        resolved = {}
        for key, value in params.items():
            # Check CLI override first
            if key in self.cli_overrides:
                resolved[key] = self.cli_overrides[key]
                continue

            # Resolve {{ }} expressions
            if isinstance(value, str):
                resolved[key] = self._resolve_string(value)
            elif isinstance(value, dict):
                resolved[key] = self.resolve(value)
            elif isinstance(value, list):
                resolved[key] = [self._resolve_item(v) for v in value]
            else:
                resolved[key] = value

        return resolved

    def _resolve_string(self, value: str) -> Any:
        """Resolve {{ }} expressions in a string.

        Args:
            value: String potentially containing {{ }} expressions

        Returns:
            Resolved value (converted to int/bool if applicable)
        """
        def replace_match(match):
            expr = match.group(1).strip()

            # Check for context values first
            if expr in self._context_values:
                return self._context_values[expr]

            # Check for function call: func(arg1, arg2)
            if '(' in expr:
                return self._resolve_function(expr)

            # Simple value lookup
            if expr in FUNCTION_REGISTRY:
                return str(FUNCTION_REGISTRY[expr]())

            # Check for env:VAR|default syntax
            if expr.startswith('env:'):
                parts = expr[4:].split('|', 1)
                var_name = parts[0]
                default = parts[1] if len(parts) > 1 else None
                from .functions import env_var
                return env_var(var_name, default)

            return match.group(0)  # Return original if not resolved

        result = self.PARAM_PATTERN.sub(replace_match, value)

        # Type conversion
        return self._convert_type(result)

    def _resolve_function(self, expr: str) -> str:
        """Resolve function call like today(-1) or env(VAR|default)."""
        # Parse: func_name(arg1, arg2, ...)
        func_match = re.match(r'(\w+)\((.*)\)', expr)
        if not func_match:
            return expr

        func_name = func_match.group(1)
        args_str = func_match.group(2).strip()

        # Parse arguments (handle strings, numbers)
        args = self._parse_args(args_str) if args_str else []

        # Call function
        if func_name in FUNCTION_REGISTRY:
            return str(FUNCTION_REGISTRY[func_name](*args))

        # Handle env() separately due to pipe syntax
        if func_name == "env":
            parts = args_str.split('|', 1) if args_str else []
            var_name = parts[0] if parts else None
            default = parts[1] if len(parts) > 1 else None
            from .functions import env_var
            return env_var(var_name, default)

        return expr

    def _parse_args(self, args_str: str) -> list:
        """Parse function arguments, handling numbers and negatives."""
        if not args_str:
            return []

        # Simple split by comma for now
        # TODO: Handle quoted strings with commas
        args = [a.strip() for a in args_str.split(',')]

        # Convert numeric arguments
        converted = []
        for arg in args:
            try:
                # Try integer first
                converted.append(int(arg))
            except ValueError:
                try:
                    # Try float
                    converted.append(float(arg))
                except ValueError:
                    # Keep as string
                    converted.append(arg)

        return converted

    def _resolve_item(self, item: Any) -> Any:
        """Resolve item in a list."""
        if isinstance(item, str):
            return self._resolve_string(item)
        elif isinstance(item, dict):
            return self.resolve(item)
        elif isinstance(item, list):
            return [self._resolve_item(i) for i in item]
        return item

    def _convert_type(self, value: str) -> Any:
        """Convert string to appropriate type."""
        # Boolean
        if value.lower() == 'true':
            return True
        if value.lower() == 'false':
            return False

        # Integer
        try:
            if '.' not in value:
                return int(value)
        except ValueError:
            pass

        # Float
        try:
            return float(value)
        except ValueError:
            pass

        return value
```

**Acceptance Criteria:**
- [ ] `resolve({"date": "{{today}}"})` returns `{"date": "2025-02-10"}`
- [ ] CLI overrides take precedence
- [ ] Context values (run_id, project_id) are accessible
- [ ] Type conversion works (int, bool, float)
- [ ] Nested dict/list resolution works

---

### Task 3: Create Helper Function for Python

**File:** `src/seeknal/workflow/parameters/helpers.py`

```python
"""Helper functions for accessing resolved parameters in Python scripts.

Provides a clean, explicit API for parameter access with type conversion.
"""

from typing import Any, Optional, Type, TypeVar
import os

T = TypeVar('T')


def get_param(
    name: str,
    default: Optional[Any] = None,
    type: Optional[Type[T]] = None,
) -> Any:
    """Get a resolved parameter value.

    Parameters are resolved from YAML and made available via environment
    variables with a SEEKNAL_PARAM_ prefix.

    Args:
        name: Parameter name (without prefix)
        default: Default value if parameter not found
        type: Expected type for conversion (int, float, bool, str)

    Returns:
        Parameter value, converted to specified type if provided

    Examples:
        >>> run_date = get_param("run_date")
        >>> batch_size = get_param("batch_size", type=int)
        >>> region = get_param("region", default="us-east-1")
        >>> enabled = get_param("enabled", type=bool)
    """
    env_name = f"SEEKNAL_PARAM_{name.upper()}"
    value = os.environ.get(env_name, default)

    if value is None:
        if default is not None:
            return default
        raise KeyError(f"Parameter '{name}' not found and no default provided")

    # Type conversion
    if type is not None:
        if type == bool:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        if type == int:
            return int(value)
        if type == float:
            return float(value)
        if type == str:
            return str(value)
        # For other types, try direct conversion
        return type(value)

    return value


def list_params() -> dict[str, str]:
    """List all available Seeknal parameters.

    Returns:
        Dictionary of parameter names to values (without prefix)
    """
    params = {}
    prefix = "SEEKNAL_PARAM_"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            param_name = key[len(prefix):].lower()
            params[param_name] = value
    return params
```

**Acceptance Criteria:**
- [ ] `get_param("run_date")` returns resolved date
- [ ] `get_param("missing", default="foo")` returns "foo"
- [ ] `get_param("count", type=int)` converts to int
- [ ] `get_param("enabled", type=bool)` handles "true"/"false"
- [ ] `list_params()` returns all SEEKNAL_PARAM_* variables

---

### Task 4: Update DAGBuilder Integration

**File:** `src/seeknal/workflow/dag.py`

Add parameter resolution to `_parse_yaml_file()` method:

```python
# Add import at top
from seeknal.workflow.parameters.resolver import ParameterResolver

# In DAGBuilder class, add __init__ parameter:
def __init__(
    self,
    project_path: Path,
    cli_overrides: Optional[Dict[str, Any]] = None,  # NEW
    run_id: Optional[str] = None,  # NEW
):
    self.project_path = project_path
    self.cli_overrides = cli_overrides or {}  # NEW
    self.run_id = run_id  # NEW
    # ... rest of init

# In _parse_yaml_file(), after creating node:
def _parse_yaml_file(self, file_path: Path) -> None:
    # ... existing parsing code ...

    # Create node (existing code)
    node = DAGNode(
        qualified_name=qualified_name,
        kind=kind,
        name=name,
        file_path=str(file_path.resolve()),
        yaml_data=data,  # Will be modified below
        description=data.get("description"),
        owner=data.get("owner"),
        tags=data.get("tags", []),
    )

    # NEW: Resolve parameters
    if "params" in data:
        resolver = ParameterResolver(
            cli_overrides=self.cli_overrides,
            run_id=self.run_id,
        )
        resolved_params = resolver.resolve(data["params"])
        node.yaml_data["params"] = resolved_params

    self.nodes[qualified_name] = node
```

**Acceptance Criteria:**
- [ ] Parameters in YAML `params:` section are resolved
- [ ] CLI overrides are passed through DAGBuilder
- [ ] Resolved values are stored in `node.yaml_data["params"]`

---

### Task 5: Extend ExecutionContext

**File:** `src/seeknal/workflow/executors/base.py`

Add resolved params to ExecutionContext:

```python
class ExecutionContext:
    """Encapsulates the execution environment for node execution.

    This context is passed to all executors and provides access to:
    - Database connections (DuckDB, Spark)
    - File system paths
    - Configuration and parameters
    - State and caching interfaces

    Attributes:
        project_name: Name of the current project
        workspace_path: Path to the workspace directory
        target_path: Path to the target/build directory
        state_dir: Path to the state storage directory
        duckdb_connection: Optional DuckDB connection (if using DuckDB)
        spark_session: Optional Spark session (if using Spark)
        config: Additional configuration dictionary
        dry_run: Whether running in dry-run mode
        verbose: Whether to enable verbose output
        params: Resolved parameters for this node (NEW)
    """

    def __init__(
        self,
        project_name: str,
        workspace_path: Path,
        target_path: Path,
        state_dir: Optional[Path] = None,
        duckdb_connection: Optional[Any] = None,
        spark_session: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        verbose: bool = False,
        materialize_enabled: Optional[bool] = None,
        params: Optional[Dict[str, Any]] = None,  # NEW
    ):
        self.project_name = project_name
        self.workspace_path = workspace_path
        self.target_path = target_path
        self.state_dir = state_dir or target_path / "state"
        self.duckdb_connection = duckdb_connection
        self.spark_session = spark_session
        self.config = config or {}
        self.dry_run = dry_run
        self.verbose = verbose
        self.materialize_enabled = materialize_enabled
        self.params = params or {}  # NEW
```

**Acceptance Criteria:**
- [ ] ExecutionContext has `params` attribute
- [ ] Params are passed when creating ExecutionContext

---

### Task 6: Update CLI

**File:** `src/seeknal/cli/main.py`

Add CLI flags for parameter overrides:

```python
@app.command()
def run(
    flow_name: Optional[str] = typer.Argument(
        None,
        help="Name of the flow to run (legacy). If omitted, executes YAML pipeline from seeknal/ directory."
    ),
    # ... existing date flags ...
    start_date: Optional[str] = typer.Option(
        None, "--start-date", "-s",
        help="Start date for the flow (YYYY-MM-DD)"
    ),
    # NEW: Parameter override flags
    param_date: Optional[str] = typer.Option(
        None, "--date",
        help="Override date parameter (YYYY-MM-DD)"
    ),
    param_run_id: Optional[str] = typer.Option(
        None, "--run-id",
        help="Custom run ID for parameterization"
    ),
    # ... rest of existing flags ...
):
    """... existing docstring ..."""

    # Build CLI overrides dict for parameter resolution
    cli_overrides = {}
    if param_date:
        cli_overrides["date"] = param_date
        cli_overrides["run_date"] = param_date
        cli_overrides["today"] = param_date
    if param_run_id:
        cli_overrides["run_id"] = param_run_id

    # In YAML pipeline mode:
    else:
        # Pass cli_overrides to DAGBuilder
        _run_yaml_pipeline(
            cli_overrides=cli_overrides,  # NEW
            run_id=param_run_id,          # NEW
            dry_run=dry_run,
            full=full,
            nodes=nodes,
            types=types,
            exclude_tags=exclude_tags,
            continue_on_error=continue_on_error,
            retry=retry,
            show_plan=show_plan,
            parallel=parallel,
            max_workers=max_workers,
            materialize=materialize,
        )
```

**Update `_run_yaml_pipeline` signature:**

```python
def _run_yaml_pipeline(
    cli_overrides: Optional[Dict[str, Any]] = None,  # NEW
    run_id: Optional[str] = None,                   # NEW
    dry_run: bool = False,
    full: bool = False,
    # ... rest of params ...
) -> None:
    """..."""

    # Pass to DAGBuilder
    dag_builder = DAGBuilder(
        project_path=project_path,
        cli_overrides=cli_overrides,  # NEW
        run_id=run_id,                # NEW
    )
    dag_builder.build()

    # ... rest of function ...

    # Pass params to ExecutionContext
    exec_context = ExecutionContext(
        project_name=project_path.name,
        workspace_path=project_path,
        target_path=target_path,
        dry_run=dry_run,
        verbose=True,
        materialize_enabled=materialize,
        params=node.yaml_data.get("params", {}),  # NEW
    )
```

**Acceptance Criteria:**
- [ ] `--date 2025-01-15` overrides date parameters
- [ ] `--run-id custom-123` sets custom run ID
- [ ] Overrides are passed to DAGBuilder
- [ ] Params are passed to ExecutionContext

---

### Task 7: Export Public API

**File:** `src/seeknal/workflow/parameters/__init__.py`

```python
"""Parameter resolution for YAML pipelines and Python scripts.

This module provides parameter substitution for dynamic values in YAML
configurations and Python scripts.

Example:
    ```yaml
    kind: source
    name: daily_events
    params:
      path: "data/{{today}}/*.parquet"
    ```

    ```python
    from seeknal.workflow.parameters import get_param

    run_date = get_param("run_date")
    ```
"""

from .resolver import ParameterResolver
from .helpers import get_param, list_params
from .functions import today, month_start, year_start

__all__ = [
    "ParameterResolver",
    "get_param",
    "list_params",
    "today",
    "month_start",
    "year_start",
]
```

**File:** `src/seeknal/workflow/executors/__init__.py`

Add export:
```python
from .base import ExecutionContext, ExecutorResult
from seeknal.workflow.parameters import get_param  # NEW

__all__ = [
    "ExecutionContext",
    "ExecutorResult",
    "get_param",  # NEW
    # ... rest of exports
]
```

**File:** `src/seeknal/__init__.py` (optional, for top-level access)

```python
# Re-export for convenience
from seeknal.workflow.parameters import get_param

__all__ = ["get_param"]
```

**Acceptance Criteria:**
- [ ] Can import `get_param` from `seeknal.workflow.parameters`
- [ ] Can import `get_param` from `seeknal.workflow.executors`
- [ ] Can import `get_param` directly from `seeknal`

---

### Task 8: Set Environment Variables in Python Executor

**File:** `src/seeknal/workflow/executors/python_executor.py`

Add environment variable injection:

```python
def run(self) -> ExecutorResult:
    """Execute Python script with uv."""

    # NEW: Set resolved params as environment variables
    if self.context.params:
        for key, value in self.context.params.items():
            env_key = f"SEEKNAL_PARAM_{key.upper()}"
            env[env_key] = str(value)

    # Existing environment variable setting
    env["SEEKNAL_PROJECT_PATH"] = str(self.context.workspace_path)
    # ... rest of executor code ...
```

**Acceptance Criteria:**
- [ ] Resolved params are set as `SEEKNAL_PARAM_*` environment variables
- [ ] `get_param()` can read these values in Python scripts

---

## Testing Strategy

### Unit Tests

**File:** `tests/workflow/test_parameterization.py`

```python
"""Tests for parameter resolution system."""

import pytest
from datetime import date
from seeknal.workflow.parameters.resolver import ParameterResolver
from seeknal.workflow.parameters.functions import today, month_start, env_var
from seeknal.workflow.parameters.helpers import get_param, list_params
import os


class TestParameterFunctions:
    """Test built-in parameter functions."""

    def test_today_returns_current_date(self):
        result = today()
        assert result == date.today().isoformat()

    def test_today_with_offset(self):
        result = today(-1)
        expected = (date.today() - timedelta(days=1)).isoformat()
        assert result == expected

    def test_month_start(self):
        result = month_start()
        assert result.endswith("-01")

    def test_env_var_with_default(self):
        os.environ["TEST_VAR"] = "test_value"
        assert env_var("TEST_VAR") == "test_value"
        assert env_var("MISSING_VAR", "default") == "default"

    def test_env_var_raises_without_default(self):
        with pytest.raises(ValueError):
            env_var("NONEXISTENT_VAR")


class TestParameterResolver:
    """Test ParameterResolver class."""

    def test_resolve_simple_param(self):
        resolver = ParameterResolver()
        result = resolver.resolve({"date": "{{today}}"})
        assert "date" in result
        assert result["date"] == date.today().isoformat()

    def test_resolve_with_cli_override(self):
        resolver = ParameterResolver(cli_overrides={"date": "2025-01-15"})
        result = resolver.resolve({"date": "{{today}}"})
        assert result["date"] == "2025-01-15"

    def test_resolve_nested_dict(self):
        resolver = ParameterResolver()
        result = resolver.resolve({
            "config": {
                "date": "{{today}}",
                "count": 10
            }
        })
        assert result["config"]["date"] == date.today().isoformat()
        assert result["config"]["count"] == 10

    def test_resolve_list(self):
        resolver = ParameterResolver()
        result = resolver.resolve({
            "dates": ["{{today}}", "{{today(-1)}}"]
        })
        assert len(result["dates"]) == 2

    def test_run_id_context(self):
        resolver = ParameterResolver(run_id="test-run-123")
        result = resolver.resolve({"id": "{{run_id}}"})
        assert result["id"] == "test-run-123"


class TestHelperFunctions:
    """Test get_param helper function."""

    def setup_method(self):
        # Set up test environment
        os.environ["SEEKNAL_PARAM_RUN_DATE"] = "2025-02-10"
        os.environ["SEEKNAL_PARAM_COUNT"] = "100"
        os.environ["SEEKNAL_PARAM_ENABLED"] = "true"

    def teardown_method(self):
        # Clean up
        for key in list(os.environ.keys()):
            if key.startswith("SEEKNAL_PARAM_"):
                del os.environ[key]

    def test_get_param_string(self):
        result = get_param("run_date")
        assert result == "2025-02-10"

    def test_get_param_with_type_conversion(self):
        result = get_param("count", type=int)
        assert result == 100
        assert isinstance(result, int)

    def test_get_param_bool_conversion(self):
        result = get_param("enabled", type=bool)
        assert result is True

    def test_get_param_with_default(self):
        result = get_param("missing", default="default_value")
        assert result == "default_value"

    def test_get_param_raises_without_default(self):
        with pytest.raises(KeyError):
            get_param("nonexistent")

    def test_list_params(self):
        result = list_params()
        assert "run_date" in result
        assert "count" in result
        assert result["run_date"] == "2025-02-10"
```

### Integration Tests

**File:** `tests/cli/test_parameterization_cli.py`

```python
"""Test CLI parameter override functionality."""

from typer.testing import CliRunner
from seeknal.cli.main import app

runner = CliRunner()


def test_date_override_sets_param(tmp_path):
    """Test --date flag overrides date parameter."""
    # Create test YAML with {{today}}
    # Run with --date 2025-01-15
    # Verify resolved value
    pass


def test_run_id_override(tmp_path):
    """Test --run-id flag sets custom run ID."""
    pass


def test_dry_run_shows_resolved_values(tmp_path):
    """Test --dry-run shows resolved parameters."""
    pass
```

### E2E Test

**File:** `tests/workflow/test_parameterization_e2e.py`

```python
"""End-to-end test for parameterization."""

def test_daily_pipeline_with_date_parameter(tmp_path):
    """Test complete pipeline with {{today}} parameter."""

    # Create YAML source with {{today}}
    # Create transform using parameter
    # Run pipeline
    # Verify correct date was used
    pass


def test_python_script_get_param(tmp_path):
    """Test Python script accessing get_param()."""

    # Create YAML with params
    # Create Python script using get_param()
    # Run pipeline
    # Verify script received correct values
    pass
```

---

## Implementation Order

**Phase 1: Core Resolution (Tasks 1-4)**
1. Create functions.py
2. Create resolver.py
3. Create helpers.py
4. Update DAGBuilder integration

**Phase 2: Execution Integration (Tasks 5-8)**
5. Extend ExecutionContext
6. Update CLI
7. Export public API
8. Set environment variables in Python executor

**Phase 3: Testing**
9. Write unit tests
10. Write integration tests
11. Write E2E test
12. Manual testing

---

## Documentation Updates

After implementation:

1. **Update `docs/getting-started-comprehensive.md`**
   - Add section on parameterization
   - Include examples with {{today}}, {{env:}}

2. **Create `docs/guides/parameterization.md`**
   - Complete parameter reference
   - Python script examples
   - CLI override examples

3. **Update `docs/examples/`**
   - Add example pipeline with parameters
   - Add example Python script using get_param()

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Breaking existing pipelines | Low | High | Parameters only resolved in `params:` section |
| Performance overhead | Low | Low | Resolution is O(n) on param count |
| Type conversion errors | Medium | Medium | Graceful fallback to string type |
| Complex function parsing | Medium | Low | Start simple, add complexity incrementally |

---

## Success Criteria

- [ ] Can use `{{today}}` in YAML and get current date
- [ ] Can override with `--date` CLI flag
- [ ] Python scripts can access params via `get_param()`
- [ ] Environment variables `{{env:VAR}}` work correctly
- [ ] All tests pass
- [ ] Documentation is updated
- [ ] Backward compatibility maintained (existing pipelines work)

---

## Open Questions for Implementation

1. **Should we support nested parameter expressions?** (e.g., `{{month_start({{today(-30)}})}}`)
   - Decision: No for MVP (YAGNI), can add later

2. **Should parameters be resolved in `transform:` SQL blocks?**
   - Decision: Only in `params:` section for consistency
   - SQL can reference params via DuckDB variables

3. **How to handle parameter validation errors?**
   - Decision: Fail fast with clear error message

4. **Should we add a `--param KEY=VALUE` flag for arbitrary overrides?**
   - Decision: Yes, after core functionality works

---

## Next Steps

1. Review this plan with team
2. Create feature branch
3. Implement Phase 1 (Tasks 1-4)
4. Write tests for Phase 1
5. Implement Phase 2 (Tasks 5-8)
6. Write remaining tests
7. Update documentation
8. Create PR for review
