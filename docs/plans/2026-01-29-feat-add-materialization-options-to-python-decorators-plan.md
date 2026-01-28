---
title: Add Materialization Options to Python Pipeline Decorators
type: feat
date: 2026-01-29
---

# Add Materialization Options to Python Pipeline Decorators

## Overview

Extend Python pipeline decorators (`@transform`, `@source`, `@feature_group`) to support inline Iceberg materialization configuration, enabling developers to specify materialization settings directly in decorator parameters rather than requiring separate YAML configuration files.

**Current State:** Materialization is configured via YAML files or only in `@feature_group` decorator.

**Desired State:** All decorators support materialization parameters with proper configuration merging: Decorator > YAML > Profile > Defaults.

---

## Problem Statement

Currently, to enable Iceberg materialization for Python transforms, users must either:

1. Create a separate YAML file:
   ```yaml
   # seeknal/transforms/sales_forecast.yml
   name: sales_forecast
   kind: transform
   file: seeknal/pipelines/sales_forecast.py
   materialization:
     enabled: true
     table: "warehouse.prod.sales_forecast"
     mode: overwrite
   ```

2. Or only use `@feature_group` decorator with materialization (not available for `@transform` or `@source`)

This creates friction:
- **Split configuration**: Logic in Python, materialization in YAML
- **Verbosity**: Simple transforms require full YAML files
- **Inconsistency**: `@feature_group` supports inline materialization, but `@transform` doesn't

---

## Proposed Solution

Add materialization parameters to all Python pipeline decorators with intelligent configuration merging.

### API Design

All decorators use a **single `materialization` parameter** that accepts either a `dict` or a `MaterializationConfig` object. This groups related settings and matches the existing `@feature_group` pattern.

#### @transform decorator

**Recommended: Type-safe dataclass**

```python
from seeknal.pipeline.materialization import MaterializationConfig

@transform(
    name="sales_forecast",
    description="ML-based sales forecast",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.prod.sales_forecast",
        mode="overwrite",
        create_table=True,
    ),
)
def sales_forecast(ctx):
    # ... implementation
    return df
```

**Alternative: Dict (for quick scripting)**

```python
@transform(
    name="sales_forecast",
    description="ML-based sales forecast",
    materialization={
        "enabled": True,
        "table": "warehouse.prod.sales_forecast",
        "mode": "overwrite",
        "create_table": True,
    },
)
def sales_forecast(ctx):
    # ... implementation
    return df
```

#### @source decorator

```python
@source(
    name="raw_orders",
    source="csv",
    table="data/orders.csv",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.raw.orders",
        mode="append",
    ),
)
def raw_orders(ctx):
    # ... implementation
    return df
```

#### @feature_group decorator (already has support)

```python
@feature_group(
    name="user_features",
    entity="user",
    features={...},
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.online.user_features",
        mode="append",
    ),
)
def user_features(ctx):
    # ... implementation
    return df
```

**Benefits of grouped dataclass approach:**
- ✅ **Type-safe** - IDE autocomplete and type checking
- ✅ **Cleaner API** - groups related settings
- ✅ **Consistent** - matches `@feature_group` pattern
- ✅ **Self-documenting** - dataclass fields show available options
- ✅ **Extensible** - easy to add new options (partitioning, schema_evolution, etc.)

### Configuration Priority

```
Decorator Parameters (highest priority)
    ↓
YAML Override (seeknal/transforms/*.yml)
    ↓
Profile Config (~/.seeknal/profiles.yml)
    ↓
Defaults (system defaults)
```

**Example Override Behavior:**

```python
# Decorator says: enabled=True, table="warehouse.dev.table"
from seeknal.pipeline.materialization import MaterializationConfig

@transform(
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.dev.table"
    )
)

# YAML says: enabled=False, table="warehouse.prod.table"
# Result: enabled=False (YAML overrides decorator)
```

This allows YAML to disable materialization in specific environments (e.g., development).

---

## Technical Approach

### Phase 1: Extend Decorator Signatures

**File:** `src/seeknal/pipeline/decorators.py`

**New File:** `src/seeknal/pipeline/materialization_config.py` (optional type-safe config)

**Changes:**

1. **Create `MaterializationConfig` dataclass** (recommended approach):
   ```python
   # src/seeknal/pipeline/materialization_config.py
   from dataclasses import dataclass, field
   from typing import Optional

   @dataclass
   class MaterializationConfig:
       """Materialization configuration for pipeline nodes.

       Recommended approach for type-safe decorator configuration.
       Also accepts dict for backwards compatibility.

       Example:
           @transform(
               materialization=MaterializationConfig(
                   enabled=True,
                   table="warehouse.prod.table",
                   mode="overwrite",
               )
           )
           def my_transform(ctx): ...

       Or with dict:
           @transform(
               materialization={
                   "enabled": True,
                   "table": "warehouse.prod.table",
                   "mode": "overwrite",
               }
           )
           def my_transform(ctx): ...
       """
       enabled: Optional[bool] = None
       table: Optional[str] = None
       mode: Optional[str] = None  # "append" or "overwrite"
       create_table: Optional[bool] = None
       # Future: partition_by, Optional[str] = None
       # Future: schema_evolution, Optional[str] = None

       def to_dict(self) -> dict:
           """Convert to dict, excluding None values."""
           return {
               k: v for k, v in [
                   ("enabled", self.enabled),
                   ("table", self.table),
                   ("mode", self.mode),
                   ("create_table", self.create_table),
               ] if v is not None
           }

       @classmethod
       def from_dict(cls, config: dict) -> "MaterializationConfig":
           """Create from dict. Useful for loading from YAML."""
           return cls(**{
           k: v for k, v in config.items()
           if k in cls.__dataclass_fields__
       })
   ```

2. **Add `materialization` parameter to `@transform`:**
   ```python
   from typing import Optional, Union

   def transform(
       name: Optional[str] = None,
       sql: Optional[str] = None,
       inputs: Optional[list[str]] = None,
       tags: Optional[list[str]] = None,
       # New grouped parameter:
       materialization: Optional[Union[dict, MaterializationConfig]] = None,
       **params,
   ):
   ```

3. **Add `materialization` parameter to `@source`:**
   ```python
   def source(
       name: str,
       source: str = "csv",
       table: str = "",
       columns: Optional[dict] = None,
       tags: Optional[list[str]] = None,
       # New grouped parameter:
       materialization: Optional[Union[dict, MaterializationConfig]] = None,
       **params,
   ):
   ```

4. **Convert and store materialization config in node metadata:**
   ```python
   # In decorator wrapper
   # Normalize materialization to dict
   mat_dict = None
   if materialization is not None:
       if isinstance(materialization, dict):
           mat_dict = materialization
       elif isinstance(materialization, MaterializationConfig):
           mat_dict = materialization.to_dict()
       else:
           raise TypeError(
               f"materialization must be dict or MaterializationConfig, "
               f"got {type(materialization)}"
           )

   wrapper._seeknal_node = {
       # ... existing fields ...
       "materialization": mat_dict,
   }
   ```

### Phase 2: Implement Configuration Merging

**File:** `src/seeknal/workflow/dag.py`

**Location:** `DAGBuilder._parse_python_file()` (lines 367-467)

**Changes:**

Implement deep merge function for materialization config:

```python
def _merge_materialization_config(
    decorator_config: Optional[dict],
    yaml_config: Optional[dict],
    profile_config: Optional[dict] = None,
) -> dict:
    """
    Merge materialization configuration with priority:
    decorator > yaml > profile > defaults
    """
    # Start with profile (or defaults)
    result = {
        "enabled": False,  # Default: opt-in
        "mode": "append",
        "create_table": True,
    }

    # Merge profile config
    if profile_config:
        result.update(profile_config)

    # Merge YAML config
    if yaml_config:
        result.update(yaml_config)

    # Merge decorator config (highest priority)
    if decorator_config:
        # Only override non-None values
        for key, value in decorator_config.items():
            if value is not None:
                result[key] = value

    return result
```

**Apply merging in node parsing:**

```python
# For each Python node
decorator_mat = node_meta.get("materialization", {})
yaml_mat = node_meta.get("materialization_from_yaml", {})  # From YAML file if exists
profile_mat = self.profile.get("materialization", {})  # From profile

merged_mat = _merge_materialization_config(decorator_mat, yaml_mat, profile_mat)
yaml_data["materialization"] = merged_mat
```

### Phase 3: Update Executor Materialization Logic

**File:** `src/seeknal/workflow/executors/transform_executor.py`

**Location:** `TransformExecutor.post_execute()` (lines 307-365)

**Changes:**

Ensure merged config is used during materialization:

```python
def post_execute(self, result: ExecutorResult) -> ExecutorResult:
    # ... existing code ...

    # Handle Iceberg materialization if enabled
    if result.status == ExecutionStatus.SUCCESS and not result.is_dry_run:
        try:
            con = self.context.get_duckdb_connection()

            # Get materialization config (already merged by DAGBuilder)
            mat_config = self.node.config.get("materialization", {})

            # Call materialization (this already uses merged config)
            mat_result = materialize_node_if_enabled(
                self.node,
                source_con=con,
                enabled_override=getattr(self.context, 'materialize_enabled', None)
            )

            if mat_result:
                result.metadata["materialization"] = {
                    "enabled": True,
                    "success": mat_result.get("success", False),
                    "table": mat_result.get("table"),
                    "row_count": mat_result.get("row_count"),
                    "mode": mat_result.get("mode"),
                    "iceberg_table": mat_result.get("iceberg_table"),
                }
        except Exception as e:
            logger.warning(f"Failed to materialize node '{self.node.id}' to Iceberg: {e}")
            result.metadata["materialization"] = {
                "enabled": True,
                "success": False,
                "error": str(e),
            }

    return result
```

### Phase 4: Validation

**File:** `src/seeknal/pipeline/decorators.py` (add validation function)

**Security Validation:**

```python
import re

def _validate_materialization_config(
    materialization: Optional[Union[dict, MaterializationConfig]]
) -> None:
    """
    Validate materialization configuration parameters.

    Args:
        materialization: Dict or MaterializationConfig to validate

    Raises:
        ValueError: If configuration is invalid
        TypeError: If materialization is not dict or MaterializationConfig
    """
    if materialization is None:
        return

    # Convert to dict for validation
    if isinstance(materialization, MaterializationConfig):
        config = materialization.to_dict()
    elif isinstance(materialization, dict):
        config = materialization
    else:
        raise TypeError(
            f"materialization must be dict or MaterializationConfig, "
            f"got {type(materialization).__name__}"
        )

    enabled = config.get("enabled")
    table = config.get("table")
    mode = config.get("mode")

    # If enabled, table is required
    if enabled is True and not table:
        raise ValueError(
            "materialization.table is required when materialization.enabled=True"
        )

    # Validate table name format (catalog.namespace.table)
    if table:
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){2}$'
        if not re.match(pattern, table):
            raise ValueError(
                f"Invalid table name '{table}'. "
                "Expected format: catalog.namespace.table "
                "(e.g., 'warehouse.prod.sales_forecast')"
            )

    # Validate mode
    if mode and mode not in ("append", "overwrite"):
        raise ValueError(
            f"Invalid materialization.mode '{mode}'. "
            "Must be 'append' or 'overwrite'"
        )
```

**Call validation in decorators:**

```python
# In @transform wrapper
_validate_materialization_config(materialization)
```

---

## Acceptance Criteria

### Functional Requirements

- [x] `@transform` accepts `materialization` parameter (dict or MaterializationConfig)
- [x] `@source` accepts `materialization` parameter
- [x] `@feature_group` materialization works consistently with other decorators
- [x] `MaterializationConfig` dataclass provides type-safe configuration
- [x] Configuration merges correctly: Decorator > YAML > Profile > Defaults
- [x] YAML file can override decorator settings (for environment-specific configs)
- [ ] Materialization executes when configured via decorator (executor verified in plan)
- [ ] Data is written to Iceberg table (Lakekeeper/MinIO)

### Validation Requirements

- [x] Table name format validated (catalog.namespace.table)
- [x] Mode validated (append or overwrite only)
- [x] Error raised if enabled=True but table is None
- [x] Clear error messages for invalid configurations

### Backward Compatibility

- [x] Existing Python transforms without materialization parameters continue to work
- [x] Existing YAML configuration continues to work
- [x] Existing `@feature_group` materialization continues to work
- [x] No breaking changes to decorator signatures

### Testing Requirements

- [x] Unit tests for `MaterializationConfig` dataclass
- [x] Unit tests for decorator dict/dataclass handling
- [x] Unit tests for configuration merging logic
- [x] Unit tests for validation function (dict and dataclass)
- [x] Integration test: Python transform with MaterializationConfig
- [x] Integration test: Decorator + YAML override
- [x] Integration test: Source with decorator materialization
- [x] E2E test: Full pipeline with mixed decorator/YAML config

---

## Success Metrics

- **Developer Experience:** Simple transforms don't require YAML files
- **Consistency:** All decorators support materialization equally
- **Flexibility:** YAML can still override for environment-specific needs
- **Adoption:** Developers can use inline config for 80% of use cases

---

## Dependencies & Risks

### Dependencies

- **Existing Code:**
  - `src/seeknal/pipeline/materialization.py` - Materialization dataclasses
  - `src/seeknal/workflow/materialization/yaml_integration.py` - Iceberg integration
  - `src/seeknal/workflow/materialization/config.py` - Config models

### Risks

| Risk | Mitigation |
|------|------------|
| **Config merging bugs** | Comprehensive unit tests for merge logic |
| **Breaking changes** | Make all new parameters Optional with None defaults |
| **YAML override confusion** | Clear documentation of priority hierarchy |
| **Security (table injection)** | Validate table names with allowlist pattern |
| **Performance impact** | Minimal - only adds config merging during DAG building |

---

## Examples

### Example 1: Simple Transform with Inline Materialization

**Before (requires YAML):**

```python
# seeknal/pipelines/sales_forecast.py
@transform(name="sales_forecast")
def sales_forecast(ctx):
    return df
```

```yaml
# seeknal/transforms/sales_forecast.yml (separate file)
name: sales_forecast
kind: transform
file: seeknal/pipelines/sales_forecast.py
materialization:
  enabled: true
  table: "warehouse.prod.sales_forecast"
  mode: overwrite
```

**After (inline with dataclass):**

```python
# seeknal/pipelines/sales_forecast.py
from seeknal.pipeline.materialization import MaterializationConfig

@transform(
    name="sales_forecast",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.prod.sales_forecast",
        mode="overwrite",
    ),
)
def sales_forecast(ctx):
    return df
```

### Example 2: YAML Override for Development

```python
from seeknal.pipeline.materialization import MaterializationConfig

# Decorator enables materialization
@transform(
    name="sales_forecast",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.dev.sales_forecast",
        mode="overwrite",
    ),
)
def sales_forecast(ctx):
    return df
```

```yaml
# seeknal/transforms/sales_forecast.yml
# Development config: disable materialization
materialization:
  enabled: false  # YAML overrides decorator!
```

**Result:** Development environment skips materialization (faster), production uses decorator config.

### Example 3: Profile Defaults with Decorator Override

**Profile (~/.seeknal/profiles.yml):**

```yaml
materialization:
  enabled: false  # Opt-in by default
  mode: append
```

**Python Transform:**

```python
from seeknal.pipeline.materialization import MaterializationConfig

@transform(
    name="critical_metrics",
    materialization=MaterializationConfig(
        enabled=True,  # Override profile
        table="warehouse.prod.critical_metrics",
    ),
)
def critical_metrics(ctx):
    return df
```

**Result:** Only this transform materializes, others use profile defaults.

---

## Implementation Phases

### Phase 1: Decorator Extensions (1-2 hours)

- [x] Create `MaterializationConfig` dataclass in new file
- [x] Add `materialization` parameter to `@transform`
- [x] Add `materialization` parameter to `@source`
- [x] Update `@feature_group` for consistency
- [x] Add validation function for grouped config
- [x] Store normalized dict in `_seeknal_node`

### Phase 2: Configuration Merging (1-2 hours)

- [x] Implement `_merge_materialization_config()` function
- [x] Update `DAGBuilder._parse_python_file()` to use merge
- [x] Support YAML override loading
- [x] Add profile config integration

### Phase 3: Executor Integration (1 hour)

- [x] Verify `TransformExecutor.post_execute()` uses merged config
- [x] Add `SourceExecutor` materialization support (if needed)
- [x] Test materialization execution

### Phase 4: Testing (2-3 hours)

- [x] Unit tests for decorators
- [x] Unit tests for merge logic
- [x] Unit tests for validation
- [x] Integration tests for full flow
- [x] E2E test with Lakekeeper/MinIO

### Phase 5: Documentation (1 hour)

- [ ] Update Python pipeline tutorial
- [ ] Add decorator examples to docs
- [ ] Document configuration priority
- [ ] Update materialization guide

---

## References & Research

### Internal References

- **Brainstorm:** `docs/brainstorms/2026-01-26-iceberg-materialization-brainstorm.md`
- **Decorators:** `src/seeknal/pipeline/decorators.py:15-218`
- **DAG Integration:** `src/seeknal/workflow/dag.py:367-467`
- **Executor:** `src/seeknal/workflow/executors/transform_executor.py:307-365`
- **Config Models:** `src/seeknal/workflow/materialization/config.py`

### Institutional Learnings

- **Security Patterns:** `docs/solutions/security-issues/sql-injection-path-traversal-repl-fixes.md`
  - Validate table names (allowlist pattern)
  - Resolve symlinks before validation
  - Layered security validation

- **Materialization Config:** `docs/solutions/workflow-executors.md`
  - Configuration hierarchy: Node-level > Profile-level > Defaults
  - Use dataclasses for configuration
  - Support environment variables for credentials

- **Iceberg Integration:** `docs/iceberg-materialization.md`
  - Schema evolution modes (safe, auto, strict)
  - Write modes: append vs overwrite
  - Partitioning strategy

### External References

- **Python Decorators:** https://docs.python.org/3/reference/datamodel.html#implementing-descriptors
- **Dataclass Merge Patterns:** https://docs.python.org/3/library/dataclasses.html
- **DuckDB Iceberg:** https://duckdb.org/docs/extensions/iceberg
- **Lakekeeper REST Catalog:** https://lakekeeper.dev/docs/rest-catalog

---

## Open Questions

| Question | Options | Decision |
|----------|---------|----------|
| **Flat parameters or grouped `materialization`?** | Flat (`materialization_enabled`, `materialization_table`), Grouped (`materialization={...}` or `MaterializationConfig(...)`) | **Grouped with dataclass** (cleaner API, type-safe, extensible) |
| **Should we support partition_by in decorators?** | Yes (later), No (scope creep) | Deferred to v2 |
| **Should we support schema_evolution in decorators?** | Yes (later), No (scope creep) | Deferred to v2 |
| **Should decorator always win, or only when explicitly set?** | Always win, Only when not None | Only when not None (safer) |

**Decisions:**
- Use **grouped `materialization` parameter** with `MaterializationConfig` dataclass (recommended) or dict (for quick scripting)
- Decorator config values that are `None` don't override YAML/profile. Only explicitly set values override.

---

## Non-Goals (v1)

- ❌ Flat `materialization_enabled`, `materialization_table` parameters (use grouped approach)
- ❌ Partition configuration in decorators (use YAML/profile for v1)
- ❌ Schema evolution configuration in decorators (use YAML/profile for v1)
- ❌ Custom write properties in decorators (use YAML/profile for v1)
- ❌ Multiple catalog support (single catalog initially)
- ❌ Changing `@feature_group` materialization parameter format (keep consistent)

**Note:** The grouped `materialization` parameter approach makes it easy to add partition_by, schema_evolution, and other options in v2 as new fields in `MaterializationConfig`.
