---
category: python-pipeline
component: ExecutorRegistry, PythonExecutor, SourceExecutor
tags: [executor, python-pipeline, iceberg, source-loading]
date_resolved: 2026-02-20
related_build: named-refs-common-config
---

# Python Source Nodes Must Use SourceExecutor, Not PythonExecutor

## Problem Symptom

Running a Python pipeline with `@source(source="iceberg", ...)` fails with `FAILED: None`. Source nodes silently fail because PythonExecutor generates a runner script that calls the decorated function, but `@source` functions just `pass` — they don't load data.

## Investigation Steps

1. `seeknal run --full` shows source nodes as `FAILED: None` with no error detail
2. Manually running `python target/_runner_cell_sites.py` shows the function executes but does nothing
3. The root issue: `get_executor()` in `__init__.py` routes ALL Python-file nodes to PythonExecutor
4. PythonExecutor can't load iceberg data — only SourceExecutor has `_load_iceberg()`

## Root Cause

Two bugs in the executor/DAG chain:

**Bug 1 — Executor routing (`__init__.py`):**
```python
# BEFORE (broken): All Python nodes go to PythonExecutor
if node.file_path and str(node.file_path).endswith('.py'):
    return PythonExecutor(node, context)

# AFTER (fixed): Source nodes always use SourceExecutor
if hasattr(node, 'file_path') and node.file_path and str(node.file_path).endswith('.py'):
    from seeknal.dag.manifest import NodeType as NT
    if node.node_type != NT.SOURCE:
        return PythonExecutor(node, context)
```

**Bug 2 — DAG builder params (`dag.py`):**
```python
# BEFORE (broken): params not copied for Python source nodes
if kind_str == "source":
    yaml_data["source"] = node_meta.get("source", "unknown")
    yaml_data["table"] = node_meta.get("table", "")
    yaml_data["columns"] = node_meta.get("columns", {})
    # params missing! catalog_uri and warehouse lost

# AFTER (fixed): params copied
    if "params" in node_meta:
        yaml_data["params"] = node_meta["params"]
```

## Working Solution

1. In `get_executor()`: Check `node.node_type != NodeType.SOURCE` before routing to PythonExecutor
2. In DAG builder: Copy `params` from Python source decorator metadata to `yaml_data`

This ensures:
- Source nodes use SourceExecutor regardless of file type (YAML or Python)
- SourceExecutor receives `catalog_uri` and `warehouse` needed for `_load_iceberg()`
- Only transform/feature_group/aggregation Python nodes use PythonExecutor

## Prevention Strategies

1. When adding new executor routing logic, always consider all node types
2. When adding metadata fields to decorators, verify they flow through the DAG builder
3. Test Python pipelines with iceberg sources specifically (not just CSV/parquet)

## Cross-References

- `src/seeknal/workflow/executors/__init__.py:91-97` — Executor routing fix
- `src/seeknal/workflow/dag.py:566-568` — Params propagation fix
- `src/seeknal/workflow/executors/source_executor.py:810-930` — `_load_iceberg()` method
- `src/seeknal/pipeline/decorators.py:140-152` — Source decorator metadata
