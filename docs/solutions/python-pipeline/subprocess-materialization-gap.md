---
category: python-pipeline
component: PythonExecutor
tags: [subprocess, materialization, duckdb, parquet]
date_resolved: 2026-02-20
related_build: lineage-inspect-postgresql-materialization
related_tasks: [C-5]
---

# PythonExecutor Subprocess Materialization Gap

## Problem Symptom

Python transforms using `@materialize` decorator showed materialization metadata with only a `stdout` key — no materialization results. PostgreSQL tables were not created despite correct decorator configuration.

## Investigation Steps

1. Verified `@materialize` decorator stored config correctly in `func._seeknal_materializations`
2. Confirmed DAGBuilder propagated `materializations` list to `node.config`
3. Discovered PythonExecutor runs via `uv run` subprocess — DuckDB connection and views exist only in subprocess
4. Parent process `post_execute()` had no materialization logic (unlike TransformExecutor)

## Root Cause

PythonExecutor runs Python transforms in a separate subprocess via `uv run`. The subprocess creates DuckDB views from input sources, executes the transform, and writes intermediate parquet. But when the subprocess exits, all DuckDB views are lost. The parent process `post_execute()` method (which handles materialization) has no access to the subprocess's DuckDB state.

## Working Solution

Added `post_execute()` method to PythonExecutor that:
1. Reads intermediate parquet file written by subprocess
2. Creates a DuckDB view in the parent process from that parquet
3. Dispatches materialization through the standard MaterializationDispatcher

```python
def post_execute(self, result: ExecutorResult) -> ExecutorResult:
    mat_targets = self.node.config.get("materializations", [])
    if not mat_targets:
        return result

    intermediate_path = self.context.target_path / "intermediate" / f"{self.node.id.replace('.', '_')}.parquet"
    con = self.context.get_duckdb_connection()
    view_name = f"transform.{self.node.name}"
    con.execute("CREATE SCHEMA IF NOT EXISTS transform")
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{intermediate_path}')")

    dispatcher = MaterializationDispatcher()
    dispatch_result = dispatcher.dispatch(con=con, view_name=view_name, targets=mat_targets, node_id=self.node.id)
```

**Result:** Python transforms now materialize to PostgreSQL correctly. All 5 PostgreSQL tables created in medallion E2E test.

## Prevention Strategies

1. When adding materialization to new executor types, always verify the DuckDB view lifecycle
2. Subprocess executors need an explicit bridge pattern (intermediate file → parent view → dispatch)
3. Test materialization for each executor type independently

## Cross-References

- Related: docs/solutions/python-pipeline/executor-routing-for-python-sources.md
- Related: docs/solutions/python-pipeline/duckdb-variable-shadowing.md
- `src/seeknal/workflow/executors/python_executor.py` — PythonExecutor.post_execute()
- `src/seeknal/workflow/materialization/dispatcher.py` — MaterializationDispatcher
