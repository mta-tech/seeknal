---
adr_id: ADR-007
date: 2026-02-20
status: accepted
title: PythonExecutor Post-Execute Parquet Bridge for Materialization
---

# ADR-007: PythonExecutor Post-Execute Parquet Bridge for Materialization

## Context

Python transforms in Seeknal run via `uv run` in a subprocess with an isolated Python environment. DuckDB views created inside that subprocess do not persist to the parent process. Materialization dispatch happens in the parent process during `post_execute()`. A mechanism was needed to make the subprocess output available to the parent process's materialization dispatcher without breaking subprocess isolation.

## Decision

`PythonExecutor.post_execute()` reads the intermediate parquet file written by the subprocess, creates a DuckDB view in the parent process from that data, then dispatches materialization through the standard `MaterializationDispatcher`. This bridges the subprocess boundary using the parquet file that the executor already writes as part of its normal execution flow.

**Rationale:**
- Maintains subprocess isolation — the `uv` environment boundary is preserved
- Reuses intermediate parquet files that already exist as part of the normal executor lifecycle
- Reuses the dispatcher infrastructure, keeping materialization logic in one place across all executor types

**Consequences:**
- [ ] Consistent materialization behavior across TransformExecutor, SourceExecutor, and PythonExecutor
- [ ] No changes required to the subprocess code to support materialization
- [ ] Extra I/O cost: the parquet file is written by the subprocess and then read back by the parent process to create the view
- [ ] Requires the intermediate parquet file to exist — if the subprocess fails before writing it, post-execute materialization is skipped

## Alternatives Considered

- In-process execution: Would lose the `uv` dependency isolation that allows Python transforms to use arbitrary package versions independent of the main Seeknal runtime
- IPC between processes: Adds complexity (sockets, pipes, or shared memory), is fragile across platforms, and provides no advantage over the existing parquet file
- Post-subprocess materialization script: Would duplicate dispatcher logic in the subprocess, creating two separate code paths to maintain for what is fundamentally the same operation

## Related

- Tasks: C-5
- File: src/seeknal/workflow/executors/python_executor.py
