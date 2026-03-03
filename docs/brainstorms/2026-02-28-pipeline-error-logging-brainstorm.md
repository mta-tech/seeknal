---
title: "Pipeline Error Logging & Verbose Mode"
type: feat
status: brainstormed
date: 2026-02-28
---

# Pipeline Error Logging & Verbose Mode

## The Problem

When a pipeline node fails, the user sees:

```
17/31: churn_model [RUNNING]
  FAILED: None
```

`FAILED: None` tells you nothing. The root cause is buried — the `error_message` field on `ExecutorResult` is sometimes `None` because not all code paths set it. Even when it IS set, only `str(e)` is captured — no stack trace, no subprocess output, no exception type.

Users have no way to diagnose failures without manually adding print statements or wrapping code in try/except.

## What We're Building

Two complementary improvements:

### 1. Per-Run Log File (`target/logs/`)

Every `seeknal run` creates a timestamped log file at `target/logs/run_YYYYMMDD_HHMMSS.log`.

**Default behavior (no flags):**
- Log file only written when at least one node fails
- Contains: full traceback, exception chain, subprocess stdout/stderr (for Python nodes)
- Terminal shows: short error summary + path to log file

**With `--verbose`:**
- Log file always written (all nodes: success, cached, failed)
- Full audit trail of the entire run

**Log file format:**
```
=== Seeknal Run Log ===
Timestamp: 2026-02-28T12:00:00
Project: my-project
Mode: Incremental
Run ID: 20260228_120000

--- Node: transform.churn_model ---
Status: FAILED
Duration: 34.20s
Error: ModuleNotFoundError: No module named 'sklearn'

Traceback:
  File "/path/to/churn_model.py", line 12, in churn_model
    from sklearn.ensemble import RandomForestClassifier
ModuleNotFoundError: No module named 'sklearn'

Subprocess stderr:
  error: Failed to resolve `scikit-learn`
  ...full uv output...

--- Node: source.products ---
Status: CACHED
Duration: 0.01s
```

### 2. `--verbose` Flag on `seeknal run`

```bash
seeknal run --verbose
```

- Shows full tracebacks inline in the terminal (not just summary)
- Writes full log file (all nodes, not just failures)
- Configures Python logging to `DEBUG` level

**Default mode (no `--verbose`):**
```
17/31: churn_model [RUNNING]
  FAILED: ModuleNotFoundError: No module named 'sklearn'
  → Full details: target/logs/run_20260228_120000.log
```

**Verbose mode:**
```
17/31: churn_model [RUNNING]
  FAILED: ModuleNotFoundError: No module named 'sklearn'
  Traceback (most recent call last):
    File "/path/to/churn_model.py", line 12, in churn_model
      from sklearn.ensemble import RandomForestClassifier
  ModuleNotFoundError: No module named 'sklearn'

  Subprocess stderr (34 lines):
    error: Failed to resolve `scikit-learn`
    ...
```

## Why This Approach

**Log file per-project (`target/logs/`)** because:
- Easy to find — same place as other artifacts (`target/cache/`, `target/intermediate/`)
- Gitignore-able (target/ is already ignored)
- One log per run — easy to compare failed vs successful runs
- No global log management complexity

**Error summary + log path** (not full trace by default) because:
- Keeps terminal output clean and scannable
- Most errors are diagnosable from the one-liner (`ModuleNotFoundError`, `ConnectionRefusedError`)
- Full details always available one click away
- `--verbose` available when you need inline detail

**Capture everything from subprocesses** because:
- Python nodes run via `uv run` subprocess — stderr contains dependency resolution errors, pip install failures, Python tracebacks
- ML training output (print statements, progress bars) can contain useful context
- Log files are cheap; lost debugging context is expensive

## Key Decisions

1. **Log location**: `target/logs/run_{run_id}.log` — per-project, per-run
2. **Default behavior**: Log file only on failure; `--verbose` for full audit trail
3. **Error display**: One-line summary + log path in default mode; full traceback with `--verbose`
4. **Subprocess capture**: Full stdout + stderr, no truncation
5. **Always set error_message**: Fix all executor paths to NEVER return `None` error_message on failure
6. **Log rotation**: Not needed initially — users can `rm target/logs/*` to clean up. Consider adding `--keep-logs N` later if needed

## Scope Boundaries

**In scope:**
- `--verbose` flag on `seeknal run`
- Per-run log file in `target/logs/`
- Fix `FAILED: None` — all executor paths must set `error_message`
- Capture full traceback + subprocess output in log and state
- Show log file path on failure

**Out of scope (YAGNI):**
- `--log-level` granular control (--verbose is enough for now)
- Log rotation / retention policies
- Remote log shipping / structured logging (JSON format)
- `--verbose` on other commands (plan, lineage) — not needed yet
- `seeknal show-error <node>` command — log file is sufficient
- Global `~/.seeknal/logs/` — per-project is enough

## Open Questions

*None — all resolved during brainstorming.*

## Resolved Questions

1. **What to show on failure?** → Error summary + log file path (not full trace by default)
2. **Where to log?** → `target/logs/` per project
3. **Add --verbose?** → Yes, shows full trace inline + writes full log
4. **Subprocess output?** → Capture everything (stdout + stderr), no truncation
5. **Log scope?** → Default: failures only. `--verbose`: all nodes
