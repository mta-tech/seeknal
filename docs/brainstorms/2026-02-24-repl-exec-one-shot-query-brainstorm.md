---
topic: REPL One-Shot Query Execution (--exec flag)
date: 2026-02-24
status: decided
---

# Brainstorm: REPL One-Shot Query Execution

## What We're Building

Add `--exec` / `-e` flag to the existing `seeknal repl` command for non-interactive, one-shot SQL query execution. When provided, the REPL initializes normally (3-phase auto-registration of parquets, PostgreSQL, Iceberg), executes the SQL, outputs the result in the requested format, and exits without entering the interactive loop.

This serves two audiences equally:
- **Developers**: Quick terminal queries without entering interactive REPL
- **AI agents**: Programmatic data exploration with structured output (JSON/CSV)

### Usage Examples

```bash
# Human-friendly table output (default)
seeknal repl --exec "SELECT * FROM orders LIMIT 5"
seeknal repl -e "SELECT COUNT(*) FROM daily_revenue"

# Agent-friendly JSON output
seeknal repl -e "SELECT * FROM orders LIMIT 10" --format json

# Export to file (format inferred from extension)
seeknal repl -e "SELECT * FROM orders" --output results.csv
seeknal repl -e "SELECT * FROM orders" --output results.parquet

# Pipe SQL from stdin (for multi-line or scripted queries)
echo "SELECT * FROM orders WHERE status = 'COMPLETED'" | seeknal repl -e -
cat complex_query.sql | seeknal repl -e -

# With environment and profile
seeknal repl -e "SELECT * FROM orders" --env dev --profile profiles-dev.yml

# CSV to stdout for piping
seeknal repl -e "SELECT * FROM orders" --format csv | head -20
```

## Why This Approach

**Approach A: `--exec` flag on existing `seeknal repl`** was chosen over:

- **Separate `seeknal sql` command**: Would require duplicating or extracting REPL registration logic. More plumbing for no additional benefit.
- **Extract shared QueryEngine**: Good architecture long-term but YAGNI — the REPL class already has everything needed.

Rationale:
1. **Zero code duplication** — reuses all 3-phase auto-registration (parquets, PostgreSQL, Iceberg)
2. **Common CLI pattern** — `bash -c`, `python -c`, `psql -c`, `sqlite3 -cmd` all use this pattern
3. **Minimal implementation** — add flag, skip interactive loop, format output
4. **Same flags work** — `--env`, `--profile` already exist on the repl command

## Key Decisions

1. **Command**: `seeknal repl --exec "SQL"` / `seeknal repl -e "SQL"` — flag on existing command, not a new command
2. **Stdin support**: Accept `-` as the SQL argument to read from stdin (`cat query.sql | seeknal repl -e -`)
3. **Output formats**: table (default), json, csv, parquet — via `--format` flag
4. **File export**: `--output file.{csv|json|parquet}` infers format from extension
5. **Same registration**: Reuses REPL's 3-phase best-effort auto-registration unchanged
6. **Same security**: SQL validation (read-only allowlist) applies to `--exec` queries too
7. **Exit code**: 0 on success, 1 on SQL error — enables scripting with `&&` chains
8. **No row limit by default**: Unlike the semantic `query` command (default 100), `--exec` returns all rows unless user adds LIMIT. Add `--limit` flag as optional safety net.

## Scope

### In Scope
- `--exec` / `-e` flag on `seeknal repl` command
- `--format` flag: table (default), json, csv
- `--output` flag: export to file (csv, json, parquet) with format inferred from extension
- Stdin support (`-e -` reads from stdin)
- `--limit` flag for optional row limiting
- Exit code 0/1 for scripting

### Out of Scope
- Modifying the interactive REPL behavior
- Adding new data source types
- Query history for `--exec` mode (ephemeral by design)
- Multiple SQL statements in one `--exec` call (single query only)

## Open Questions

None — all design decisions resolved during brainstorming.
