---
date: 2026-01-23
topic: sql-repl
---

# SQL REPL for Seeknal

## What We're Building

An interactive SQL REPL (`seeknal repl`) for exploring data across any registered source in seeknal. The REPL provides a DuckDB-style interface with simple dot-commands for navigation and tabular query results.

**Target users:** Data engineers and ML practitioners exploring feature stores, debugging transformations, and validating data pipelines interactively.

## Why This Approach

We evaluated three architectures:

1. **Integrated CLI Command** (chosen) — Adds `seeknal repl` as a Typer command, keeping the REPL logic in `cli/repl.py`. Simple, cohesive, builds on existing patterns.

2. **Standalone Module** — Separate `seeknal.repl` package. More separation but overkill for initial scope.

3. **Plugin Architecture** — Optional plugin like Atlas. Maximum modularity but adds friction.

**Decision:** Integrated CLI wins because the REPL is a core exploration tool, not an optional extension. It should "just work" after installing seeknal.

## Key Decisions

### Credential Handling: Fernet Encryption
- **Rationale:** Following KAI's proven pattern. Store an `ENCRYPT_KEY` in config.toml, encrypt sensitive connection params (passwords, tokens) before storing in SourceTable.params.
- **Implementation:** Add `seeknal.utils.encryption` module with `encrypt_value()` and `decrypt_value()` functions using `cryptography.fernet`.

### Query Engines: Hybrid Approach
- **SQLAlchemy** for database sources (Postgres, MySQL, SQLite, etc.)
- **DuckDB** for file-based sources (Parquet, CSV)
- **Rationale:** Plays to each tool's strengths. SQLAlchemy provides native driver performance and full SQL dialect support. DuckDB excels at file-based analytics.

### REPL Style: DuckDB-Inspired
- Fast startup, minimal UI
- Dot-commands: `.sources`, `.connect <name>`, `.tables`, `.schema <table>`, `.quit`
- Tabular output using `tabulate` or `rich`
- History and readline support

### Data Sources: Any Registered Source
- Query any source registered via `seeknal source add` (future command) or existing SourceRequest API
- Sources are workspace-scoped, respecting multi-tenant isolation

## User Experience

```bash
# Start REPL
$ seeknal repl

seeknal> .sources
NAME          TYPE       DESCRIPTION
────────────────────────────────────────
analytics_db  postgres   Production analytics
features      parquet    Feature store offline
raw_events    csv        Raw event logs

seeknal> .connect analytics_db
Connected to analytics_db (postgres)

analytics_db> .tables
users
orders
products

analytics_db> SELECT COUNT(*) FROM users;
┌──────────┐
│ count(*) │
├──────────┤
│   125432 │
└──────────┘

analytics_db> .connect features
Connected to features (parquet @ ~/.seeknal/feature_store/)

features> SELECT * FROM user_features LIMIT 3;
┌─────────┬───────┬────────────┐
│ user_id │ score │ segment    │
├─────────┼───────┼────────────┤
│ 1       │ 0.85  │ high_value │
│ 2       │ 0.42  │ medium     │
│ 3       │ 0.91  │ high_value │
└─────────┴───────┴────────────┘

features> .quit
Goodbye!
```

## Technical Components

### New Files
- `src/seeknal/cli/repl.py` — REPL command and session logic
- `src/seeknal/utils/encryption.py` — Fernet encryption utilities

### Modified Files
- `src/seeknal/cli/main.py` — Register `repl` command
- `src/seeknal/request.py` — Encrypt/decrypt source params on save/load
- `pyproject.toml` — Add `cryptography` dependency

### Source Registration Enhancement
Need to add CLI commands for source management:
- `seeknal source add <name> --type postgres --connection-string "..."`
- `seeknal source list`
- `seeknal source remove <name>`

(This may be a prerequisite or parallel workstream)

## Open Questions

1. **Should feature groups be auto-registered as sources?** Users might expect `seeknal repl` to immediately query feature groups without explicit source registration.

2. **Multi-line SQL support?** DuckDB REPL handles this with semicolon detection. Do we need it for v1?

3. **Output formats?** Start with tabular only, or support `.format json`, `.format csv` from day one?

## Next Steps

Run `/workflows:plan` to generate implementation plan with file changes and testing strategy.

## References

- KAI credential handling: `~/project/mta/KAI/app/server/config.py` (Fernet + pydantic-settings)
- Seeknal source management: `src/seeknal/request.py` (SourceRequest class)
- DuckDB CLI for UX inspiration: https://duckdb.org/docs/api/cli
