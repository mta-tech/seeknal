# Brainstorm: Project-Aware REPL

**Date:** 2026-02-21
**Status:** Ready for planning

## What We're Building

Extend `seeknal repl` to auto-detect when running inside a seeknal project and automatically register all queryable data sources — common config sources, node intermediate outputs, and externally materialized tables — so developers can inspect pipeline data with plain SQL without manual `.connect` setup.

## Why This Approach

The current REPL is a generic SQL shell. Developers debugging pipelines must manually connect to each data source, remember parquet file paths, and look up connection strings. By auto-registering everything on startup, the REPL becomes a zero-config development companion: start it, query anything.

**Approach chosen:** Full auto-registration (DuckDB-native). On REPL start, detect project context and register all data as DuckDB views/attached databases. No new commands needed — everything is plain SQL.

## Key Decisions

1. **Extend existing `seeknal repl`** — no new command. Auto-detect project context by checking for `seeknal_project.yml` in the current directory.

2. **Auto-load common config sources** — parse `seeknal/common/sources.yml` at startup, resolve connections via ProfileLoader, and attach using DuckDB extensions (postgres_scanner, iceberg, etc.).

3. **Register node intermediate outputs** — scan `target/intermediate/*.parquet` and create DuckDB views named after their node IDs (e.g., `"transform.clean_orders"`).

4. **Connect to external materialized targets** — read materialization configs from YAML/Python nodes, resolve connection profiles, and attach PostgreSQL/Iceberg tables using DuckDB extensions. These are the actual production tables, not local intermediates.

5. **Profile loading** — use ProfileLoader (default `~/.seeknal/profiles.yml` or `--profile` flag) to resolve connection credentials for external targets and common sources.

## Scope

### In Scope
- Auto-detect seeknal project on REPL start
- Load and register common config sources as queryable tables
- Register intermediate parquet outputs as views by node name
- Attach externally materialized PostgreSQL tables via DuckDB postgres_scanner
- Attach externally materialized Iceberg tables via DuckDB iceberg extension
- Display summary of registered sources on startup (count of sources, nodes, external targets)
- Support `--profile` flag for custom profile path

### Out of Scope
- Modifying any pipeline execution behavior
- Writing/mutating data through the REPL (read-only, enforced by existing allowlist)
- Semantic layer query compilation (that's `seeknal query`)
- New dot-commands beyond what's needed for this feature

## Data Flow

```
seeknal repl (inside project)
  |
  ├─ Detect seeknal_project.yml
  |
  ├─ Load ProfileLoader (for credentials)
  |
  ├─ Parse seeknal/common/sources.yml
  |   └─ For each source: ATTACH or CREATE VIEW via DuckDB extension
  |
  ├─ Scan target/intermediate/*.parquet
  |   └─ CREATE VIEW "source.X" AS SELECT * FROM 'path/to/X.parquet'
  |
  ├─ Parse YAML/Python nodes for materialization configs
  |   ├─ PostgreSQL targets: ATTACH postgres database
  |   └─ Iceberg targets: ATTACH iceberg catalog
  |
  └─ Print startup summary:
       "Project: my_project
        Common sources: 3 registered
        Node outputs: 8 available
        External targets: 2 attached (postgresql: 1, iceberg: 1)"
```

## Open Questions

None — all key decisions resolved through dialogue.

## References

- Existing REPL: `src/seeknal/cli/repl.py`
- Common config: `src/seeknal/workflow/common_config.py`
- ProfileLoader: `src/seeknal/workflow/profile_loader.py`
- DAGBuilder: `src/seeknal/workflow/dag.py`
- DuckDB postgres_scanner: `INSTALL postgres; LOAD postgres; ATTACH 'postgres://...' AS db`
- DuckDB iceberg: `INSTALL iceberg; LOAD iceberg; SELECT * FROM iceberg_scan('...')`
