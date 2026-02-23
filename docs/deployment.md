---
last_updated: 2026-02-21
environment: development
platform: cross-platform
---

# Deployment Guide

Documentation for deploying Seeknal.

## Quick Start

This system runs locally using Claude Code with custom commands and agents. No traditional deployment needed.

## Environment

- **Platform**: macOS/Linux/Windows (Claude Code supported)
- **Runtime**: Python 3.11+
- **Configuration**: `.claude/` directory

## Configuration

Located in `.claude/`:
- `commands/` - Slash command implementations
- `agents/` - Sub-agent definitions
- `skills/` - Reusable prompt templates
- `docs/` - Learnings and compounded knowledge

## Changelog

### 2026-02-21 - Project-Aware REPL + Environment-Aware Execution

**Environment:** development
**Platform:** macOS/Linux (cross-platform)

**Changes:**
- [Feature] REPL auto-detection of seeknal_project.yml and three-phase registration (parquets, PostgreSQL, Iceberg)
- [Feature] REPL startup banner shows project context (node count, last run, registration summary)
- [Feature] ExecutionContext integration in DAGRunner with backward compatibility
- [Feature] Per-env profile auto-discovery (profiles-{env}.yml)
- [Feature] Convention-based namespace prefixing ({env}_ prefix for PostgreSQL schemas and Iceberg namespaces)
- [Feature] profile_path persistence in plan.json for env plan -> env apply round-trip
- [Feature] Promotion with re-materialization support and --dry-run mode
- [Config] profiles-{env}.yml convention for environment-specific credentials

**New Files:**
- tests/cli/test_repl_project.py (18 tests)
- tests/workflow/test_env_aware.py (41 tests)
- tests/workflow/test_dag_source_defaults.py (10 tests)

**Configuration:**
- profiles-{env}.yml auto-discovered in project root or ~/.seeknal/
- connect_timeout=5 hardcoded for PostgreSQL REPL attachments
- plan.json now stores profile_path for env apply restoration

**Lessons Learned:**
- Best-effort three-phase registration keeps REPL startup reliable even with partial infrastructure
- Optional parameter + legacy fallback preserves backward compatibility during incremental migration
- Plan.json as durable state store eliminates re-specification of plan-time decisions at apply time
- Convention-based prefixing ({env}_schema) gives zero-config environment isolation

### 2026-02-20 - PostgreSQL Source Pushdown & Multi-Target Materialization

**Environment:** development + docker
**Platform:** macOS/Linux (cross-platform)

**Changes:**
- [Feature] PostgreSQL connection factory with profile-based config (`connections:` in profiles.yml)
- [Feature] PostgreSQL source pushdown queries (`query:` field for server-side SQL execution)
- [Feature] PostgreSQL materialization helper with 3 write modes: full, incremental_by_time, upsert_by_key
- [Feature] Multi-target materialization dispatcher (write to both PostgreSQL + Iceberg from one node)
- [Feature] `materializations:` (plural) YAML key for multi-target config
- [Feature] Stackable `@materialize` Python decorator
- [Feature] PythonExecutor post_execute materialization via intermediate parquet bridge
- [Feature] DDL/DML validation for pushdown queries (SELECT/WITH only)
- [Infrastructure] Local PostgreSQL Docker container (port 5433) for E2E testing
- [Infrastructure] Medallion E2E test project (CSV + Iceberg + PostgreSQL sources)
- [Infrastructure] Lineage & Inspect E2E test project (YAML + Python + Iceberg)
- [Backward Compat] Singular `materialization:` key continues to work (normalized to list)

**New Files:**
- `src/seeknal/connections/postgresql.py` - Connection factory
- `src/seeknal/workflow/materialization/postgresql.py` - PostgreSQL materialization helper
- `src/seeknal/workflow/materialization/pg_config.py` - Config model
- `src/seeknal/workflow/materialization/dispatcher.py` - Multi-target dispatcher

**Configuration:**
- `profiles.yml` now supports `connections:` section for named database connections
- Environment variable interpolation with defaults: `${PG_HOST:localhost}`
- PostgreSQL materialization requires `type: postgresql` in materializations config

**Lessons Learned:**
- DuckDB's postgres extension eliminates need for separate PostgreSQL drivers
- Subprocess executors (PythonExecutor) need explicit parquet bridge for materialization
- CAST is required for string-to-timestamp arithmetic in DuckDB SQL
- Best-effort multi-target execution is more practical than all-or-nothing transactions
- Lazy imports break circular dependency chains in dispatcher modules

### 2026-02-11 - Fix Integration Security and Functionality Issues

**Environment:** development
**Platform:** cross-platform (Linux, macOS, Windows)

**Changes:**
- [Security] Path traversal vulnerability fixed in FileStateBackend
- [Validation] Parameter name validation enforced
- [Validation] Date range validation (2000-2100, max 1 year future)
- [Breaking Change] FileStateBackend raises ValueError for insecure paths
- [Breaking Change] Invalid parameter names now raise ValueError
- [Code Quality] `type` parameter renamed to `param_type`
- [Code Quality] Centralized type conversion module created

**Migration Notes:**
- Set `SEEKNAL_BASE_CONFIG_PATH` to secure directory (not /tmp)
- Rename parameters with special characters to alphanumeric only

**Lessons Learned:**
- Path sanitization AND validation provides defense in depth against traversal attacks
- Warning-based parameter collision handling maintains backward compatibility
- Centralized type conversion ensures consistent behavior across codebase
- Date validation prevents unreasonable dates (year range: 2000-2100, max 1 year future)
- Parameter names must follow Python identifier conventions (alphanumeric + underscores)
