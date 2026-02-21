---
adr_id: ADR-008
date: 2026-02-21
status: accepted
title: REPL three-phase best-effort auto-registration
---

# ADR-008: REPL three-phase best-effort auto-registration

## Context

Developers running `seeknal repl` inside a project directory had to manually issue `.connect` commands to inspect pipeline outputs. Three distinct data sources need automatic registration at startup: intermediate parquet files from the pipeline cache, PostgreSQL connections defined in profiles, and Iceberg catalogs. Each source has different failure modes: missing parquet files (first run), PostgreSQL network timeouts, or missing OAuth2 config for Iceberg. A naive single-block registration would let one failure silence the remaining sources.

## Decision

Implement REPL project auto-registration as three independent phases, each wrapped in its own `try/except` block. A failure in any phase emits a warning and allows the next phase to proceed. Registration counts are tracked as instance attributes for display in the startup banner.

**Rationale:**
- Independent `try/except` per phase ensures maximum source availability at startup — a timed-out PostgreSQL connection does not prevent parquet files from being queryable
- Failures surface as warnings, not exceptions, preserving the interactive startup experience
- Zero manual `.connect` steps required when entering a project directory
- REPL outside a project directory is completely unaffected (no regression)

**Consequences:**
- REPL startup may take a few extra seconds when PostgreSQL connections time out (mitigated by a hard-coded `connect_timeout=5` in the libpq string)
- Failed registrations are visible as warnings — developers see which sources are available versus which failed
- Registration counts are shown in the startup banner for immediate feedback
- Three separate code paths to maintain versus one combined path

## Alternatives Considered

- Single `try/except` around all three phases: rejected because one failure would silence the remaining two phases entirely
- Interactive prompts at startup asking what to register: rejected, reduces the automation value of auto-registration
- Lazy registration on first `.tables` call: rejected, the startup banner must show a registration summary immediately

## Related

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Tasks: task-2-repl, task-3-repl, task-9-repl
- Related ADRs: ADR-007
