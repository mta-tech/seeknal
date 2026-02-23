---
adr_id: ADR-006
date: 2026-02-20
status: accepted
title: Multi-Target Materialization Dispatcher with Best-Effort Execution
---

# ADR-006: Multi-Target Materialization Dispatcher with Best-Effort Execution

## Context

Nodes need to write to multiple targets (e.g., PostgreSQL for serving and Iceberg for the lakehouse) from a single transform step. A routing mechanism was needed to direct output to the correct backend, along with a clearly defined failure policy for when one target fails while others succeed.

## Decision

Created `MaterializationDispatcher` that routes targets to backend-specific helpers based on the `type:` field in each target's configuration. The dispatcher uses a best-effort execution policy: all targets are attempted, failures are logged per target, and succeeded targets are not rolled back if a later target fails.

**Rationale:**
- Best-effort prevents one slow or failing target from blocking writes to other independent targets
- Per-target result objects enable granular error reporting and observability
- The dispatcher pattern allows new storage backends to be added without modifying executor code

**Consequences:**
- [ ] Extensible — new backends are added by registering a handler in the dispatcher
- [ ] Resilient — a single target failure does not abort the entire materialization step
- [ ] Per-target results provide clear visibility into which targets succeeded or failed
- [ ] Partial materialization is possible: some targets may have newer data than others after a partial failure
- [ ] No cross-system transaction guarantee — consistency across targets must be managed at the pipeline level if required

## Alternatives Considered

- All-or-nothing: Requires a distributed transaction across heterogeneous systems (PostgreSQL + Iceberg), which is impractical and adds significant complexity with no clear coordination protocol
- Sequential with rollback: Iceberg snapshot rollback is non-trivial and PostgreSQL rollback after COPY completion requires explicit cleanup logic, making this approach fragile and hard to maintain

## Related

- Tasks: B-8
- File: src/seeknal/workflow/materialization/dispatcher.py
