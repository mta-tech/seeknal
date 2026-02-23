---
adr_id: ADR-009
date: 2026-02-21
status: accepted
title: ExecutionContext optional parameter with legacy fallback in DAGRunner
---

# ADR-009: ExecutionContext optional parameter with legacy fallback in DAGRunner

## Context

DAGRunner was previously invoked without an execution context by many callers: `seeknal run`, parallel mode, and the test suite. Adding `ExecutionContext` support for the new environment-aware executor path must not break any of these existing callers. The new path is required for `seeknal env apply` and `seeknal run --env`, which need to resolve profile paths, namespace prefixes, and executor registry lookups at runtime.

## Decision

`DAGRunner.__init__` accepts `exec_context` as `Optional[ExecutionContext]` defaulting to `None`. Inside `_execute_by_type`, the method checks `if self.exec_context is not None` to route to the new `get_executor()` registry path; otherwise it falls back to the legacy `execute_*` function dispatch. The new path is purely opt-in.

**Rationale:**
- All existing callers continue working without modification — no breaking changes
- The new environment-aware path is activated only when an `ExecutionContext` is explicitly passed by the caller (`seeknal env apply`, `seeknal run --env`)
- `TYPE_CHECKING` import is used for `ExecutionContext` to avoid circular imports at runtime
- The opt-in pattern allows incremental migration of callers without a flag day

**Consequences:**
- All existing callers (`seeknal run`, parallel mode, tests) continue working without modification
- New env-aware callers pass an `ExecutionContext` and get the new executor registry path
- `TYPE_CHECKING` guard prevents circular import issues at module load time
- Both dispatch paths coexist indefinitely until the legacy path is explicitly deprecated

## Alternatives Considered

- Require `ExecutionContext` for all callers: rejected, would require simultaneous changes across too many call sites
- Separate `RunnerV2` class: rejected, would duplicate the majority of `DAGRunner` logic for marginal isolation benefit

## Related

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Tasks: task-4-env, task-10-env
