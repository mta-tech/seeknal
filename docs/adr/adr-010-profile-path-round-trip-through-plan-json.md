---
adr_id: ADR-010
date: 2026-02-21
status: accepted
title: profile_path round-trip through plan.json for env plan / env apply decoupling
---

# ADR-010: profile_path round-trip through plan.json for env plan / env apply decoupling

## Context

`seeknal env plan` and `seeknal env apply` are separate commands that may run at different times and in different shell sessions. The `profile_path` chosen during planning must be available at apply time without requiring the user to re-specify it. Without persistence, users would need to remember and re-pass `--profile profiles-dev.yml` on every `apply` invocation — an error-prone workflow that defeats the purpose of the plan/apply split.

## Decision

`EnvironmentPlan` dataclass carries an optional `profile_path` field (stored as a string in JSON, re-converted to `Path` on restore). `plan()` serializes it into `plan.json` via `dataclasses.asdict()`. `apply()` reads it back with `plan_data.get('profile_path')` and returns it in the result dict. `_run_in_environment` then picks it up if no explicit `--profile` flag was provided at apply time.

**Rationale:**
- Plan-time decisions are durable: the user specifies the profile once at `env plan` and it persists automatically to `env apply`
- `plan.json` is the natural location — it already captures all other plan-time decisions (manifest snapshot, node list)
- The value is stored as a plain string in JSON, making it human-readable and auditable
- Explicit `--profile` on `apply` still overrides the stored value, preserving full user control

**Consequences:**
- Users run `seeknal env plan dev --profile profiles-dev.yml` once; subsequent `seeknal env apply dev` uses the stored profile without re-specification
- Explicit `--profile` on apply overrides the stored path for CI/CD or one-off credential substitution
- `plan.json` is human-readable — `profile_path` is visible and auditable by operators
- `None` is stored as JSON `null` when no profile was specified, so `plan.json` is always valid JSON

## Alternatives Considered

- Store `profile_path` in `env_config.json`: rejected, `plan.json` is the natural place as it captures all plan-time decisions; `env_config.json` covers TTL metadata
- Require `--profile` on every `apply` invocation: rejected, poor UX and error-prone when applying the same environment multiple times

## Related

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Tasks: task-5-env, task-10-env
