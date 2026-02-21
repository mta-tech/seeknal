# Planning Patterns

Reusable patterns captured from past planning sessions. Referenced by `/plan_w_team` during plan creation.

## Team Composition

### Parallel Builders Partitioned by File Ownership
Split the team into two builders when two features touch largely disjoint files, converging only at a single shared integration point (e.g., the CLI entry file). Assign each builder a named section (Section A / Section B) with explicit task numbers, so dependency chains within each section stay sequential while the sections run in parallel.
- **When to apply:** Two features are being built simultaneously, they own distinct modules, and they share only one or two integration files at the end
- **Source:** specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md, 2026-02-21

## Testing Strategy

### Unit Tests Per Feature Plus Shared QA E2E Against Live Infrastructure
Write unit tests scoped to each feature (mocking filesystem, connections, and APIs) and a single shared QA E2E spec that exercises both features together against real infrastructure (PostgreSQL, Iceberg/Lakekeeper). This gives fast local feedback from unit tests and end-to-end confidence from the QA spec without duplicating coverage.
- **When to apply:** A build combines two or more features that both interact with the same external systems (databases, object storage, REST APIs) and a live infrastructure environment is available for QA
- **Source:** specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md, 2026-02-21

## Architecture Patterns

### Backward Compatibility as a Hard Requirement When Refactoring Shared Infrastructure
When adding a new parameter or context object to a shared, widely-called component (e.g., DAGRunner, a base executor), make the new parameter optional with a fallback to existing behavior. State this explicitly in the task description ("callers without context must still work") and add a dedicated test for the no-context path.
- **When to apply:** A shared infrastructure class used by many callers needs a new capability, and you cannot update all call sites in the same PR
- **Source:** specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md, 2026-02-21

## Workflow Preferences

### Brainstorm Before Plan
Always run `/workflows:brainstorm` before `/workflows:plan` to clarify WHAT to build through collaborative dialogue before planning HOW to implement it. Brainstorming surfaces key decisions (approach, scope, trade-offs) that prevent rework during planning.
- **When to apply:** Any new feature or significant improvement — especially when multiple approaches exist or requirements are ambiguous
- **Source:** User preference, captured 2026-02-21

### Merge Related Plans When They Share a Common Dependency
When two feature plans in flight share a concrete code dependency (same file, same data structure, same propagation gap to fix), merge them into a single merged spec before starting the build. The merged plan lists both feature sections, assigns them to separate builders, and fixes the shared dependency once in whichever section owns the affected file.
- **When to apply:** Two features are planned close together, both require the same underlying fix or plumbing change, and building them separately would cause merge conflicts or duplicate fixes
- **Source:** specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md, 2026-02-21

### Best-Effort Registration for Non-Critical Side Effects
When a feature registers or attaches optional resources at startup (e.g., auto-connecting REPL to PostgreSQL and Iceberg), wrap each registration phase in an independent try/except and emit a warning on failure rather than aborting. Track registration counts for the startup banner so users see partial success rather than a silent no-op.
- **When to apply:** A feature performs multiple independent setup actions at startup where partial success is better than no startup, and failures are informational rather than fatal
- **Source:** specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md, 2026-02-21

### Convention-Based Auto-Discovery as the Zero-Config Fallback
When a flag (e.g., `--profile`) has an obvious per-environment convention (e.g., `profiles-dev.yml`), implement auto-discovery as a fallback when the flag is omitted. Use a clear priority chain: explicit flag > convention file in project root > convention file in home dir > global default. Log an INFO message when a convention file is used so the user knows which file was picked.
- **When to apply:** A CLI flag has a predictable naming convention per environment or context, and you want zero-config usage while still allowing explicit override
- **Source:** specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md, 2026-02-21
