# Brainstorm: Unified Developer Experience — dbt-Style + Phase 2 SQLMesh Features

> **Date:** 2026-02-09 | **Status:** Draft | **Author:** Claude Code

## Problem Statement

Seeknal currently has **two distinct developer workflows** that evolved independently:

1. **Phase 1 (dbt-style):** `seeknal init` → `seeknal draft` → `seeknal dry-run` → `seeknal apply` → `seeknal run`
2. **Phase 2 (SQLMesh-inspired):** `seeknal env plan` → `seeknal env apply` → `seeknal env promote` + `seeknal run --parallel`

Users learning Seeknal encounter a split personality — the core pipeline workflow and the environment workflow use overlapping but inconsistent terminology (e.g., `apply` means "move a draft file into the project" in Phase 1, but `env apply` means "execute a plan in an environment" in Phase 2). This creates confusion about which commands to use and when.

**Goal:** Design a **single, coherent developer journey** that naturally incorporates both workflows.

---

## Current Workflows — Side by Side

### Phase 1: dbt-Style (Core Pipeline)

```
seeknal init --name my_project
  ↓  Creates: seeknal_project.yml, profiles.yml, seeknal/ dirs, target/
seeknal draft <type> <name>
  ↓  Generates: draft_<type>_<name>.yml (or .py with --python)
  ↓  User edits the draft file
seeknal dry-run draft_<type>_<name>.yml
  ↓  Validates: YAML syntax, schema, dependencies, preview execution
seeknal apply draft_<type>_<name>.yml
  ↓  Moves to: seeknal/<type>s/<name>.yml, updates manifest
seeknal parse
  ↓  Generates: target/manifest.json (DAG, nodes, edges)
seeknal run
  ↓  Executes: incremental pipeline with state tracking
```

**Key properties:**
- File-centric: drafts are created → validated → moved into project
- Single "production" environment (no isolation)
- Sequential execution only (until --parallel flag)
- State tracked in `target/run_state.json`
- Change detection via YAML content hashing

### Phase 2: SQLMesh-Inspired (Environments + Safety)

```
seeknal env plan <env_name>
  ↓  Creates: plan.json with categorized changes (BREAKING/NON_BREAKING/METADATA)
  ↓  Shows: impact analysis, downstream cascade
seeknal env apply <env_name>
  ↓  Executes: changed nodes in isolated environment
  ↓  Unchanged nodes reference production (zero-copy)
seeknal env promote <env_name>
  ↓  Moves: environment state to production
seeknal run --parallel --max-workers 8
  ↓  Executes: topological layer-based concurrent execution
seeknal diff
  ↓  Shows: categorized changes without environment
```

**Key properties:**
- Environment-centric: plan → apply → promote lifecycle
- Isolated testing before production
- Change categorization with downstream impact analysis
- Parallel execution for wide DAGs

---

## Tension Points

### 1. `apply` means two different things

| Context | Command | What it does |
|---------|---------|-------------|
| Phase 1 | `seeknal apply draft.yml` | Move a draft file into `seeknal/<type>s/` |
| Phase 2 | `seeknal env apply dev` | Execute a plan in the dev environment |

**Users ask:** "Do I `apply` my new source, or do I `env apply`?"

### 2. No environment awareness in the core workflow

The Phase 1 workflow always writes to "production" — there's no concept of testing changes in isolation before deploying. `seeknal run` just runs everything.

**Users ask:** "How do I test my changes safely before deploying?"

### 3. `parse` vs `plan` confusion

- `seeknal parse` → generates manifest from YAML files (DAG structure)
- `seeknal env plan` → generates change plan with categorization

These are conceptually related (both analyze the current state of YAML files) but presented as unrelated commands.

### 4. `dry-run` vs `--dry-run` vs `--show-plan`

- `seeknal dry-run <file>` → validate a single YAML file
- `seeknal run --dry-run` → preview whole pipeline execution
- `seeknal run --show-plan` → show execution plan
- `seeknal env plan` → show categorized change plan

Four ways to "preview" with different semantics.

### 5. Parallel execution is opt-in, not discoverable

Users must know about `--parallel` to benefit. It should feel natural, not like an afterthought.

---

## Design Approaches

### Approach A: "Grow Into It" (Minimal Changes)

Keep both workflows separate but improve discoverability and documentation.

**Changes:**
- Add `seeknal env` commands to `init` output ("Next steps" suggestions)
- Add environment-awareness hints to `seeknal run` output
- Create a single tutorial that teaches both workflows together
- Rename nothing, just improve docs

**Pros:** No breaking changes, minimal code changes
**Cons:** Doesn't solve the fundamental split; users still learn two mental models

### Approach B: "Unified Journey" (Moderate Refactor)

Integrate environment workflow into the core commands while keeping backward compatibility.

**Proposed unified flow:**

```
seeknal init --name my_project
  ↓  (same as today)
seeknal draft source my_data
  ↓  (same as today)
seeknal dry-run draft_source_my_data.yml
  ↓  (same as today)
seeknal apply draft_source_my_data.yml
  ↓  (same as today — moves file into project)
seeknal plan [env_name]
  ↓  NEW: merges parse + diff + plan
  ↓  Without env_name: shows changes vs last run (like `seeknal diff`)
  ↓  With env_name: creates isolated environment plan
seeknal run [--env <name>] [--parallel]
  ↓  Without --env: runs in production (current behavior)
  ↓  With --env: runs in isolated environment
seeknal promote <env_name>
  ↓  Promotes environment to production
```

**Key changes:**
1. `seeknal plan` replaces `seeknal parse` + `seeknal diff` + `seeknal env plan`
2. `seeknal run --env dev` replaces `seeknal env apply dev`
3. `seeknal promote` replaces `seeknal env promote`
4. `seeknal env` subgroup still works (backward compat) but new docs teach the unified commands

**Tutorial would teach:**

```
# Day 1: Basic pipeline (like dbt)
seeknal init --name my_project
seeknal draft source customers
# ... edit the draft ...
seeknal dry-run draft_source_customers.yml
seeknal apply draft_source_customers.yml
seeknal plan                    # See what will run
seeknal run                     # Execute it

# Day 2: Safe changes (like SQLMesh)
# ... modify a transform ...
seeknal plan dev                # Plan changes in dev env
seeknal run --env dev           # Test in isolation
seeknal promote dev             # Push to production

# Day 3: Performance (parallel execution)
seeknal run --parallel           # 8x faster for wide DAGs
seeknal run --env dev --parallel # Combine everything
```

**Pros:** Single learning curve, natural progression from simple → advanced
**Cons:** Requires renaming some commands, potentially confusing for existing users

### Approach C: "SQLMesh-First" (Major Refactor)

Make environments the default, with "production" as just another environment.

```
seeknal init --name my_project
seeknal draft source my_data
seeknal plan                    # Always plans against current env
seeknal run                     # Always runs in current env (default: "prod")
seeknal env switch dev          # Switch active environment
seeknal promote dev             # Promote dev → prod
```

**Pros:** Most consistent model (everything is an environment)
**Cons:** Major breaking change, over-engineering for simple use cases

---

## Recommendation: Approach B — "Unified Journey"

Approach B gives us the best balance:
- Keeps the existing dbt-style simplicity for beginners
- Naturally introduces environments and safety when users are ready
- Makes parallel execution more discoverable
- Maintains full backward compatibility

### Implementation Plan

#### Phase 1: New `seeknal plan` Command (Top-Level)

```python
@app.command()
def plan(
    env_name: Optional[str] = typer.Argument(None, help="Environment name (optional)"),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Analyze changes and show execution plan.

    Without environment: shows changes vs last run.
    With environment: creates isolated environment plan.

    Examples:
        seeknal plan              # What changed since last run?
        seeknal plan dev          # Plan changes in dev environment
        seeknal plan staging      # Plan changes in staging
    """
```

This command would:
1. Build the DAG (like `parse`)
2. Compare against last run state (like incremental detection)
3. Categorize changes (like `diff`)
4. If env_name provided: save as environment plan (like `env plan`)
5. Display results with clear categorization

#### Phase 2: Add `--env` Flag to `seeknal run`

```python
def run(
    ...
    env: Optional[str] = typer.Option(None, "--env", help="Run in environment"),
    ...
):
```

When `--env` is specified, `run` behaves like `env apply`:
- Validates the saved plan
- Executes only changed nodes
- Unchanged nodes reference production

#### Phase 3: Top-Level `seeknal promote`

```python
@app.command()
def promote(
    env_name: str = typer.Argument(..., help="Environment to promote"),
):
    """Promote environment changes to production."""
```

#### Phase 4: Improved `seeknal init` Output

Update the "Next steps" to teach the unified flow:

```
Project 'my_project' initialized successfully!

Next steps:
  1. Create sources:     seeknal draft source <name>
  2. Validate drafts:    seeknal dry-run draft_source_<name>.yml
  3. Apply to project:   seeknal apply draft_source_<name>.yml
  4. Plan execution:     seeknal plan
  5. Run pipeline:       seeknal run

Advanced:
  - Test in isolation:   seeknal plan dev && seeknal run --env dev
  - Promote to prod:     seeknal promote dev
  - Parallel execution:  seeknal run --parallel
```

#### Phase 5: Auto-Parallel Suggestion

When `seeknal run` detects a DAG with >3 independent nodes in any layer, suggest parallel execution:

```
Execution Plan:
  Layer 0: 8 sources (can run in parallel)
  Layer 1: 4 transforms
  Layer 2: 2 feature groups
  Layer 3: 1 exposure

Tip: This pipeline has 8 independent nodes. Use --parallel for ~3x speedup.
```

---

## Unified Tutorial Outline

The new tutorial would teach a **single progressive journey**:

```
Part 1: Setup (seeknal init)
Part 2: Define Pipeline (seeknal draft → dry-run → apply)
Part 3: Run Pipeline (seeknal plan → seeknal run)
Part 4: Make Changes (edit YAML → seeknal plan → seeknal run)
Part 5: Safe Testing (seeknal plan dev → seeknal run --env dev → seeknal promote dev)
Part 6: Performance (seeknal run --parallel)
Part 7: Breaking Changes (change categorization in action)
Part 8: Production Tips
```

Each part builds on the previous. Users don't need to learn "Phase 2" separately — it's just the natural next step.

---

## Command Mapping

| User Goal | Current Commands | Unified Command |
|-----------|-----------------|-----------------|
| Start a project | `seeknal init` | `seeknal init` (same) |
| Create a node | `seeknal draft <type> <name>` | `seeknal draft <type> <name>` (same) |
| Validate a draft | `seeknal dry-run <file>` | `seeknal dry-run <file>` (same) |
| Move draft to project | `seeknal apply <file>` | `seeknal apply <file>` (same) |
| See what changed | `seeknal parse` + `seeknal run --show-plan` | `seeknal plan` |
| Plan in environment | `seeknal env plan dev` | `seeknal plan dev` |
| Run pipeline | `seeknal run` | `seeknal run` (same) |
| Run in environment | `seeknal env apply dev` | `seeknal run --env dev` |
| Promote to prod | `seeknal env promote dev` | `seeknal promote dev` |
| Parallel execution | `seeknal run --parallel` | `seeknal run --parallel` (same) |
| List environments | `seeknal env list` | `seeknal env list` (keep) |
| Delete environment | `seeknal env delete dev` | `seeknal env delete dev` (keep) |

---

## Backward Compatibility

All existing `seeknal env *` commands continue to work unchanged. The new top-level commands (`plan`, `promote`) are **aliases** that call the same underlying code. No existing scripts or CI/CD pipelines break.

---

## Open Questions

1. **Should `seeknal plan` auto-run `parse`?** Currently users run `parse` explicitly. The unified `plan` command would incorporate parsing automatically, making `parse` still available but rarely needed.

2. **Should `seeknal run` auto-detect parallelism?** We could enable parallel by default when DAG width > threshold, but explicit opt-in is safer for initial rollout.

3. **Should environments auto-expire?** The current TTL-based cleanup (in EnvironmentManager) is good. Should `seeknal plan dev` auto-create the env if it doesn't exist?

4. **What about `seeknal diff`?** Currently lives in Phase 2. Could become `seeknal plan --diff-only` or remain as a separate command.

---

## Summary

The unified approach keeps Seeknal's dbt-style simplicity for day-one users while naturally introducing SQLMesh's safety features (environments, change categorization) and performance features (parallel execution) as users grow. The key insight is that these aren't separate "phases" — they're a **single progressive workflow** from simple to production-grade.

```
Simple:     init → draft → apply → run
Safe:       init → draft → apply → plan dev → run --env dev → promote dev
Fast:       init → draft → apply → run --parallel
Production: init → draft → apply → plan staging → run --env staging --parallel → promote staging
```
