---
name: prepare-seeknal-project
description: Use when helping initialize, configure, or prepare a Seeknal project like a coding agent
---

# Prepare Seeknal Project Skill

Use this skill when the user asks Ask to help create, initialize, configure, or
prepare a Seeknal project. The goal is to turn user/project knowledge into
safe, durable Seeknal assets rather than hardcoding project logic in the Ask
harness.

## First steps

1. Read existing project assets when they exist:
   - `AGENTS.md`
   - `seeknal_agent.yml`
   - `SEEKNAL_ASK.md`
   - relevant files under `context/`, `seeknal/sql_pairs/`, and `seeknal/tests/`
2. Decide the setup lane and state it clearly:
   - **Tap-in / read-only connected-source analyst**: existing analytical DB is the source of truth.
   - **Data pipeline builder**: Seeknal should create managed data assets.
   - **Hybrid**: both connected sources and managed pipelines are used.
3. Ask one concise scoping question if the lane, source type, or expected
   deliverable is unclear. Do not ask for secrets.

## Safe write rules

- Use `write_seeknal_project_asset` for durable Ask/setup assets only:
  - `SEEKNAL_ASK.md`
  - `seeknal_agent.yml`
  - `context/**/*.md|yml|yaml|txt`
  - `seeknal/sql_pairs/**/*.yml|yaml`
  - `seeknal/tests/**/*.yml|yaml`
  - `seeknal/skills/<skill-name>/SKILL.md`
- Use `draft_node` → `dry_run_draft` → `apply_draft` for pipeline definitions.
- Never write actual DSNs, passwords, tokens, API keys, or bearer tokens.
  `seeknal_agent.yml` must store env-var names such as `dsn_env: WAREHOUSE_URL`.
- Do not edit generated `.seeknal/context/sources/` files; tell the user to run
  `seeknal source sync <source> --project .` instead.

## Tap-in / connected-source checklist

1. Ensure `seeknal_agent.yml` has `mode.default: auto` or `project_setup` during setup.
2. Add or explain a source registry entry with:
   - `source_kind: connected`
   - `source_type: database`
   - `connector: postgresql|duckdb|mysql|...`
   - `access: read_only`
   - `dsn_env: <SAFE_ENV_VAR_NAME>`
   - `context_sync.enabled: true`
3. Write durable business context to `SEEKNAL_ASK.md` or `context/*.md`.
4. Create reusable SQL examples in `seeknal/sql_pairs/*.yml`.
5. Create executable QA cases in `seeknal/tests/*.yml`.
6. Tell the user to put the real DSN in `.env`, then run:
   - `seeknal source connect ...` if the source was not registered yet
   - `seeknal source sync <source> --project .`
   - `seeknal source test <source> --project .`
   - `seeknal ask test --project . --sql-only`
   - `seeknal ask chat --project .`

## Pipeline-builder checklist

1. Identify the first source, transform, feature group, model, metric, or semantic model.
2. Create a draft with `draft_node`.
3. Validate with `dry_run_draft`.
4. Apply with `apply_draft(..., confirmed=True)` only after the user explicitly approves or the task clearly authorizes implementation.
5. Use `plan_pipeline` and `run_pipeline` only when the user asked for validation/execution.
6. Add tests or context explaining expected outputs and business meaning.

## Output pattern

When done, summarize:

- Files created/updated
- Chosen project mode and why
- Secrets the user still needs to place in `.env`
- Commands to run next
- Any assumptions or TODOs that remain
