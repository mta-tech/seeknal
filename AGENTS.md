# Seeknal Agent Guide

This file gives coding/assistant agents project-local guidance for the Seeknal
repository. It complements `CLAUDE.md`; direct user/developer instructions still
win over this file.

## Project overview

Seeknal is an all-in-one data and AI/ML engineering platform. It includes:

- YAML/Python pipeline definitions under `seeknal/`
- feature store and entity consolidation workflows
- a Typer CLI in `src/seeknal/cli/main.py`
- Seeknal Ask: an agent harness for data questions, reports, read-only sources,
  SQL examples, and Ask QA tests

## Ask harness conventions

- Keep tools thin and deterministic. Put workflow and domain behavior in skills,
  generated source context, SQL pairs, tests, and documentation.
- Before guessing Seeknal CLI syntax or generated-file conventions, use the
  local docs CLI: `seeknal docs --list`, `seeknal docs <topic>`, and
  `seeknal docs --json <topic>` for machine-readable guidance.
- `seeknal_agent.yml` configures Ask mode and read-only connected sources.
- `seeknal source connect/status/inspect/sync/test` manages existing databases
  that Ask can query without a pipeline.
- `seeknal/sql_pairs/*.yml` are prompt-to-SQL examples for agent context.
- `seeknal/tests/*.yml` are executable Ask SQL QA cases for
  `seeknal ask test`.
- In tap-in analysis mode, the connected database remains read-only, but users
  can explicitly teach the agent by saying “remember”, “write this down”, or
  “save this query”; persist non-secret rules to `preferences.yml` and longer
  notes/SQL pairs under `context/`.
- Generated source context under `.seeknal/context/sources/` is derived state;
  refresh it with `seeknal source sync` rather than hand-editing.
- Never hardcode customer/domain SQL in the agent harness.

## Init-generated project modes

`seeknal init` must give downstream coding agents enough guidance to initialize
different project types without hardcoding customer logic:

- **Tap-in / read-only connected-source analyst**: use when final analytical
  tables already exist. Scaffold `.env.example`, `seeknal_agent.yml`,
  `SEEKNAL_ASK.md`, `context/`, `seeknal/sql_pairs/`, and `seeknal/tests/`.
  Guide agents to run `seeknal source connect`, `seeknal source sync`,
  `seeknal ask test`, and a TUI smoke test.
- **Data pipeline builder**: use when Seeknal should create managed data assets.
  Guide agents to configure `profiles.yml`, create `seeknal/sources`,
  `seeknal/transforms`, `seeknal/feature_groups`, `seeknal/models`, then run
  `seeknal dry-run`, `seeknal apply`, `seeknal plan`, and `seeknal run`.
- **Hybrid**: use when both managed pipelines and read-only connected sources
  exist. Keep `mode.default: auto`; use SQL pairs/tests/context to make source
  selection explicit for important business questions.

## Safety

- Never commit database DSNs, API keys, or credentials.
- Use `.env` plus `dsn_env` in `seeknal_agent.yml` for connected sources.
- Prefer read-only SQL for exploration and validation.
- Preserve existing CLI and gateway behavior when changing Ask internals.

## Verification

For Ask harness changes, run focused tests such as:

```bash
PYTHONPATH=src pytest -q tests/ask/test_ask_sql_tests.py tests/ask/test_context_tools.py tests/ask/test_toolset.py tests/ask/test_prompt_builder.py tests/cli/test_source.py
```

For init/template changes, include:

```bash
PYTHONPATH=src pytest -q tests/test_cli.py::TestInitCommand tests/ask/test_skills.py
```
